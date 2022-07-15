# coding: utf-8, frozen_string_literal: true
require "puma"
require "puma/plugin"
require "datadog/statsd"

# Wrap puma's stats in a safe API
class PumaStats
  def initialize(stats)
    @stats = stats
  end

  def clustered?
    @stats.has_key?(:workers)
  end

  def workers
    @stats.fetch(:workers, 1)
  end

  def booted_workers
    @stats.fetch(:booted_workers, 1)
  end

  def old_workers
    @stats.fetch(:old_workers, 0)
  end

  def running
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:running, 0) }.inject(0, &:+)
    else
      @stats.fetch(:running, 0)
    end
  end

  def backlog
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:backlog, 0) }.inject(0, &:+)
    else
      @stats.fetch(:backlog, 0)
    end
  end

  def pool_capacity
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:pool_capacity, 0) }.inject(0, &:+)
    else
      @stats.fetch(:pool_capacity, 0)
    end
  end

  def max_threads
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:max_threads, 0) }.inject(0, &:+)
    else
      @stats.fetch(:max_threads, 0)
    end
  end

  def requests_count
    if clustered?
      @stats[:worker_status].map { |s| s[:last_status].fetch(:requests_count, 0) }.inject(0, &:+)
    else
      @stats.fetch(:requests_count, 0)
    end
  end
end

Puma::Plugin.create do
  # We can start doing something when we have a launcher:
  def start(launcher)
    @launcher = launcher

    @statsd = Datadog::Statsd.new(ENV.fetch('DD_AGENT_HOST', 'localhost'),
                                   ENV.fetch('DD_METRIC_AGENT_PORT', '8125'))
    @launcher.events.debug "statsd: enabled (host: #{@statsd.host})"

    # Fetch global metric prefix from env variable
    @metric_prefix = ENV.fetch("STATSD_METRIC_PREFIX", nil)
    if @metric_prefix && !@metric_prefix.end_with?(::StatsdConnector::METRIC_DELIMETER)
      @metric_prefix += ::StatsdConnector::METRIC_DELIMETER
    end

    register_hooks
  end

  private

  def register_hooks
    in_background(&method(:stats_loop))
  end

  def environment_variable_tags
    # Tags are separated by spaces, and while they are normally a tag and
    # value separated by a ':', they can also just be tagged without any
    # associated value.
    #
    # Examples: simple-tag-0 tag-key-1:tag-value-1
    #
    tags = []

    if ENV.has_key?('DD_DOGSTATSD_TAGS')
      ENV["DD_DOGSTATSD_TAGS"].split(/\s+|,/).each do |t|
        tags << t
      end
    end

    # Support the Unified Service Tagging from Datadog, so that we can share
    # the metric tags with the application running
    #
    # https://docs.datadoghq.com/getting_started/tagging/unified_service_tagging
    if ENV.has_key?("DD_ENV")
      tags << "env:#{ENV["DD_ENV"]}"
    end

    if ENV.has_key?("DD_SERVICE")
      tags << "service:#{ENV["DD_SERVICE"]}"
    end

    if ENV.has_key?("DD_VERSION")
      tags << "version:#{ENV["DD_VERSION"]}"
    end

    # Return nil if we have no environment variable tags. This way we don't
    # send an unnecessary '|' on the end of each stat
    return nil if tags.empty?

    tags.join(",")
  end

  def prefixed_metric_name(puma_metric)
    "#{@metric_prefix}#{puma_metric}"
  end

  # Send data to statsd every few seconds
  def stats_loop
    tags = environment_variable_tags

    sleep 5
    loop do
      @launcher.events.debug "statsd: notify statsd"
      begin
        stats = ::PumaStats.new(Puma.stats_hash)
        @statsd.gauge(prefixed_metric_name("puma.workers"), stats.workers, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.booted_workers"), stats.booted_workers, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.old_workers"), stats.old_workers, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.running"), stats.running, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.backlog"), stats.backlog, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.pool_capacity"), stats.pool_capacity, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.max_threads"), stats.max_threads, tags: tags)
        @statsd.gauge(prefixed_metric_name("puma.requests_count"), stats.requests_count, tags: tags)
      rescue StandardError => e
        @launcher.events.unknown_error e, nil, "! statsd: notify stats failed"
      ensure
        sleep 2
      end
    end
  end
end
