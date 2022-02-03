#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <vector>
#include <atomic>
#include <ctime>


namespace DB
{

/** MetricLog is a log of metric values measured at regular time interval.
  */

struct MetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 milliseconds{};

    std::vector<ProfileEvents::Count> profile_events;
    std::vector<CurrentMetrics::Metric> current_metrics;

    static std::string name() { return "MetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class MetricLog : public SystemLog<MetricLogElement>
{
    using SystemLog<MetricLogElement>::SystemLog;

public:
    void shutdown() override;

    /// Launches a background thread to collect metrics with interval
    void startCollectMetric(size_t collect_interval_milliseconds_);

    /// Stop background thread. Call before shutdown.
    void stopCollectMetric();

private:
    void metricThreadFunction();

    ThreadFromGlobalPool metric_flush_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_metric_thread{false};
};

}
