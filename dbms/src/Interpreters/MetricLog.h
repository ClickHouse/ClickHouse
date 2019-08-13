#pragma once
#include <Interpreters/SystemLog.h>
#include <Interpreters/AsynchronousMetrics.h>

namespace DB
{

using Poco::Message;

struct MetricLogElement
{
    time_t event_time{};

    static std::string name() { return "MetricLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class MetricLog : public SystemLog<MetricLogElement>
{
    using SystemLog<MetricLogElement>::SystemLog;

public:
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
