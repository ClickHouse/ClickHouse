#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>

#include <atomic>
#include <chrono>

#define SYSTEM_PERIODIC_LOG_ELEMENTS(M) \
    M(ErrorLogElement) \
    M(MetricLogElement) \
    M(TransposedMetricLogElement) \
    M(AggregatedZooKeeperLogElement) \

namespace DB
{

template <typename LogElement>
class PeriodicLog : public SystemLog<LogElement>
{
    using SystemLog<LogElement>::SystemLog;
    using Base = SystemLog<LogElement>;

public:
    using TimePoint = std::chrono::system_clock::time_point;

    /// Launches a background thread to collect metrics with periodic interval
    void startCollect(ThreadName thread_name, size_t collect_interval_milliseconds_);

    void shutdown() final;

    void flushBufferToLog(TimePoint current_time) final;

protected:
    /// Stop background thread
    void stopCollect();

    virtual void stepFunction(TimePoint current_time) = 0;

private:
    void threadFunction();

    std::unique_ptr<ThreadFromGlobalPool> collecting_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_metric_thread{false};
};

}
