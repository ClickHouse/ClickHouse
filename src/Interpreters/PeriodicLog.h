#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>

#define SYSTEM_PERIODIC_LOG_ELEMENTS(M) \
    M(ErrorLogElement) \
    M(MetricLogElement)

namespace DB
{

template <typename LogElement>
class PeriodicLog : public SystemLog<LogElement>
{
    using SystemLog<LogElement>::SystemLog;

public:
    using TimePoint = std::chrono::system_clock::time_point;

    /// Launches a background thread to collect metrics with interval
    void startCollect(size_t collect_interval_milliseconds_);

    /// Stop background thread
    void stopCollect();

    void shutdown() final;

protected:
    virtual void stepFunction(TimePoint current_time) = 0;

private:
    void threadFunction();

    std::unique_ptr<ThreadFromGlobalPool> flush_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_metric_thread{false};
};

}
