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

    /// Launches a background thread to collect metrics with periodic interval
    void startCollect(const String & thread_name, size_t collect_interval_milliseconds_);

    /// Stop background thread
    virtual void stopCollect();

    void shutdown() final;

protected:
    virtual void stepFunction(TimePoint current_time) = 0;
    virtual void threadFunction();

    std::atomic<bool> is_shutdown_metric_thread{false};
    std::unique_ptr<ThreadFromGlobalPool> worker_thread;

private:
    size_t collect_interval_milliseconds;
};

}
