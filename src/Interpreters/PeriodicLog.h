#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>

#include <atomic>
#include <chrono>
#include <mutex>

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
    /// Acquires step_mutex and calls stepFunction. Serializes calls between
    /// the background periodic thread and flushBufferToLog (called by
    /// SYSTEM FLUSH LOGS). Without this, flushBufferToLog could observe an
    /// empty buffer while the background thread is still adding elements to the
    /// queue from a concurrent stepFunction call, causing getLastLogIndex to
    /// return a stale value.
    void stepFunctionSafe(TimePoint current_time);

    void threadFunction();

    std::mutex step_mutex;
    std::unique_ptr<ThreadFromGlobalPool> collecting_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_metric_thread{false};
};

}
