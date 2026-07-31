#include <Common/setThreadName.h>
#include <Common/SystemLogBase.h>
#include <Interpreters/ErrorLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/TransposedMetricLog.h>
#include <Interpreters/PeriodicLog.h>
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/AggregatedZooKeeperLog.h>

namespace DB
{

template <typename LogElement>
void PeriodicLog<LogElement>::startCollect(ThreadName thread_name, size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown_metric_thread = false;
    collecting_thread = std::make_unique<ThreadFromGlobalPool>([this, thread_name] {
        DB::setThreadName(thread_name);
        threadFunction();
    });
}

template <typename LogElement>
void PeriodicLog<LogElement>::stopCollect()
{
    bool old_val = false;
    if (!is_shutdown_metric_thread.compare_exchange_strong(old_val, true))
        return;
    {
        std::lock_guard lock(collect_mutex);
        /// Wake the thread if it is waiting while STOP LOGS is in effect.
    }
    collect_cond.notify_all();
    if (collecting_thread)
        collecting_thread->join();
}

template <typename LogElement>
void PeriodicLog<LogElement>::shutdown()
{
    stopCollect();
    Base::shutdown();
}

template <typename LogElement>
void PeriodicLog<LogElement>::stop()
{
    Base::stop();
    {
        std::lock_guard lock(collect_mutex);
        is_stopped_collect = true;
    }
    collect_cond.notify_all();
}

template <typename LogElement>
void PeriodicLog<LogElement>::start()
{
    {
        std::lock_guard lock(collect_mutex);
        is_stopped_collect = false;
    }
    collect_cond.notify_all();
    Base::start();
}

template <typename LogElement>
void PeriodicLog<LogElement>::stepFunctionSafe(TimePoint current_time)
{
    std::lock_guard lock(step_mutex);
    stepFunction(current_time);
}

template <typename LogElement>
void PeriodicLog<LogElement>::threadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();
    while (!is_shutdown_metric_thread)
    {
        try
        {
            {
                std::unique_lock lock(collect_mutex);
                collect_cond.wait(lock, [this] TSA_REQUIRES(collect_mutex)
                {
                    return !is_stopped_collect || is_shutdown_metric_thread;
                });
                if (is_shutdown_metric_thread)
                    break;
            }

            const auto current_time = std::chrono::system_clock::now();

            stepFunctionSafe(current_time);

            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (desired_timepoint <= current_time)
                desired_timepoint += std::chrono::milliseconds(collect_interval_milliseconds);

            std::unique_lock lock(collect_mutex);
            collect_cond.wait_until(lock, desired_timepoint, [this] TSA_REQUIRES(collect_mutex)
            {
                return is_stopped_collect || is_shutdown_metric_thread;
            });
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

template <typename LogElement>
void PeriodicLog<LogElement>::flushBufferToLog(TimePoint current_time)
{
    /// Do not produce new records while SYSTEM STOP LOGS is in effect; residual queue
    /// entries are still flushed by the saving thread / SYSTEM FLUSH LOGS.
    if (this->isStopped())
        return;

    stepFunctionSafe(current_time);
}

#define INSTANTIATE_PERIODIC_SYSTEM_LOG(ELEMENT) template class PeriodicLog<ELEMENT>;
SYSTEM_PERIODIC_LOG_ELEMENTS(INSTANTIATE_PERIODIC_SYSTEM_LOG)

}
