#include <Common/setThreadName.h>
#include <Common/SystemLogBase.h>
#include <Interpreters/ErrorLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/TransposedMetricLog.h>
#include <Interpreters/PeriodicLog.h>
#include <Interpreters/QueryMetricLog.h>

namespace DB
{

template <typename LogElement>
void PeriodicLog<LogElement>::startCollect(const String & thread_name, size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown_metric_thread = false;
    collecting_thread = std::make_unique<ThreadFromGlobalPool>([this, thread_name] {
        setThreadName(thread_name.c_str());
        threadFunction();
    });
}

template <typename LogElement>
void PeriodicLog<LogElement>::stopCollect()
{
    bool old_val = false;
    if (!is_shutdown_metric_thread.compare_exchange_strong(old_val, true))
        return;
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
void PeriodicLog<LogElement>::threadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();
    while (!is_shutdown_metric_thread)
    {
        try
        {
            const auto current_time = std::chrono::system_clock::now();

            stepFunction(current_time);

            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (desired_timepoint <= current_time)
                desired_timepoint += std::chrono::milliseconds(collect_interval_milliseconds);

            std::this_thread::sleep_until(desired_timepoint);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

#define INSTANTIATE_PERIODIC_SYSTEM_LOG(ELEMENT) template class PeriodicLog<ELEMENT>;
SYSTEM_PERIODIC_LOG_ELEMENTS(INSTANTIATE_PERIODIC_SYSTEM_LOG)

}
