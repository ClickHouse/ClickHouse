#include "ThreadProfileEvents.h"

#if defined(__linux__)

#include "TaskStatsInfoGetter.h"
#include "ProcfsMetricsProvider.h"

#include <optional>


namespace DB
{

bool TasksStatsCounters::checkIfAvailable()
{
    return findBestAvailableProvider() != MetricsProvider::None;
}

std::unique_ptr<TasksStatsCounters> TasksStatsCounters::create(const UInt64 tid)
{
    std::unique_ptr<TasksStatsCounters> instance;
    if (checkIfAvailable())
        instance.reset(new TasksStatsCounters(tid, findBestAvailableProvider()));
    return instance;
}

TasksStatsCounters::MetricsProvider TasksStatsCounters::findBestAvailableProvider()
{
    /// This initialization is thread-safe and executed once since C++11
    static std::optional<MetricsProvider> provider =
        []() -> MetricsProvider
        {
            if (TaskStatsInfoGetter::checkPermissions())
            {
                return MetricsProvider::Netlink;
            }
            else if (ProcfsMetricsProvider::isAvailable())
            {
                return MetricsProvider::Procfs;
            }
            return MetricsProvider::None;
        }();

    return *provider;
}


TasksStatsCounters::TasksStatsCounters(const UInt64 tid, const MetricsProvider provider)
{
    switch (provider)
    {
    case MetricsProvider::Netlink:
        stats_getter = [metrics_provider = std::make_shared<TaskStatsInfoGetter>(), tid]()
                {
                    ::taskstats result;
                    metrics_provider->getStat(result, tid);
                    return result;
                };
        break;
    case MetricsProvider::Procfs:
        stats_getter = [metrics_provider = std::make_shared<ProcfsMetricsProvider>(tid)]()
                {
                    ::taskstats result;
                    metrics_provider->getTaskStats(result);
                    return result;
                };
        break;
    case MetricsProvider::None:
        ;
    }
}

void TasksStatsCounters::reset()
{
    if (stats_getter)
        stats = stats_getter();
}

void TasksStatsCounters::updateCounters(ProfileEvents::Counters & profile_events)
{
    if (!stats_getter)
        return;

    const auto new_stats = stats_getter();
    incrementProfileEvents(stats, new_stats, profile_events);
    stats = new_stats;
}

void TasksStatsCounters::incrementProfileEvents(const ::taskstats & prev, const ::taskstats & curr, ProfileEvents::Counters & profile_events)
{
    profile_events.increment(ProfileEvents::OSCPUWaitMicroseconds,
                             safeDiff(prev.cpu_delay_total, curr.cpu_delay_total) / 1000U);
    profile_events.increment(ProfileEvents::OSIOWaitMicroseconds,
                             safeDiff(prev.blkio_delay_total, curr.blkio_delay_total) / 1000U);
    profile_events.increment(ProfileEvents::OSCPUVirtualTimeMicroseconds,
                             safeDiff(prev.cpu_run_virtual_total, curr.cpu_run_virtual_total) / 1000U);

    /// Since TASKSTATS_VERSION = 3 extended accounting and IO accounting is available.
    if (curr.version < 3)
        return;

    profile_events.increment(ProfileEvents::OSReadChars, safeDiff(prev.read_char, curr.read_char));
    profile_events.increment(ProfileEvents::OSWriteChars, safeDiff(prev.write_char, curr.write_char));
    profile_events.increment(ProfileEvents::OSReadBytes, safeDiff(prev.read_bytes, curr.read_bytes));
    profile_events.increment(ProfileEvents::OSWriteBytes, safeDiff(prev.write_bytes, curr.write_bytes));
}
}

#endif
