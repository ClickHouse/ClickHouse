#include "ThreadProfileEvents.h"

#if defined(__linux__)

#include "TaskStatsInfoGetter.h"
#include "ProcfsMetricsProvider.h"
#include "hasLinuxCapability.h"

#include <optional>
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <cerrno>


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

static PerfEventInfo softwareEvent(int event_config, ProfileEvents::Event profile_event)
{
    return PerfEventInfo
        {
            .event_type = perf_type_id::PERF_TYPE_SOFTWARE,
            .event_config = event_config,
            .profile_event = profile_event,
            .profile_event_running = std::nullopt,
            .profile_event_enabled = std::nullopt
        };
}

#define HARDWARE_EVENT(PERF_NAME, LOCAL_NAME) \
    PerfEventInfo \
    { \
        .event_type = perf_type_id::PERF_TYPE_HARDWARE, \
        .event_config = PERF_NAME, \
        .profile_event = ProfileEvents::LOCAL_NAME, \
        .profile_event_running = {ProfileEvents::LOCAL_NAME##Running}, \
        .profile_event_enabled = {ProfileEvents::LOCAL_NAME##Enabled} \
    }

// descriptions' source: http://man7.org/linux/man-pages/man2/perf_event_open.2.html
const PerfEventInfo PerfEventsCounters::raw_events_info[] = {
    HARDWARE_EVENT(PERF_COUNT_HW_CPU_CYCLES, PerfCpuCycles),
    HARDWARE_EVENT(PERF_COUNT_HW_INSTRUCTIONS, PerfInstructions),
    HARDWARE_EVENT(PERF_COUNT_HW_CACHE_REFERENCES, PerfCacheReferences),
    HARDWARE_EVENT(PERF_COUNT_HW_CACHE_MISSES, PerfCacheMisses),
    HARDWARE_EVENT(PERF_COUNT_HW_BRANCH_INSTRUCTIONS, PerfBranchInstructions),
    HARDWARE_EVENT(PERF_COUNT_HW_BRANCH_MISSES, PerfBranchMisses),
    HARDWARE_EVENT(PERF_COUNT_HW_BUS_CYCLES, PerfBusCycles),
    HARDWARE_EVENT(PERF_COUNT_HW_STALLED_CYCLES_FRONTEND, PerfStalledCyclesFrontend),
    HARDWARE_EVENT(PERF_COUNT_HW_STALLED_CYCLES_BACKEND, PerfStalledCyclesBackend),
    HARDWARE_EVENT(PERF_COUNT_HW_REF_CPU_CYCLES, PerfRefCpuCycles),
    // This reports the CPU clock, a high-resolution per-CPU timer.
    // a bit broken according to this: https://stackoverflow.com/a/56967896
//    softwareEvent(PERF_COUNT_SW_CPU_CLOCK, ProfileEvents::PerfCpuClock),
    softwareEvent(PERF_COUNT_SW_TASK_CLOCK, ProfileEvents::PerfTaskClock),
    softwareEvent(PERF_COUNT_SW_PAGE_FAULTS, ProfileEvents::PerfPageFaults),
    softwareEvent(PERF_COUNT_SW_CONTEXT_SWITCHES, ProfileEvents::PerfContextSwitches),
    softwareEvent(PERF_COUNT_SW_CPU_MIGRATIONS, ProfileEvents::PerfCpuMigrations),
    softwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MIN, ProfileEvents::PerfPageFaultsMin),
    softwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MAJ, ProfileEvents::PerfPageFaultsMaj),
    softwareEvent(PERF_COUNT_SW_ALIGNMENT_FAULTS, ProfileEvents::PerfAlignmentFaults),
    softwareEvent(PERF_COUNT_SW_EMULATION_FAULTS, ProfileEvents::PerfEmulationFaults)
    // This is a placeholder event that counts nothing. Informational sample record types such as mmap or
    // comm must be associated with an active event. This dummy event allows gathering such records
    // without requiring a counting event.
//            softwareEventInfo(PERF_COUNT_SW_DUMMY, ProfileEvents::PERF_COUNT_SW_DUMMY)
};

#undef HARDWARE_EVENT

thread_local PerfDescriptorsHolder PerfEventsCounters::thread_events_descriptors_holder{};
thread_local bool PerfEventsCounters::thread_events_descriptors_opened = false;
thread_local PerfEventsCounters * PerfEventsCounters::current_thread_counters = nullptr;

std::atomic<bool> PerfEventsCounters::perf_unavailability_logged = false;
std::atomic<bool> PerfEventsCounters::particular_events_unavailability_logged = false;

Logger * PerfEventsCounters::getLogger()
{
    return &Logger::get("PerfEventsCounters");
}

PerfEventValue PerfEventsCounters::getRawValue(int event_type, int event_config) const
{
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        const PerfEventInfo & event_info = raw_events_info[i];
        if (event_info.event_type == event_type && event_info.event_config == event_config)
            return raw_event_values[i];
    }

    LOG_WARNING(getLogger(), "Can't find perf event info for event_type=" << event_type << ", event_config=" << event_config);
    return {};
}

static int openPerfEvent(perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, UInt64 flags)
{
    return static_cast<int>(syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags));
}

static bool getPerfEventParanoid(Int32 & result)
{
    // the longest possible variant: "-1\0"
    constexpr Int32 max_length = 3;

    FILE * fp = fopen("/proc/sys/kernel/perf_event_paranoid", "r");
    if (fp == nullptr)
        return false;

    char str[max_length];
    char * res = fgets(str, max_length, fp);
    fclose(fp);
    if (res == nullptr)
        return false;

    str[max_length - 1] = '\0';
    result = atoi(str);
    return true;
}

static void perfEventOpenDisabled(Int32 perf_event_paranoid, bool has_cap_sys_admin, int perf_event_type, int perf_event_config, int & event_file_descriptor)
{
    perf_event_attr pe = perf_event_attr();
    pe.type = perf_event_type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = perf_event_config;
    // disable by default to add as little extra time as possible
    pe.disabled = 1;
    // can record kernel only when `perf_event_paranoid` <= 1 or have CAP_SYS_ADMIN
    pe.exclude_kernel = perf_event_paranoid >= 2 && !has_cap_sys_admin;
    pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;

    event_file_descriptor = openPerfEvent(&pe, /* measure the calling thread */ 0, /* on any cpu */ -1, -1, 0);
}

bool PerfEventsCounters::initializeThreadLocalEvents(PerfEventsCounters & counters)
{
    if (thread_events_descriptors_opened)
        return true;

    Int32 perf_event_paranoid = 0;
    bool is_pref_available = getPerfEventParanoid(perf_event_paranoid);
    if (!is_pref_available)
    {
        bool expected_value = false;
        if (perf_unavailability_logged.compare_exchange_strong(expected_value, true))
            LOG_INFO(getLogger(), "Perf events are unsupported");
        return false;
    }

    bool has_cap_sys_admin = hasLinuxCapability(CAP_SYS_ADMIN);
    if (perf_event_paranoid >= 3 && !has_cap_sys_admin)
    {
        bool expected_value = false;
        if (perf_unavailability_logged.compare_exchange_strong(expected_value, true))
            LOG_INFO(getLogger(), "Not enough permissions to record perf events");
        return false;
    }

    bool expected = false;
    bool log_unsupported_event = particular_events_unavailability_logged.compare_exchange_strong(expected, true);
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        counters.raw_event_values[i] = {};
        const PerfEventInfo & event_info = raw_events_info[i];
        int & fd = thread_events_descriptors_holder.descriptors[i];
        perfEventOpenDisabled(perf_event_paranoid, has_cap_sys_admin, event_info.event_type, event_info.event_config, fd);

        if (fd == -1 && log_unsupported_event)
        {
            LOG_INFO(getLogger(), "Perf event is unsupported: event_type=" << event_info.event_type
                                                                           << ", event_config=" << event_info.event_config);
        }
    }

    thread_events_descriptors_opened = true;
    return true;
}

void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters & counters)
{
    if (current_thread_counters == &counters)
        return;
    if (current_thread_counters != nullptr)
    {
        LOG_WARNING(getLogger(), "Only one instance of `PerfEventsCounters` can be used on the thread");
        return;
    }

    if (!initializeThreadLocalEvents(counters))
        return;

    for (PerfEventValue & raw_value : counters.raw_event_values)
        raw_value = {};

    for (int fd : thread_events_descriptors_holder.descriptors)
    {
        if (fd != -1)
            ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
    }

    current_thread_counters = &counters;
}

void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events)
{
    if (current_thread_counters != &counters)
        return;
    if (!thread_events_descriptors_opened)
        return;

    // process raw events

    // only read counters here to have as little overhead for processing as possible
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        int fd = counters.thread_events_descriptors_holder.descriptors[i];
        if (fd == -1)
            continue;

        constexpr ssize_t bytes_to_read = sizeof(counters.raw_event_values[0]);
        if (read(fd, &counters.raw_event_values[i], bytes_to_read) != bytes_to_read)
        {
            LOG_WARNING(getLogger(), "Can't read event value from file descriptor: " << fd);
            counters.raw_event_values[i] = {};
        }
    }

    // actually process counters' values and stop measuring
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        int fd = counters.thread_events_descriptors_holder.descriptors[i];
        if (fd == -1)
            continue;

        const PerfEventInfo & info = raw_events_info[i];
        const PerfEventValue & raw_value = counters.raw_event_values[i];
        profile_events.increment(info.profile_event, raw_value.value);
        if (info.profile_event_running.has_value())
            profile_events.increment(info.profile_event_running.value(), raw_value.time_running);
        if (info.profile_event_enabled.has_value())
            profile_events.increment(info.profile_event_enabled.value(), raw_value.time_enabled);

        if (ioctl(fd, PERF_EVENT_IOC_DISABLE, 0))
            LOG_WARNING(getLogger(), "Can't disable perf event with file descriptor: " << fd);
        if (ioctl(fd, PERF_EVENT_IOC_RESET, 0))
            LOG_WARNING(getLogger(), "Can't reset perf event with file descriptor: " << fd);
    }

    current_thread_counters = nullptr;
}

PerfDescriptorsHolder::PerfDescriptorsHolder()
{
    for (int & descriptor : descriptors)
        descriptor = -1;
}

PerfDescriptorsHolder::~PerfDescriptorsHolder()
{
    for (int & descriptor : descriptors)
    {
        if (descriptor == -1)
            continue;

        if (ioctl(descriptor, PERF_EVENT_IOC_DISABLE, 0))
            LOG_WARNING(getLogger(), "Can't disable perf event with file descriptor: " << descriptor);
        if (close(descriptor))
            LOG_WARNING(getLogger(),"Can't close perf event file descriptor: " << descriptor
                                                                               << "; error: " << errno << " - " << strerror(errno));

        descriptor = -1;
    }
}

Logger * PerfDescriptorsHolder::getLogger()
{
    return &Logger::get("PerfDescriptorsHolder");
}
}

#else

namespace DB
{
    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters &) {}
    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters &, ProfileEvents::Counters &) {}
}

#endif
