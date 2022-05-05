#include "ThreadProfileEvents.h"

#if defined(__linux__)

#include "TaskStatsInfoGetter.h"
#include "ProcfsMetricsProvider.h"
#include "hasLinuxCapability.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <unordered_set>

#include <fcntl.h>
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <cerrno>
#include <sys/types.h>
#include <dirent.h>

#include <boost/algorithm/string/split.hpp>

#include <base/errnoToString.h>


namespace ProfileEvents
{
    extern const Event OSIOWaitMicroseconds;
    extern const Event OSCPUWaitMicroseconds;
    extern const Event OSCPUVirtualTimeMicroseconds;
    extern const Event OSReadChars;
    extern const Event OSWriteChars;
    extern const Event OSReadBytes;
    extern const Event OSWriteBytes;

    extern const Event PerfCpuCycles;
    extern const Event PerfInstructions;
    extern const Event PerfCacheReferences;
    extern const Event PerfCacheMisses;
    extern const Event PerfBranchInstructions;
    extern const Event PerfBranchMisses;
    extern const Event PerfBusCycles;
    extern const Event PerfStalledCyclesFrontend;
    extern const Event PerfStalledCyclesBackend;
    extern const Event PerfRefCpuCycles;

    extern const Event PerfCpuClock;
    extern const Event PerfTaskClock;
    extern const Event PerfContextSwitches;
    extern const Event PerfCpuMigrations;
    extern const Event PerfAlignmentFaults;
    extern const Event PerfEmulationFaults;
    extern const Event PerfMinEnabledTime;
    extern const Event PerfMinEnabledRunningTime;
    extern const Event PerfDataTLBReferences;
    extern const Event PerfDataTLBMisses;
    extern const Event PerfInstructionTLBReferences;
    extern const Event PerfInstructionTLBMisses;
    extern const Event PerfLocalMemoryReferences;
    extern const Event PerfLocalMemoryMisses;
}

namespace DB
{

const char * TasksStatsCounters::metricsProviderString(MetricsProvider provider)
{
    switch (provider)
    {
        case MetricsProvider::None:
            return "none";
        case MetricsProvider::Procfs:
            return "procfs";
        case MetricsProvider::Netlink:
            return "netlink";
    }
    __builtin_unreachable();
}

bool TasksStatsCounters::checkIfAvailable()
{
    return findBestAvailableProvider() != MetricsProvider::None;
}

std::unique_ptr<TasksStatsCounters> TasksStatsCounters::create(UInt64 tid)
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
                    ::taskstats result{};
                    metrics_provider->getStat(result, tid);
                    return result;
                };
        break;
    case MetricsProvider::Procfs:
        stats_getter = [metrics_provider = std::make_shared<ProcfsMetricsProvider>(tid)]()
                {
                    ::taskstats result{};
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

#if defined(__linux__)

namespace DB
{

thread_local PerfEventsCounters current_thread_counters;

#define SOFTWARE_EVENT(PERF_NAME, LOCAL_NAME) \
    PerfEventInfo \
    { \
        .event_type = perf_type_id::PERF_TYPE_SOFTWARE, \
        .event_config = (PERF_NAME), \
        .profile_event = ProfileEvents::LOCAL_NAME, \
        .settings_name = #LOCAL_NAME \
    }

#define HARDWARE_EVENT(PERF_NAME, LOCAL_NAME) \
    PerfEventInfo \
    { \
        .event_type = perf_type_id::PERF_TYPE_HARDWARE, \
        .event_config = (PERF_NAME), \
        .profile_event = ProfileEvents::LOCAL_NAME, \
        .settings_name = #LOCAL_NAME \
    }

// One event for cache accesses and one for cache misses.
// Type is ACCESS or MISS
#define CACHE_EVENT(PERF_NAME, LOCAL_NAME, TYPE) \
    PerfEventInfo \
    { \
        .event_type = perf_type_id::PERF_TYPE_HW_CACHE, \
        .event_config = (PERF_NAME) \
            | (PERF_COUNT_HW_CACHE_OP_READ << 8) \
            | (PERF_COUNT_HW_CACHE_RESULT_ ## TYPE << 16), \
        .profile_event = ProfileEvents::LOCAL_NAME, \
        .settings_name = #LOCAL_NAME \
    }

// descriptions' source: http://man7.org/linux/man-pages/man2/perf_event_open.2.html
static const PerfEventInfo raw_events_info[] = {
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

    // `cpu-clock` is a bit broken according to this: https://stackoverflow.com/a/56967896
    SOFTWARE_EVENT(PERF_COUNT_SW_CPU_CLOCK, PerfCpuClock),
    SOFTWARE_EVENT(PERF_COUNT_SW_TASK_CLOCK, PerfTaskClock),
    SOFTWARE_EVENT(PERF_COUNT_SW_CONTEXT_SWITCHES, PerfContextSwitches),
    SOFTWARE_EVENT(PERF_COUNT_SW_CPU_MIGRATIONS, PerfCpuMigrations),
    SOFTWARE_EVENT(PERF_COUNT_SW_ALIGNMENT_FAULTS, PerfAlignmentFaults),
    SOFTWARE_EVENT(PERF_COUNT_SW_EMULATION_FAULTS, PerfEmulationFaults),

    // Don't add them -- they are the same as SoftPageFaults and HardPageFaults,
    // match well numerically.
    // SOFTWARE_EVENT(PERF_COUNT_SW_PAGE_FAULTS_MIN, PerfPageFaultsMinor),
    // SOFTWARE_EVENT(PERF_COUNT_SW_PAGE_FAULTS_MAJ, PerfPageFaultsMajor),

    CACHE_EVENT(PERF_COUNT_HW_CACHE_DTLB, PerfDataTLBReferences, ACCESS),
    CACHE_EVENT(PERF_COUNT_HW_CACHE_DTLB, PerfDataTLBMisses, MISS),

    // Apparently it doesn't make sense to treat these values as relative:
    // https://stackoverflow.com/questions/49933319/how-to-interpret-perf-itlb-loads-itlb-load-misses
    CACHE_EVENT(PERF_COUNT_HW_CACHE_ITLB, PerfInstructionTLBReferences, ACCESS),
    CACHE_EVENT(PERF_COUNT_HW_CACHE_ITLB, PerfInstructionTLBMisses, MISS),

    CACHE_EVENT(PERF_COUNT_HW_CACHE_NODE, PerfLocalMemoryReferences, ACCESS),
    CACHE_EVENT(PERF_COUNT_HW_CACHE_NODE, PerfLocalMemoryMisses, MISS),
};

static_assert(sizeof(raw_events_info) / sizeof(raw_events_info[0]) == NUMBER_OF_RAW_EVENTS);

#undef HARDWARE_EVENT
#undef SOFTWARE_EVENT
#undef CACHE_EVENT

// A map of event name -> event index, to parse event list in settings.
static std::unordered_map<std::string_view, size_t> populateEventMap()
{
    std::unordered_map<std::string_view, size_t> name_to_index;
    name_to_index.reserve(NUMBER_OF_RAW_EVENTS);

    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        name_to_index.emplace(raw_events_info[i].settings_name, i);
    }

    return name_to_index;
}

static const auto event_name_to_index = populateEventMap();

static int openPerfEvent(perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, UInt64 flags)
{
    return static_cast<int>(syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags));
}

static int openPerfEventDisabled(Int32 perf_event_paranoid, bool has_cap_sys_admin, UInt32 perf_event_type, UInt64 perf_event_config)
{
    perf_event_attr pe{};
    pe.type = perf_event_type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = perf_event_config;
    // disable by default to add as little extra time as possible
    pe.disabled = 1;
    // can record kernel only when `perf_event_paranoid` <= 1 or have CAP_SYS_ADMIN
    pe.exclude_kernel = perf_event_paranoid >= 2 && !has_cap_sys_admin;
    pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;

    return openPerfEvent(&pe, /* measure the calling thread */ 0, /* on any cpu */ -1, -1, 0);
}

static void enablePerfEvent(int event_fd)
{
    if (ioctl(event_fd, PERF_EVENT_IOC_ENABLE, 0))
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Can't enable perf event with file descriptor {}: '{}' ({})",
            event_fd, errnoToString(errno), errno);
    }
}

static void disablePerfEvent(int event_fd)
{
    if (ioctl(event_fd, PERF_EVENT_IOC_DISABLE, 0))
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Can't disable perf event with file descriptor {}: '{}' ({})",
            event_fd, errnoToString(errno), errno);
    }
}

static void releasePerfEvent(int event_fd)
{
    if (close(event_fd))
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Can't close perf event file descriptor {}: {} ({})",
            event_fd, errnoToString(errno), errno);
    }
}

static bool validatePerfEventDescriptor(int & fd)
{
    if (fcntl(fd, F_GETFL) != -1)
        return true;

    if (errno == EBADF)
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Event descriptor {} was closed from the outside; reopening", fd);
    }
    else
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Error while checking availability of event descriptor {}: {} ({})",
            fd, errnoToString(errno), errno);

        disablePerfEvent(fd);
        releasePerfEvent(fd);
    }

    fd = -1;
    return false;
}

bool PerfEventsCounters::processThreadLocalChanges(const std::string & needed_events_list)
{
    const auto valid_event_indices = eventIndicesFromString(needed_events_list);

    // find state changes (if there are any)
    bool old_state[NUMBER_OF_RAW_EVENTS];
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        old_state[i] = thread_events_descriptors_holder.descriptors[i] != -1;

    bool new_state[NUMBER_OF_RAW_EVENTS];
    std::fill_n(new_state, NUMBER_OF_RAW_EVENTS, false);
    for (size_t opened_index : valid_event_indices)
        new_state[opened_index] = true;

    std::vector<size_t> events_to_open;
    std::vector<size_t> events_to_release;
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        bool old_one = old_state[i];
        bool new_one = new_state[i];

        if (old_one == new_one)
        {
            if (old_one
                && !validatePerfEventDescriptor(
                    thread_events_descriptors_holder.descriptors[i]))
            {
                events_to_open.push_back(i);
            }
            continue;
        }

        if (new_one)
            events_to_open.push_back(i);
        else
            events_to_release.push_back(i);
    }

    // release unused descriptors
    for (size_t i : events_to_release)
    {
        int & fd = thread_events_descriptors_holder.descriptors[i];
        disablePerfEvent(fd);
        releasePerfEvent(fd);
        fd = -1;
    }

    if (events_to_open.empty())
    {
        return true;
    }

    // check permissions
    // cat /proc/sys/kernel/perf_event_paranoid
    // -1: Allow use of (almost) all events by all users
    // >=0: Disallow raw tracepoint access by users without CAP_IOC_LOCK
    // >=1: Disallow CPU event access by users without CAP_SYS_ADMIN
    // >=2: Disallow kernel profiling by users without CAP_SYS_ADMIN
    // >=3: Disallow all event access by users without CAP_SYS_ADMIN
    Int32 perf_event_paranoid = 0;
    std::ifstream paranoid_file("/proc/sys/kernel/perf_event_paranoid");
    paranoid_file >> perf_event_paranoid;

    bool has_cap_sys_admin = hasLinuxCapability(CAP_SYS_ADMIN);
    if (perf_event_paranoid >= 3 && !has_cap_sys_admin)
    {
        LOG_WARNING(&Poco::Logger::get("PerfEvents"),
            "Not enough permissions to record perf events: "
            "perf_event_paranoid = {} and CAP_SYS_ADMIN = 0",
            perf_event_paranoid);
        return false;
    }

    // Open descriptors for new events.
    // Theoretically, we can run out of file descriptors. Threads go up to 10k,
    // and there might be a dozen perf events per thread, so we're looking at
    // 100k open files. In practice, this is not likely -- perf events are
    // mostly used in performance tests or other kinds of testing, and the
    // number of threads stays below hundred.
    // We used to check the number of open files by enumerating /proc/self/fd,
    // but listing all open files before opening more files is obviously
    // quadratic, and quadraticity never ends well.
    for (size_t i : events_to_open)
    {
        const PerfEventInfo & event_info = raw_events_info[i];
        int & fd = thread_events_descriptors_holder.descriptors[i];
        // disable by default to add as little extra time as possible
        fd = openPerfEventDisabled(perf_event_paranoid, has_cap_sys_admin, event_info.event_type, event_info.event_config);

        if (fd == -1 && errno != ENOENT)
        {
            // ENOENT means that the event is not supported. Don't log it, because
            // this is called for each thread and would be too verbose. Log other
            // error codes because they might signify an error.
            LOG_WARNING(&Poco::Logger::get("PerfEvents"),
                "Failed to open perf event {} (event_type={}, event_config={}): "
                "'{}' ({})", event_info.settings_name, event_info.event_type,
                event_info.event_config, errnoToString(errno), errno);
        }
    }

    return true;
}

// Parse comma-separated list of event names. Empty means all available events.
std::vector<size_t> PerfEventsCounters::eventIndicesFromString(const std::string & events_list)
{
    std::vector<size_t> result;
    result.reserve(NUMBER_OF_RAW_EVENTS);

    if (events_list.empty())
    {
        for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        {
            result.push_back(i);
        }
        return result;
    }

    std::vector<std::string> event_names;
    boost::split(event_names, events_list, [](char c) { return c == ','; });

    for (auto & event_name : event_names)
    {
        // Allow spaces at the beginning of the token, so that you can write 'a, b'.
        event_name.erase(0, event_name.find_first_not_of(' '));

        auto entry = event_name_to_index.find(event_name);
        if (entry != event_name_to_index.end())
        {
            result.push_back(entry->second);
        }
        else
        {
            LOG_ERROR(&Poco::Logger::get("PerfEvents"),
                "Unknown perf event name '{}' specified in settings", event_name);
        }
    }

    return result;
}

void PerfEventsCounters::initializeProfileEvents(const std::string & events_list)
{
    if (!processThreadLocalChanges(events_list))
        return;

    for (int fd : thread_events_descriptors_holder.descriptors)
    {
        if (fd == -1)
            continue;

        // We don't reset the event, because the time_running and time_enabled
        // can't be reset anyway and we have to calculate deltas.
        enablePerfEvent(fd);
    }
}

void PerfEventsCounters::finalizeProfileEvents(ProfileEvents::Counters & profile_events)
{
    // Disable all perf events.
    for (auto fd : thread_events_descriptors_holder.descriptors)
    {
        if (fd == -1)
            continue;
        disablePerfEvent(fd);
    }

    // Read the counter values.
    PerfEventValue current_values[NUMBER_OF_RAW_EVENTS];
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        int fd = thread_events_descriptors_holder.descriptors[i];
        if (fd == -1)
            continue;

        constexpr ssize_t bytes_to_read = sizeof(current_values[0]);
        const int bytes_read = read(fd, &current_values[i], bytes_to_read);

        if (bytes_read != bytes_to_read)
        {
            LOG_WARNING(&Poco::Logger::get("PerfEvents"),
                "Can't read event value from file descriptor {}: '{}' ({})",
                fd, errnoToString(errno), errno);
            current_values[i] = {};
        }
    }

    // Actually process counters' values. Track the minimal time that a performance
    // counter was enabled, and the corresponding running time, to give some idea
    // about the amount of counter multiplexing.
    UInt64 min_enabled_time = -1;
    UInt64 running_time_for_min_enabled_time = 0;

    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        int fd = thread_events_descriptors_holder.descriptors[i];
        if (fd == -1)
            continue;

        const PerfEventInfo & info = raw_events_info[i];
        const PerfEventValue & previous_value = previous_values[i];
        const PerfEventValue & current_value = current_values[i];

        // Account for counter multiplexing. time_running and time_enabled are
        // not reset by PERF_EVENT_IOC_RESET, so we don't use it and calculate
        // deltas from old values.
        const auto enabled = current_value.time_enabled - previous_value.time_enabled;
        const auto running = current_value.time_running - previous_value.time_running;
        const UInt64 delta = (current_value.value - previous_value.value)
            * enabled / std::max(1.f, float(running));

        if (min_enabled_time > enabled)
        {
            min_enabled_time = enabled;
            running_time_for_min_enabled_time = running;
        }

        profile_events.increment(info.profile_event, delta);
    }

    // If we had at least one enabled event, also show multiplexing-related
    // statistics.
    if (min_enabled_time != UInt64(-1))
    {
        profile_events.increment(ProfileEvents::PerfMinEnabledTime,
            min_enabled_time);
        profile_events.increment(ProfileEvents::PerfMinEnabledRunningTime,
            running_time_for_min_enabled_time);
    }

    // Store current counter values for the next profiling period.
    memcpy(previous_values, current_values, sizeof(current_values));
}

void PerfEventsCounters::closeEventDescriptors()
{
    thread_events_descriptors_holder.releaseResources();
}

PerfDescriptorsHolder::PerfDescriptorsHolder()
{
    for (int & descriptor : descriptors)
        descriptor = -1;
}

PerfDescriptorsHolder::~PerfDescriptorsHolder()
{
    releaseResources();
}

void PerfDescriptorsHolder::releaseResources()
{
    for (int & descriptor : descriptors)
    {
        if (descriptor == -1)
            continue;

        disablePerfEvent(descriptor);
        releasePerfEvent(descriptor);
        descriptor = -1;
    }
}

}

#else

namespace DB
{

// the functionality is disabled when we are not running on Linux.
PerfEventsCounters current_thread_counters;

}

#endif
