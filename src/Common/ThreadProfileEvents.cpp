#include "ThreadProfileEvents.h"

#if defined(__linux__)

#include "TaskStatsInfoGetter.h"
#include "ProcfsMetricsProvider.h"
#include "hasLinuxCapability.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <unordered_set>

#include <fcntl.h>
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <cerrno>
#include <sys/types.h>
#include <dirent.h>


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
    // `cpu-clock` is a bit broken according to this: https://stackoverflow.com/a/56967896
    SOFTWARE_EVENT(PERF_COUNT_SW_CPU_CLOCK, PerfCpuClock),
    SOFTWARE_EVENT(PERF_COUNT_SW_TASK_CLOCK, PerfTaskClock),
    SOFTWARE_EVENT(PERF_COUNT_SW_CONTEXT_SWITCHES, PerfContextSwitches),
    SOFTWARE_EVENT(PERF_COUNT_SW_CPU_MIGRATIONS, PerfCpuMigrations),
    SOFTWARE_EVENT(PERF_COUNT_SW_ALIGNMENT_FAULTS, PerfAlignmentFaults),
    SOFTWARE_EVENT(PERF_COUNT_SW_EMULATION_FAULTS, PerfEmulationFaults)
};

#undef HARDWARE_EVENT

std::atomic<PerfEventsCounters::Id> PerfEventsCounters::latest_counters_id = 0;

thread_local PerfDescriptorsHolder PerfEventsCounters::thread_events_descriptors_holder{};
thread_local std::optional<PerfEventsCounters::Id> PerfEventsCounters::current_thread_counters_id = std::nullopt;
thread_local std::optional<PerfEventsCounters::ParsedEvents> PerfEventsCounters::last_parsed_events = std::nullopt;

Logger * PerfEventsCounters::getLogger()
{
    return &Logger::get("PerfEventsCounters");
}

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

using getLoggerFunc = Logger * ();

static void enablePerfEvent(int event_fd, getLoggerFunc getLogger)
{
    if (ioctl(event_fd, PERF_EVENT_IOC_ENABLE, 0))
        LOG_WARNING(getLogger(), "Can't enable perf event with file descriptor {}", event_fd);
}

static void resetPerfEvent(int event_fd, getLoggerFunc getLogger)
{
    if (ioctl(event_fd, PERF_EVENT_IOC_RESET, 0))
        LOG_WARNING(getLogger(), "Can't reset perf event with file descriptor {}",
                    event_fd);
}

static void disablePerfEvent(int event_fd, getLoggerFunc getLogger)
{
    if (ioctl(event_fd, PERF_EVENT_IOC_DISABLE, 0))
        LOG_WARNING(getLogger(), "Can't disable perf event with file descriptor {}"  , event_fd);
}

static void releasePerfEvent(int event_fd, getLoggerFunc getLogger)
{
    if (close(event_fd))
    {
        LOG_WARNING(getLogger(), "Can't close perf event file descriptor {}: {} ({})", event_fd, errno, strerror(errno));
    }
}

static bool validatePerfEventDescriptor(int & fd, getLoggerFunc getLogger)
{
    if (fcntl(fd, F_GETFL) != -1)
        return true;

    if (errno == EBADF)
    {
        LOG_WARNING(getLogger(), "Event descriptor {} was closed from the outside; reopening", fd);
    }
    else
    {
        LOG_WARNING(getLogger(), "Error while checking availability of event descriptor {}: {} ({})", fd, strerror(errno), errno);

        disablePerfEvent(fd, getLogger);
        releasePerfEvent(fd, getLogger);
    }

    fd = -1;
    return false;
}

bool PerfEventsCounters::processThreadLocalChanges(const std::string & needed_events_list)
{
    std::vector<size_t> valid_event_indices = eventIndicesFromString(needed_events_list);

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
            if (old_one && !validatePerfEventDescriptor(thread_events_descriptors_holder.descriptors[i], getLogger))
                events_to_open.push_back(i);
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
        disablePerfEvent(fd, getLogger);
        releasePerfEvent(fd, getLogger);
        fd = -1;
    }

    if (events_to_open.empty())
        return true;

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
        LOG_WARNING(getLogger(), "Not enough permissions to record perf events: "
            "perf_event_paranoid = {} and CAP_SYS_ADMIN = 0",
            perf_event_paranoid);
        return false;
    }

    // check file descriptors limit
    rlimit64 limits{};
    if (getrlimit64(RLIMIT_NOFILE, &limits))
    {
        LOG_WARNING(getLogger(), "Unable to get rlimit: {} ({})", strerror(errno),
                    errno);
        return false;
    }
    UInt64 maximum_open_descriptors = limits.rlim_cur;

    const size_t opened_descriptors = std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator());

    UInt64 fd_count_afterwards = opened_descriptors + events_to_open.size();
    UInt64 threshold = static_cast<UInt64>(maximum_open_descriptors * FILE_DESCRIPTORS_THRESHOLD);
    if (fd_count_afterwards > threshold)
    {
        LOG_WARNING(getLogger(), "Can't measure perf events as the result number of file descriptors ({}) is more than the current threshold ({} = {} * {})",
            fd_count_afterwards, threshold, maximum_open_descriptors,
            FILE_DESCRIPTORS_THRESHOLD);
        return false;
    }

    // open descriptors for new events
    for (size_t i : events_to_open)
    {
        const PerfEventInfo & event_info = raw_events_info[i];
        int & fd = thread_events_descriptors_holder.descriptors[i];
        // disable by default to add as little extra time as possible
        fd = openPerfEventDisabled(perf_event_paranoid, has_cap_sys_admin, event_info.event_type, event_info.event_config);

        if (fd == -1)
        {
            LOG_WARNING(getLogger(), "Perf event is unsupported: {}"
                " (event_type={}, event_config={})",
                event_info.settings_name, event_info.event_type,
                event_info.event_config);
        }
    }

    return true;
}

// Parse comma-separated list of event names. Empty or 'all' means all available
// events.
// TODO add validation to setting
std::vector<size_t> PerfEventsCounters::eventIndicesFromString(const std::string & events_list)
{
    std::unordered_set<std::string> requested_events;
    std::istringstream iss(events_list);
    std::string event_name;
    while (std::getline(iss, event_name, ','))
    {
        requested_events.insert(event_name);
    }

    std::vector<size_t> result;
    result.reserve(PerfEventsCounters::NUMBER_OF_RAW_EVENTS);
    if (requested_events.size() == 0
        || requested_events.count("all") > 0)
    {
        for (size_t i = 0; i < PerfEventsCounters::NUMBER_OF_RAW_EVENTS; ++i)
        {
            result.push_back(i);
        }
        return result;
    }

    for (size_t i = 0; i < PerfEventsCounters::NUMBER_OF_RAW_EVENTS; ++i)
    {
        if (requested_events.count(
            PerfEventsCounters::raw_events_info[i].settings_name) > 0)
        {
            result.push_back(i);
        }
    }

    return result;
}

void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters & counters, const std::string & events_list)
{
    if (current_thread_counters_id.has_value())
    {
        if (current_thread_counters_id != counters.id)
            LOG_WARNING(getLogger(), "Only one instance of `PerfEventsCounters` can be used on the thread");
        return;
    }

    if (!processThreadLocalChanges(events_list))
        return;

    for (int fd : thread_events_descriptors_holder.descriptors)
    {
        if (fd == -1)
            continue;

        resetPerfEvent(fd, getLogger);
        enablePerfEvent(fd, getLogger);
    }

    current_thread_counters_id = counters.id;
}

void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events)
{
    if (current_thread_counters_id != counters.id)
        return;

    const auto old_values = counters.raw_event_values;

    // only read counters here to have as little overhead for processing as possible
    for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
    {
        int fd = counters.thread_events_descriptors_holder.descriptors[i];
        if (fd == -1)
            continue;

        constexpr ssize_t bytes_to_read = sizeof(counters.raw_event_values[0]);
        const int bytes_read = read(fd, &counters.raw_event_values[i],
            bytes_to_read);

        if (bytes_read != bytes_to_read)
        {
            LOG_WARNING(getLogger(), "Can't read event value from file descriptor: {}", fd);
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
        const PerfEventValue & old_value = old_values[i];
        const PerfEventValue & new_value = counters.raw_event_values[i];

        // Account for counter multiplexing. time_running/time_enabled are
        // not reset by PERF_EVENT_IOC_RESET, so we have to calculate deltas
        // from old values.
        profile_events.increment(info.profile_event,
            (new_value.value - old_value.value)
                * (new_value.time_running - old_value.time_running)
                / std::max(1.f,
                    float(new_value.time_enabled - old_value.time_enabled)));

        disablePerfEvent(fd, getLogger);
    }

    current_thread_counters_id.reset();
}

void PerfEventsCounters::closeEventDescriptors()
{
    if (current_thread_counters_id.has_value())
    {
        LOG_WARNING(getLogger(), "Tried to close event descriptors while measurements are in process; ignoring");
        return;
    }

    thread_events_descriptors_holder.releaseResources();
}

PerfEventsCounters::PerfEventsCounters(): id(latest_counters_id++) {}

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

        disablePerfEvent(descriptor, getLogger);
        releasePerfEvent(descriptor, getLogger);
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
    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters &, const std::string &) {}
    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters &, ProfileEvents::Counters &) {}
    void PerfEventsCounters::closeEventDescriptors() {}
}

#endif
