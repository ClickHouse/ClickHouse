#include <Common/Jemalloc.h>

#if USE_JEMALLOC

#    include <Common/Exception.h>
#    include <Common/FramePointers.h>
#    include <Common/MemoryTracker.h>
#    include <Common/StackTrace.h>
#    include <Common/Stopwatch.h>
#    include <Common/TraceSender.h>
#    include <Common/logger_useful.h>

#    define STRINGIFY_HELPER(x) #x
#    define STRINGIFY(x) STRINGIFY_HELPER(x)

namespace ProfileEvents
{
extern const Event MemoryAllocatorPurge;
extern const Event MemoryAllocatorPurgeTimeMicroseconds;
extern const Event JemallocFailedAllocationSampleTracking;
extern const Event JemallocFailedDeallocationSampleTracking;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Jemalloc
{

void purgeArenas()
{
    Stopwatch watch;
    mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

void checkProfilingEnabled()
{
    bool active = true;
    size_t active_size = sizeof(active);
    mallctl("opt.prof", &active, &active_size, nullptr, 0);

    if (!active)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ClickHouse was started without enabling profiling for jemalloc. To use jemalloc's profiler, following env variable should be "
            "set: MALLOC_CONF=background_thread:true,prof:true");
}

std::string_view flushProfile(const char * file_prefix)
{
    checkProfilingEnabled();
    char * prefix_buffer;
    size_t prefix_size = sizeof(prefix_buffer);
    int n = mallctl("opt.prof_prefix", &prefix_buffer, &prefix_size, nullptr, 0); // NOLINT
    if (!n && std::string_view(prefix_buffer) != "jeprof")
    {
        mallctl("prof.dump", nullptr, nullptr, nullptr, 0);
        return getLastFlushProfileForThread();
    }

    static std::atomic<size_t> profile_counter{0};
    std::string profile_dump_path = fmt::format("{}.{}.{}.heap", file_prefix, getpid(), profile_counter.fetch_add(1));
    const auto * profile_dump_path_str = profile_dump_path.c_str();

    mallctl("prof.dump", nullptr, nullptr, &profile_dump_path_str, sizeof(profile_dump_path_str)); // NOLINT
    return getLastFlushProfileForThread();
}

void setBackgroundThreads(bool enabled)
{
    setValue("background_thread", enabled);
}

void setMaxBackgroundThreads(size_t max_threads)
{
    setValue("max_background_threads", max_threads);
}

void setProfileSamplingRate(size_t lg_prof_sample)
{
    size_t current = getValue<size_t>("prof.lg_sample");
    if (current == lg_prof_sample)
        return;

    mallctl("prof.reset", nullptr, nullptr, &lg_prof_sample, sizeof(lg_prof_sample));
}


namespace
{

std::atomic<bool> collect_global_profiles_in_trace_log = false;
thread_local bool collect_local_profiles_in_trace_log = false;

void jemallocAllocationTracker(const void * ptr, size_t /*size*/, void ** backtrace, unsigned backtrace_length, size_t usize)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!collect_local_profiles_in_trace_log && !collect_global_profiles_in_trace_log)
        return;

    try
    {
        FramePointers frame_pointers;
        auto stacktrace_size = std::min<size_t>(backtrace_length, frame_pointers.size());
        memcpy(frame_pointers.data(), backtrace, stacktrace_size * sizeof(void *)); // NOLINT(bugprone-bitwise-pointer-cast)
        TraceSender::send(
            TraceType::JemallocSample,
            StackTrace(std::move(frame_pointers), stacktrace_size),
            TraceSender::Extras{
                .size = static_cast<Int64>(usize),
                .ptr = const_cast<void *>(ptr),
                .memory_blocked_context = MemoryTrackerBlockerInThread::getLevel(),
            });
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::JemallocFailedAllocationSampleTracking);
    }
}

void jemallocDeallocationTracker(const void * ptr, unsigned usize)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!collect_local_profiles_in_trace_log && !collect_global_profiles_in_trace_log)
        return;

    try
    {
        TraceSender::send(
            TraceType::JemallocSample,
            StackTrace(),
            TraceSender::Extras{
                .size = -static_cast<Int64>(usize),
                .ptr = const_cast<void *>(ptr),
                .memory_blocked_context = MemoryTrackerBlockerInThread::getLevel(),
            });
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::JemallocFailedDeallocationSampleTracking);
    }
}

thread_local std::array<char, 256> last_flush_profile_buffer;
thread_local std::string_view last_flush_profile;

void setLastFlushProfile(const char * filename)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto last_flush_profile_size = std::min(last_flush_profile_buffer.size(), strlen(filename));
    std::memcpy(last_flush_profile_buffer.data(), filename, last_flush_profile_size);
    last_flush_profile = std::string_view{last_flush_profile_buffer.data(), last_flush_profile_size};
}

}

void setCollectLocalProfileSamplesInTraceLog(bool value)
{
    collect_local_profiles_in_trace_log = value;
}

void setup(
    bool enable_global_profiler,
    bool enable_background_threads,
    size_t max_background_threads_num,
    bool collect_global_profile_samples_in_trace_log,
    size_t profiler_sampling_rate)
{
    if (enable_global_profiler)
    {
        getThreadProfileInitMib().setValue(true);
        getThreadProfileActiveMib().setValue(true);
    }

    setBackgroundThreads(enable_background_threads);

    if (max_background_threads_num)
        setValue("max_background_threads", max_background_threads_num);

    if (profiler_sampling_rate != default_profiler_sampling_rate)
        setProfileSamplingRate(profiler_sampling_rate);

    collect_global_profiles_in_trace_log = collect_global_profile_samples_in_trace_log;
    setValue("experimental.hooks.prof_sample", &jemallocAllocationTracker);
    setValue("experimental.hooks.prof_sample_free", &jemallocDeallocationTracker);
    setValue("experimental.hooks.prof_dump", &setLastFlushProfile);
}

void verifySetup(
    bool enable_global_profiler,
    bool enable_background_threads,
    size_t max_background_threads_num,
    bool collect_global_profile_samples_in_trace_log,
    size_t profiler_sampling_rate)
{
    /// Verify that the settings match what was configured by the earlier `setup` call.
    /// Catch mismatches between server settings defaults and the manually defined config names in `BaseDaemon`.
    auto log_warning = [](std::string_view setting)
    {
        chassert(false, fmt::format("Jemalloc settings mismatch: `{}` differs between BaseDaemon and server settings", setting));
        LOG_WARNING(
            &Poco::Logger::get("Jemalloc"), "Jemalloc settings mismatch: `{}` differs between BaseDaemon and server settings", setting);
    };

    if (getThreadProfileInitMib().getValue() != enable_global_profiler)
        log_warning(config_enable_global_profiler);
    if (getValue<bool>("background_thread") != enable_background_threads)
        log_warning(config_enable_background_threads);
    if (max_background_threads_num && getValue<size_t>("max_background_threads") != max_background_threads_num)
        log_warning(config_max_background_threads_num);
    if (profiler_sampling_rate != default_profiler_sampling_rate && getValue<size_t>("prof.lg_sample") != profiler_sampling_rate)
        log_warning(config_profiler_sampling_rate);
    if (collect_global_profiles_in_trace_log != collect_global_profile_samples_in_trace_log)
        log_warning(config_collect_global_profile_samples_in_trace_log);
}


const MibCache<bool> & getThreadProfileActiveMib()
{
    static MibCache<bool> thread_profile_active("thread.prof.active");
    return thread_profile_active;
}

const MibCache<bool> & getThreadProfileInitMib()
{
    static MibCache<bool> thread_profile_init("prof.thread_active_init");
    return thread_profile_init;
}

std::string_view getLastFlushProfileForThread()
{
    return last_flush_profile;
}

}

}

#endif
