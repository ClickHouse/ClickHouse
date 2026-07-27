#include <Common/Jemalloc.h>

#if USE_JEMALLOC

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void purgeJemallocArenas()
{
    Stopwatch watch;
    mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

void checkJemallocProfilingEnabled()
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

void setJemallocProfileActive(bool value)
{
    checkJemallocProfilingEnabled();
    bool active = true;
    size_t active_size = sizeof(active);
    mallctl("prof.active", &active, &active_size, nullptr, 0);
    if (active == value)
    {
        LOG_TRACE(getLogger("SystemJemalloc"), "Profiling is already {}", active ? "enabled" : "disabled");
        return;
    }

    setJemallocValue("prof.active", value);
    LOG_TRACE(getLogger("SystemJemalloc"), "Profiling is {}", value ? "enabled" : "disabled");
}

std::string flushJemallocProfile(const std::string & file_prefix)
{
    checkJemallocProfilingEnabled();
    char * prefix_buffer;
    size_t prefix_size = sizeof(prefix_buffer);
    int n = mallctl("opt.prof_prefix", &prefix_buffer, &prefix_size, nullptr, 0); // NOLINT
    if (!n && std::string_view(prefix_buffer) != "jeprof")
    {
        LOG_TRACE(getLogger("SystemJemalloc"), "Flushing memory profile with prefix {}", prefix_buffer);
        mallctl("prof.dump", nullptr, nullptr, nullptr, 0);
        return prefix_buffer;
    }

    static std::atomic<size_t> profile_counter{0};
    std::string profile_dump_path = fmt::format("{}.{}.{}.heap", file_prefix, getpid(), profile_counter.fetch_add(1));
    const auto * profile_dump_path_str = profile_dump_path.c_str();

    LOG_TRACE(getLogger("SystemJemalloc"), "Flushing memory profile to {}", profile_dump_path_str);
    mallctl("prof.dump", nullptr, nullptr, &profile_dump_path_str, sizeof(profile_dump_path_str)); // NOLINT
    return profile_dump_path;
}

void setJemallocBackgroundThreads(bool enabled)
{
    setJemallocValue("background_thread", enabled);
}

void setJemallocMaxBackgroundThreads(size_t max_threads)
{
    setJemallocValue("max_background_threads", max_threads);
}

}

#endif
