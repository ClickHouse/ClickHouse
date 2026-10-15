#include <Interpreters/getEffectiveMaxThreads.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/ConcurrencyControl.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool use_concurrency_control_for_max_threads;
}

std::optional<Int64> QueryMaxThreadsSnapshot::getFreeMemory()
{
    std::call_once(free_memory_once, [this] { free_memory = getServerFreeMemory(); });
    return free_memory;
}

size_t QueryMaxThreadsSnapshot::getCPUShare()
{
    std::call_once(cpu_share_once, [this] { cpu_share = ConcurrencyControl::instance().getFairShare(); });
    return cpu_share;
}

namespace
{

/// A context that is not a query context (e.g. the global context) has no snapshot: it reads the current state.
std::optional<Int64> getFreeMemory(const Context & context)
{
    if (const auto & snapshot = context.getQueryMaxThreadsSnapshot())
        return snapshot->getFreeMemory();
    return getServerFreeMemory();
}

size_t getCPUShare(const Context & context)
{
    if (const auto & snapshot = context.getQueryMaxThreadsSnapshot())
        return snapshot->getCPUShare();
    return ConcurrencyControl::instance().getFairShare();
}

}

size_t limitThreadsByFreeMemory(const Context & context, size_t threads, UInt64 min_free_per_thread)
{
    if (min_free_per_thread == 0 || threads <= 1)
        return threads;
    return getMaxThreadsForFreeMemory(threads, min_free_per_thread, getFreeMemory(context));
}

size_t getEffectiveMaxThreads(const Context & context)
{
    const Settings & settings = context.getSettingsRef();
    size_t max_threads = limitThreadsByFreeMemory(context, settings[Setting::max_threads], settings[Setting::max_threads_min_free_memory_per_thread]);

    /// An explicitly set `max_threads` is respected as is.
    if (max_threads <= 1 || !settings[Setting::max_threads].is_auto
        || !settings[Setting::use_concurrency_control] || !settings[Setting::use_concurrency_control_for_max_threads])
        return max_threads;

    size_t fair_share = getCPUShare(context);
    if (fair_share >= max_threads)
        return max_threads;

    LOG_TRACE(getLogger("ConcurrencyControl"), "Lower number of threads for query to the fair share of CPU slots {} ({} requested)", fair_share, max_threads);
    return fair_share;
}

}
