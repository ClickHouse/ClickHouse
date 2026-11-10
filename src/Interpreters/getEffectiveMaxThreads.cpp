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

QueryMaxThreadsSnapshot::Values QueryMaxThreadsSnapshot::take()
{
    return {getServerFreeMemory(), ConcurrencyControl::instance().getFairShare()};
}

const QueryMaxThreadsSnapshot::Values & QueryMaxThreadsSnapshot::get()
{
    std::call_once(once, [this] { values = take(); });
    return values;
}

namespace
{

/// A context that is not a query context (e.g. the global context) has no snapshot: it reads the current state.
QueryMaxThreadsSnapshot::Values getValues(const Context & context)
{
    if (const auto & snapshot = context.getQueryMaxThreadsSnapshot())
        return snapshot->get();
    return QueryMaxThreadsSnapshot::take();
}

}

size_t limitThreadsByFreeMemory(const Context & context, size_t threads, UInt64 min_free_per_thread)
{
    if (min_free_per_thread == 0 || threads <= 1)
        return threads;
    return getMaxThreadsForFreeMemory(threads, min_free_per_thread, getValues(context).free_memory);
}

size_t getEffectiveMaxThreads(const Context & context)
{
    const Settings & settings = context.getSettingsRef();
    size_t max_threads = limitThreadsByFreeMemory(context, settings[Setting::max_threads], settings[Setting::max_threads_min_free_memory_per_thread]);

    /// An explicitly set `max_threads` is respected as is.
    if (max_threads <= 1 || !settings[Setting::max_threads].is_auto
        || !settings[Setting::use_concurrency_control] || !settings[Setting::use_concurrency_control_for_max_threads])
        return max_threads;

    size_t fair_share = getValues(context).cpu_share;
    if (fair_share >= max_threads)
        return max_threads;

    LOG_TRACE(getLogger("ConcurrencyControl"), "Lower number of threads for query to the fair share of CPU slots {} ({} requested)", fair_share, max_threads);
    return fair_share;
}

}
