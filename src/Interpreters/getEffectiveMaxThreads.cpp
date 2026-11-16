#include <Interpreters/getEffectiveMaxThreads.h>

#include <Core/Settings.h>
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

size_t getEffectiveMaxThreads(const Settings & settings)
{
    size_t max_threads = getMaxThreadsForAvailableMemory(settings[Setting::max_threads], settings[Setting::max_threads_min_free_memory_per_thread]);

    /// An explicitly set `max_threads` is respected as is.
    if (max_threads <= 1 || !settings[Setting::max_threads].is_auto
        || !settings[Setting::use_concurrency_control] || !settings[Setting::use_concurrency_control_for_max_threads])
        return max_threads;

    SlotCount fair_share = ConcurrencyControl::instance().getFairShare();
    if (fair_share >= max_threads)
        return max_threads;

    LOG_TRACE(getLogger("ConcurrencyControl"), "Lower number of threads for query to the fair share of CPU slots {} ({} requested)", fair_share, max_threads);
    return fair_share;
}

}
