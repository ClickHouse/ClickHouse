#include <Storages/MergeTree/IMergeTreeCleanupThread.h>

#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ZooKeeper/KeeperException.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 cleanup_delay_period;
    extern const MergeTreeSettingsUInt64 cleanup_delay_period_random_add;
    extern const MergeTreeSettingsUInt64 cleanup_thread_preferred_points_per_iteration;
    extern const MergeTreeSettingsUInt64 max_cleanup_delay_period;
}

IMergeTreeCleanupThread::IMergeTreeCleanupThread(MergeTreeData & data_)
    : data(data_)
    , log_name(data.getStorageID().getFullTableName() + " (CleanupThread)")
    , log(getLogger(log_name))
    , sleep_ms((*data.getSettings())[MergeTreeSetting::cleanup_delay_period] * 1000)
{
    task = data.getContext()->getSchedulePool().createTask(data.getStorageID(), log_name, [this] { run(); });
}

IMergeTreeCleanupThread::~IMergeTreeCleanupThread() = default;

void IMergeTreeCleanupThread::start()
{
    task->activateAndSchedule();
}

void IMergeTreeCleanupThread::wakeup()
{
    task->schedule();
}

void IMergeTreeCleanupThread::stop()
{
    task->deactivate();
}

void IMergeTreeCleanupThread::wakeupEarlierIfNeeded()
{
    /// It may happen that the tables was idle for a long time, but then a user started to aggressively insert (or mutate) data.
    /// In this case, sleep_ms was set to the highest possible value, the task is not going to wake up soon,
    /// but the number of objects to clean up is growing. We need to wakeup the task earlier.
    auto storage_settings = data.getSettings();
    if (!(*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration])
        return;

    /// The number of other objects (logs, blocks, etc) is usually correlated with the number of Outdated parts.
    /// Do not wake up unless we have too many.
    size_t number_of_outdated_objects = data.getOutdatedPartsCount();
    if (number_of_outdated_objects < (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration] * 2)
        return;

    /// A race condition is possible here, but it's okay
    if (is_running.load(std::memory_order_relaxed))
        return;

    /// Do not re-check all parts too often (avoid constantly calling getNumberOfOutdatedPartsWithExpiredRemovalTime())
    if (!wakeup_check_timer.compareAndRestart((*storage_settings)[MergeTreeSetting::cleanup_delay_period] / 4.0))
        return;

    UInt64 prev_run_timestamp_ms = prev_cleanup_timestamp_ms.load(std::memory_order_relaxed);
    UInt64 now_ms = clock_gettime_ns_adjusted(prev_run_timestamp_ms * 1'000'000) / 1'000'000;
    if (!prev_run_timestamp_ms || now_ms <= prev_run_timestamp_ms)
        return;

    /// Don't run it more often than cleanup_delay_period
    UInt64 seconds_passed = (now_ms - prev_run_timestamp_ms) / 1000;
    if (seconds_passed < (*storage_settings)[MergeTreeSetting::cleanup_delay_period])
        return;

    /// Do not count parts that cannot be removed anyway. Do not wake up unless we have too many.
    number_of_outdated_objects = data.getNumberOfOutdatedPartsWithExpiredRemovalTime();
    if (number_of_outdated_objects < (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration] * 2)
        return;

    LOG_TRACE(
        log,
        "Waking up cleanup thread because there are {} outdated objects and previous cleanup finished {}s ago",
        number_of_outdated_objects,
        seconds_passed);

    wakeup();
}

void IMergeTreeCleanupThread::run()
{
    if (cleanup_blocker.isCancelled())
    {
        LOG_TRACE(LogFrequencyLimiter(log, 30), "Cleanup is cancelled, exiting");
        return;
    }

    SCOPE_EXIT({ is_running.store(false, std::memory_order_relaxed); });
    is_running.store(true, std::memory_order_relaxed);

    auto storage_settings = data.getSettings();

    Float32 cleanup_points = 0;
    try
    {
        cleanup_points = iterate();
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    UInt64 prev_timestamp = prev_cleanup_timestamp_ms.load(std::memory_order_relaxed);
    UInt64 now_ms = clock_gettime_ns_adjusted(prev_timestamp * 1'000'000) / 1'000'000;

    /// Do not adjust sleep_ms on the first run after starting the server
    if (prev_timestamp && (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration])
    {
        /// We don't want to run the task too often when the table was barely changed and there's almost nothing to cleanup.
        /// But we cannot simply sleep max_cleanup_delay_period (300s) when nothing was cleaned up and cleanup_delay_period (30s)
        /// when we removed something, because inserting one part per 30s will lead to running cleanup each 30s just to remove one part.
        /// So we need some interpolation based on preferred batch size.
        auto expected_cleanup_points = (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration];

        /// How long should we sleep to remove cleanup_thread_preferred_points_per_iteration on the next iteration?
        Float32 ratio = cleanup_points / expected_cleanup_points;
        if (ratio == 0)
            sleep_ms = (*storage_settings)[MergeTreeSetting::max_cleanup_delay_period] * 1000;
        else
            sleep_ms = static_cast<UInt64>(sleep_ms / ratio);

        sleep_ms = std::clamp(
            sleep_ms,
            (*storage_settings)[MergeTreeSetting::cleanup_delay_period] * 1000,
            (*storage_settings)[MergeTreeSetting::max_cleanup_delay_period] * 1000);

        UInt64 interval_ms = now_ms - prev_timestamp;
        LOG_TRACE(
            log,
            "Scheduling next cleanup after {}ms (points: {}, interval: {}ms, ratio: {}, points per minute: {})",
            sleep_ms,
            cleanup_points,
            interval_ms,
            ratio,
            cleanup_points / interval_ms * 60'000);
    }
    prev_cleanup_timestamp_ms.store(now_ms, std::memory_order_relaxed);

    sleep_ms
        += std::uniform_int_distribution<UInt64>(0, (*storage_settings)[MergeTreeSetting::cleanup_delay_period_random_add] * 1000)(rng);
    task->scheduleAfter(sleep_ms);
}

}
