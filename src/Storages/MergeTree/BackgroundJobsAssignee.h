#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/IStorage.h>

#include <pcg_random.hpp>
#include <Interpreters/StorageID.h>


namespace DB
{

/// Settings for background tasks scheduling. Each background assignee has one
/// BackgroundSchedulingPoolTask and depending on execution result may put this
/// task to sleep according to settings. Look at scheduleTask function for details.
struct BackgroundTaskSchedulingSettings
{
    double thread_sleep_seconds_random_part = 1.0;
    double thread_sleep_seconds_if_nothing_to_do = 0.1;
    double task_sleep_seconds_when_no_work_max = 600;
    /// For exponential backoff.
    double task_sleep_seconds_when_no_work_multiplier = 1.1;

    double task_sleep_seconds_when_no_work_random_part = 1.0;

     /// Deprecated settings, don't affect background execution
    double thread_sleep_seconds = 10;
    double task_sleep_seconds_when_no_work_min = 10;
};

class MergeTreeData;
class BackgroundJobsAssignee;

class IBackgroundOperation
{
public:
    virtual bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) = 0;
    virtual bool scheduleDataMovingJob(BackgroundJobsAssignee & assignee) = 0;
    virtual bool scheduleStreamingJob(BackgroundJobsAssignee & /*assignee*/) { return false; }
    virtual Int32 getBiasBackoffSeconds() const { return 0; }

    virtual ~IBackgroundOperation() = default;
};

class BackgroundJobsAssignee : public WithContext
{
public:
    /// In case of ReplicatedMergeTree the first assignee will be responsible for
    /// polling the replication queue and schedule operations according to the LogEntry type
    /// e.g. merges, mutations and fetches. The same will be for Plain MergeTree except there is no
    /// replication queue, so we will just scan parts and decide what to do.
    /// Moving operations are the same for all types of MergeTree and also have their own timetable.
    enum class Type : uint8_t
    {
        DataProcessing,
        Moving,
        Streaming,
    };
    Type type{Type::DataProcessing};

    /// Allocates the scheduling task if needed and activates it. Idempotent.
    /// Returns true if the task was created by this call, so that the caller can `finish` exactly
    /// the assignees it started when the operation that started them is rolled back.
    /// All or nothing: if activating a task created by this call throws, the task is destroyed
    /// again before the exception leaves, so the assignee is exactly as it was before the call.
    bool start();
    void trigger();
    void postpone();
    void finish();

    /// Update the cached storage ID after a table rename,
    /// so that finish() can correctly find tasks belonging to this storage.
    void updateStorageID(const StorageID & new_id);

    bool scheduleMergeMutateTask(ExecutableTaskPtr merge_task);
    bool scheduleFetchTask(ExecutableTaskPtr fetch_task);
    bool scheduleMoveTask(ExecutableTaskPtr move_task);
    bool scheduleCommonTask(ExecutableTaskPtr common_task, bool need_trigger);

    /// Just call finish
    ~BackgroundJobsAssignee();

    BackgroundJobsAssignee(
        IBackgroundOperation & data_,
        const StorageID & storage_id_,
        Type type,
        ContextPtr global_context_);

private:
    IBackgroundOperation & data;
    StorageID storage_id TSA_GUARDED_BY(storage_id_mutex);
    mutable std::mutex storage_id_mutex;

    /// Useful for random backoff timeouts generation
    pcg64 rng;

    /// How many times execution of background job failed or we have
    /// no new jobs.
    size_t no_work_done_count = 0;

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePoolTaskHolder holder;
    /// Mutex for thread safety
    std::mutex holder_mutex;

    /// Settings for execution control of background scheduling task
    BackgroundTaskSchedulingSettings sleep_settings;

    static String toString(Type type);

    /// Must be called under `holder_mutex`. Returns true if the task was created by this call.
    /// Takes the storage ID as an argument because it must be read before `holder_mutex` is taken,
    /// so that `holder_mutex` and `storage_id_mutex` are never nested.
    bool createHolderIfNeeded(const StorageID & current_storage_id);

    /// Function that executes in background scheduling pool
    void threadFunc();

    BackgroundTaskSchedulingSettings getSettings() const;

    StorageID getStorageID() const;
};
}
