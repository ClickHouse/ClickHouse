#include <Disks/DiskObjectStorage/Replication/BlobKillerThread.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <Interpreters/Context.h>

#include <Core/BackgroundSchedulePool.h>

#include <Common/DelayWithJitter.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <cmath>
#include <utility>
#include <vector>
#include <ranges>

namespace ProfileEvents
{
    extern const Event BlobKillerThreadRuns;
    extern const Event BlobKillerThreadLockedBlobs;
    extern const Event BlobKillerThreadRemoveTasks;
    extern const Event BlobKillerThreadRemovedBlobs;
    extern const Event BlobKillerThreadRecordedBlobs;
    extern const Event BlobKillerThreadLockBlobsErrors;
    extern const Event BlobKillerThreadRemoveBlobsErrors;
    extern const Event BlobKillerThreadRecordBlobsErrors;
}

namespace CurrentMetrics
{
    extern const Metric BlobKillerThreads;
    extern const Metric BlobKillerThreadsActive;
    extern const Metric BlobKillerThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

constexpr int64_t DEFAULT_RESCHEDULE_INTERVAL_SEC = 1;
constexpr int64_t DEFAULT_METADATA_REQUEST_SIZE = 1600;
constexpr int64_t DEFAULT_MAX_METADATA_REQUEST_SIZE = 64000;
constexpr int64_t DEFAULT_THREADS_COUNT = 16;
constexpr int64_t DEFAULT_MAX_THREADS_COUNT = 64;
constexpr int64_t DEFAULT_BLOBS_IN_TASK = 100;
constexpr int64_t DEFAULT_MAX_BLOBS_IN_TASK = 1000;
constexpr int64_t BLOBS_IN_TASK_HARDWARE_LIMIT = 1000; /// S3 API prevents deletion if the value exceeds this limit

struct BlobKillerEffectiveParams
{
    int64_t metadata_request_batch = 0;
    int64_t blobs_in_task = 0;
    int64_t threads_count = 0;
};

BlobKillerEffectiveParams calculateBlobKillerEffectiveParams(
    int64_t dead_queue_estimate,
    int64_t base_batch,
    int64_t max_batch,
    int64_t base_blobs_in_task,
    int64_t max_blobs_in_task,
    int64_t base_threads,
    int64_t max_threads)
{
    if (dead_queue_estimate <= base_batch || base_batch == max_batch)
        return {base_batch, base_blobs_in_task, base_threads};

    const int64_t batch = std::clamp(dead_queue_estimate, base_batch, max_batch);
    const double scale = static_cast<double>(batch - base_batch) / static_cast<double>(max_batch - base_batch);

    const int64_t threads = std::clamp(static_cast<int64_t>(std::ceil(static_cast<double>(base_threads) + scale * static_cast<double>(max_threads - base_threads))), base_threads, max_threads);
    const int64_t blobs = std::clamp((batch + threads - 1) / threads, base_blobs_in_task, max_blobs_in_task);

    return {batch, blobs, threads};
}

IMetadataStorage::BlobsToRemove findBlobsToRemove(
    size_t request_batch,
    const ClusterConfigurationPtr & cluster,
    const MetadataStoragePtr & metadata_storage,
    const LoggerPtr & log) noexcept
{
    try
    {
        auto blobs_to_remove = metadata_storage->getBlobsToRemove(cluster, request_batch);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadLockedBlobs, blobs_to_remove.size());
        return blobs_to_remove;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadLockBlobsErrors);
        return {};
    }
}

std::vector<std::pair<Location, StoredObjects>> sliceIntoRemoveTasks(
    const IMetadataStorage::BlobsToRemove & blobs_to_remove,
    size_t blobs_in_task) noexcept
{
    std::vector<std::pair<Location, StoredObjects>> tasks;

    std::unordered_map<Location, StoredObjects> incomplete_tasks;
    for (const auto & [blob, locations_cleanup_list] : blobs_to_remove)
    {
        for (const auto & location : locations_cleanup_list)
        {
            auto & incomplete_task_blobs = incomplete_tasks[location];
            incomplete_task_blobs.push_back(blob);

            if (incomplete_task_blobs.size() >= blobs_in_task)
                tasks.emplace_back(location, std::exchange(incomplete_task_blobs, {}));
        }
    }

    for (auto & [location, blobs] : incomplete_tasks)
        if (!blobs.empty())
            tasks.emplace_back(location, std::move(blobs));

    return tasks;
}

bool removeBlobsBatch(
    const Location & location,
    const StoredObjects & remove_batch,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    try
    {
        LOG_TRACE(log, "Removing {} blobs from '{}' location", remove_batch.size(), location);

        object_storages->takePointingTo(location)->removeObjectsIfExist(remove_batch);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadRemovedBlobs, remove_batch.size());

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadRemoveBlobsErrors);
        return false;
    }
}

std::vector<std::shared_ptr<ThreadPoolCallbackRunnerLocal<bool>::Task>> scheduleRemovalTasks(
    ThreadPoolCallbackRunnerLocal<bool> & remove_tasks_runner,
    const std::vector<std::pair<Location, StoredObjects>> & batches_to_remove,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    std::vector<std::shared_ptr<ThreadPoolCallbackRunnerLocal<bool>::Task>> running_removals;

    try
    {
        for (const auto & [location, remove_batch] : batches_to_remove)
            running_removals.push_back(remove_tasks_runner.enqueueAndGiveOwnership([&location, &remove_batch, &object_storages, &log]()
            {
                return removeBlobsBatch(location, remove_batch, object_storages, log);
            }));
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    return running_removals;
}

void recordBlobsRemoval(
    const StoredObjects & removed_blobs,
    const MetadataStoragePtr & metadata_storage,
    const LoggerPtr & log) noexcept
{
    try
    {
        int64_t recorded_count = metadata_storage->recordAsRemoved(removed_blobs);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadRecordedBlobs, recorded_count);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobKillerThreadRecordBlobsErrors);
    }
}

int64_t removeBlobs(
    IMetadataStorage::BlobsToRemove blobs_to_remove,
    size_t blobs_in_task,
    ThreadPoolCallbackRunnerLocal<bool> & remove_tasks_runner,
    const MetadataStoragePtr & metadata_storage,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    if (blobs_to_remove.empty())
        return 0;

    auto tasks = sliceIntoRemoveTasks(blobs_to_remove, blobs_in_task);
    ProfileEvents::increment(ProfileEvents::BlobKillerThreadRemoveTasks, tasks.size());
    LOG_TRACE(log, "Distributed removal of {} blobs into {} tasks", blobs_to_remove.size(), tasks.size());

    auto removals = scheduleRemovalTasks(remove_tasks_runner, tasks, object_storages, log);
    ThreadPoolCallbackRunnerLocal<bool>::waitForAllToFinish(removals);

    for (auto [removal, task] : std::views::zip(removals, tasks))
    {
        const auto & [location, remove_batch] = task;
        const bool is_removed = removal->future.get();
        if (is_removed)
            for (const auto & removed_blob : remove_batch)
                blobs_to_remove[removed_blob].erase(location);
    }

    /// If removal from some locations failed we should not mark this blob in metadata storage.
    StoredObjects blobs_removed_from_all_locations;
    for (const auto & [blob, locations_cleanup_list] : blobs_to_remove)
        if (locations_cleanup_list.empty())
            blobs_removed_from_all_locations.push_back(blob);

    recordBlobsRemoval(blobs_removed_from_all_locations, metadata_storage, log);
    return blobs_removed_from_all_locations.size();
}

int64_t executeBlobsCleanup(
    size_t max_to_remove,
    size_t blobs_in_task,
    ThreadPoolCallbackRunnerLocal<bool> & remove_tasks_runner,
    const ClusterConfigurationPtr & cluster,
    const MetadataStoragePtr & metadata,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    ProfileEvents::increment(ProfileEvents::BlobKillerThreadRuns);
    auto blobs_to_remove = findBlobsToRemove(max_to_remove, cluster, metadata, log);
    return removeBlobs(std::move(blobs_to_remove), blobs_in_task, remove_tasks_runner, metadata, object_storages, log);
}

}

BlobKillerThread::BlobKillerThread(
    std::string disk_name_,
    ContextPtr context,
    ClusterConfigurationPtr cluster_,
    MetadataStoragePtr metadata_storage_,
    ObjectStorageRouterPtr object_storages_,
    std::shared_ptr<BlobKillerThread> wrapped_blob_killer_)
    : disk_name(std::move(disk_name_))
    , cluster(std::move(cluster_))
    , metadata_storage(std::move(metadata_storage_))
    , object_storages(std::move(object_storages_))
    , wrapped_blob_killer(std::move(wrapped_blob_killer_))
    , log(getLogger(fmt::format("{}::BlobKillerThread", disk_name)))
    , remove_tasks_pool(CurrentMetrics::BlobKillerThreads, CurrentMetrics::BlobKillerThreadsActive, CurrentMetrics::BlobKillerThreadsScheduled, 0, 0, 0)
    , remove_tasks_runner(remove_tasks_pool, ThreadName::BLOB_KILLER_TASK)
{
    task = context->getSchedulePool().createTask(StorageID::createEmpty(), log->name(), [this]() { run(); });
    task->deactivate();
}

BlobKillerThread::~BlobKillerThread()
{
    task->deactivate();
}

void BlobKillerThread::run()
{
    auto component_guard = Coordination::setCurrentComponent("BlobKillerThread::run");
    LOG_TEST(log, "Starting cleanup");

    const int64_t dead_queue_estimate = metadata_storage->getDeadBlobsQueueEstimate();
    const auto [round_request_batch, round_blobs_in_task, round_threads_count] = calculateBlobKillerEffectiveParams(
        dead_queue_estimate, metadata_request_batch, max_metadata_request_batch, blobs_in_task, max_blobs_in_task, threads_count, max_threads_count);
    LOG_TRACE(log, "Dead blobs queue estimate: {}, cleanup round parameters: batch {}, blobs in task {}, threads {}",
        dead_queue_estimate, round_request_batch, round_blobs_in_task, round_threads_count);

    remove_tasks_pool.setMaxThreads(round_threads_count);
    int64_t removed_blobs = executeBlobsCleanup(round_request_batch, round_blobs_in_task, remove_tasks_runner, cluster, metadata_storage, object_storages, log);
    finished_rounds.fetch_add(1);
    finished_rounds.notify_all();

    if (removed_blobs < static_cast<int64_t>(0.9 * static_cast<double>(round_request_batch)))
    {
        const int64_t interval = reschedule_interval_sec.load();
        const int64_t schedule_after_ms = DelayWithJitter(interval * 1000).getDelayWithJitter(-500, 500);
        task->scheduleAfter(schedule_after_ms);
        LOG_TEST(log, "Scheduled after: {} ms", schedule_after_ms);
    }
    else
    {
        task->schedule();
        LOG_TRACE(log, "Removed {} blobs it is more than 90% of requested size - rescheduling imediately", removed_blobs);
    }
}

void BlobKillerThread::startup()
{
    started = true;

    if (enabled)
    {
        LOG_INFO(log, "Execution started");
        task->activateAndSchedule();
    }
}

void BlobKillerThread::shutdown()
{
    auto component_guard = Coordination::setCurrentComponent("BlobKillerThread::shutdown");
    LOG_INFO(log, "Shutting down Blob Killer thread");

    task->deactivate();

    /// We need to execute it here explicitly because some blobs may be in the metadata storage queue.
    executeBlobsCleanup(/*max_to_remove=*/0, blobs_in_task, remove_tasks_runner, cluster, metadata_storage, object_storages, log);
}

int64_t BlobKillerThread::trigger()
{
    if (!started || !enabled)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Blobs cleanup was not enabled for disk {}", disk_name);

    int64_t current_round = finished_rounds.load();
    int64_t expected_round = current_round + 1;

    task->schedule();

    return expected_round;
}

void BlobKillerThread::waitRound(int64_t expected_round)
{
    int64_t current_round = finished_rounds.load();
    while (current_round < expected_round)
    {
        finished_rounds.wait(current_round);
        current_round = finished_rounds.load();
    }
}

void BlobKillerThread::triggerAndWait()
{
    int64_t expected_round = trigger();

    if (wrapped_blob_killer)
        wrapped_blob_killer->triggerAndWait();

    waitRound(expected_round);
}

void BlobKillerThread::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    enabled = config.getBool(config_prefix + ".enabled", true);
    reschedule_interval_sec = std::max<int64_t>(config.getUInt64(config_prefix + ".interval_sec", DEFAULT_RESCHEDULE_INTERVAL_SEC), 1);
    metadata_request_batch = std::max<int64_t>(config.getUInt64(config_prefix + ".metadata_request_size", DEFAULT_METADATA_REQUEST_SIZE), 1);
    max_metadata_request_batch = std::max<int64_t>(config.getUInt64(config_prefix + ".max_metadata_request_size", DEFAULT_MAX_METADATA_REQUEST_SIZE), metadata_request_batch);
    blobs_in_task = std::clamp<int64_t>(config.getUInt64(config_prefix + ".blobs_in_task", DEFAULT_BLOBS_IN_TASK), 1, BLOBS_IN_TASK_HARDWARE_LIMIT);
    max_blobs_in_task = std::clamp<int64_t>(config.getUInt64(config_prefix + ".max_blobs_in_task", DEFAULT_MAX_BLOBS_IN_TASK), blobs_in_task, BLOBS_IN_TASK_HARDWARE_LIMIT);
    threads_count = std::max<int64_t>(config.getUInt64(config_prefix + ".threads_count", DEFAULT_THREADS_COUNT), 1);
    max_threads_count = std::max<int64_t>(config.getUInt64(config_prefix + ".max_threads_count", DEFAULT_MAX_THREADS_COUNT), threads_count);
    remove_tasks_pool.setMaxThreads(threads_count);

    LOG_INFO(log, "Applying new settings: Enabled: {}, Started: {}", enabled.load(), started.load());

    if (enabled && started)
        task->activateAndSchedule();
    else
        task->deactivate();
}

}
