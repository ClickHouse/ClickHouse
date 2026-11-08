#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/BlobCopierThread.h>

#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>

#include <Core/BackgroundSchedulePool.h>

#include <Common/DelayWithJitter.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <utility>
#include <ranges>

namespace ProfileEvents
{
    extern const Event BlobCopierThreadRuns;
    extern const Event BlobCopierThreadLockedBlobs;
    extern const Event BlobCopierThreadReplicatedBlobs;
    extern const Event BlobCopierThreadRecordedBlobs;
    extern const Event BlobCopierThreadLockBlobsErrors;
    extern const Event BlobCopierThreadReplicateBlobsErrors;
    extern const Event BlobCopierThreadRecordBlobsErrors;
}

namespace CurrentMetrics
{
    extern const Metric BlobCopierThreads;
    extern const Metric BlobCopierThreadsActive;
    extern const Metric BlobCopierThreadsScheduled;
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
constexpr int64_t DEFAULT_METADATA_REQUEST_SIZE = 500;
constexpr int64_t DEFAULT_THREADS_COUNT = 16;

IMetadataStorage::BlobsToReplicate findBlobsToReplicate(
    size_t request_batch,
    const ClusterConfigurationPtr & cluster,
    const MetadataStoragePtr & metadata_storage,
    const LoggerPtr & log) noexcept
{
    try
    {
        auto blobs_to_replicate = metadata_storage->getBlobsToReplicate(cluster, request_batch);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadLockedBlobs, blobs_to_replicate.size());
        return blobs_to_replicate;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadLockBlobsErrors);
        return {};
    }
}

bool replicateBlob(
    const StoredObject & blob,
    const Location & from_location,
    const Location & to_location,
    const ObjectStorageRouterPtr & object_storages,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const LoggerPtr & log) noexcept
{
    try
    {
        LOG_TRACE(log, "Replicating blob {}: {} -> {}", blob.remote_path, from_location, to_location);

        const auto from_object_storage = object_storages->takePointingTo(from_location);
        const auto to_object_storage = object_storages->takePointingTo(to_location);
        from_object_storage->copyObjectToAnotherObjectStorage(blob, blob, read_settings, write_settings, *to_object_storage);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadReplicatedBlobs);

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadReplicateBlobsErrors);
        return false;
    }
}

std::vector<std::shared_ptr<ThreadPoolCallbackRunnerLocal<bool>::Task>> scheduleReplicationTasks(
    ThreadPoolCallbackRunnerLocal<bool> & replication_tasks_runner,
    const IMetadataStorage::BlobsToReplicate & blobs_to_replicate,
    const ObjectStorageRouterPtr & object_storages,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const LoggerPtr & log) noexcept
{
    std::vector<std::shared_ptr<ThreadPoolCallbackRunnerLocal<bool>::Task>> running_replications;
    try
    {
        for (const auto & [blob, from_location, to_location] : blobs_to_replicate)
            running_replications.push_back(replication_tasks_runner.enqueueAndGiveOwnership([&blob, &from_location, &to_location, &object_storages, &read_settings, &write_settings, &log]()
            {
                return replicateBlob(blob, from_location, to_location, object_storages, read_settings, write_settings, log);
            }));
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    return running_replications;
}

void recordBlobsReplication(
    IMetadataStorage::BlobsToReplicate replicated_blobs,
    const MetadataStoragePtr & metadata_storage,
    const LoggerPtr & log) noexcept
{
    try
    {
        int64_t recorded_count = metadata_storage->recordAsReplicated(replicated_blobs);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadRecordedBlobs, recorded_count);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::BlobCopierThreadRecordBlobsErrors);
    }
}

void replicateBlobs(
    IMetadataStorage::BlobsToReplicate blobs_to_replicate,
    ThreadPoolCallbackRunnerLocal<bool> & replication_tasks_runner,
    const MetadataStoragePtr & metadata_storage,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    if (blobs_to_replicate.empty())
        return;

    LOG_TRACE(log, "Will replicate {} blobs", blobs_to_replicate.size());

    const ReadSettings read_settings = getReadSettings();
    const WriteSettings write_settings = getWriteSettings();
    auto replications = scheduleReplicationTasks(replication_tasks_runner, blobs_to_replicate, object_storages, read_settings, write_settings, log);
    ThreadPoolCallbackRunnerLocal<bool>::waitForAllToFinish(replications);

    IMetadataStorage::BlobsToReplicate replicated_blobs;
    for (const auto [replication, task] : std::views::zip(replications, blobs_to_replicate))
    {
        const auto & [blob, from_location, to_location] = task;
        const bool is_replicated = replication->future.get();
        if (is_replicated)
            replicated_blobs.emplace_back(blob, from_location, to_location);
    }

    recordBlobsReplication(std::move(replicated_blobs), metadata_storage, log);
}

void executeBlobsReplication(
    int64_t max_to_replicate,
    ThreadPoolCallbackRunnerLocal<bool> & replication_tasks_runner,
    const ClusterConfigurationPtr & cluster,
    const MetadataStoragePtr & metadata,
    const ObjectStorageRouterPtr & object_storages,
    const LoggerPtr & log) noexcept
{
    ProfileEvents::increment(ProfileEvents::BlobCopierThreadRuns);
    auto blobs_to_replicate = findBlobsToReplicate(max_to_replicate, cluster, metadata, log);
    replicateBlobs(std::move(blobs_to_replicate), replication_tasks_runner, metadata, object_storages, log);
}

}

BlobCopierThread::BlobCopierThread(
    std::string disk_name_,
    ContextPtr context,
    ClusterConfigurationPtr cluster_,
    MetadataStoragePtr metadata_storage_,
    ObjectStorageRouterPtr object_storages_)
    : disk_name(disk_name_)
    , cluster(std::move(cluster_))
    , metadata_storage(std::move(metadata_storage_))
    , object_storages(std::move(object_storages_))
    , log(getLogger(fmt::format("{}::BlobCopierThread", disk_name)))
    , replication_tasks_pool(CurrentMetrics::BlobCopierThreads, CurrentMetrics::BlobCopierThreadsActive, CurrentMetrics::BlobCopierThreadsScheduled, 0, 0, 0)
    , replication_tasks_runner(replication_tasks_pool, ThreadName::BLOB_COPIER_TASK)
{
    task = context->getSchedulePool().createTask(StorageID::createEmpty(), log->name(), [this]() { run(); });
    task->deactivate();
}

void BlobCopierThread::run()
{
    auto component_guard = Coordination::setCurrentComponent("BlobCopierThread::run");
    LOG_TEST(log, "Starting replication");

    executeBlobsReplication(metadata_request_batch.load(), replication_tasks_runner, cluster, metadata_storage, object_storages, log);
    finished_rounds.fetch_add(1);

    const int64_t schedule_after_ms = DelayWithJitter(reschedule_interval_sec.load() * 1000).getDelayWithJitter(-500, 500);
    task->scheduleAfter(schedule_after_ms);
    LOG_TEST(log, "Scheduled after: {} ms", schedule_after_ms);
}

void BlobCopierThread::startup()
{
    started = true;

    if (enabled)
    {
        LOG_INFO(log, "Execution started");
        task->activateAndSchedule();
    }
}

void BlobCopierThread::shutdown()
{
    LOG_INFO(log, "Shutting down Blob Copier thread");
    task->deactivate();
}

void BlobCopierThread::triggerAndWait()
{
    if (!started || !enabled)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Blobs replication was not enabled for disk {}", disk_name);

    int64_t current_round = finished_rounds.load();
    int64_t expected_round = current_round + 1;

    task->schedule();

    /// Wait at least one task run
    while (current_round < expected_round)
    {
        finished_rounds.wait(current_round);
        current_round = finished_rounds.load();
    }
}

void BlobCopierThread::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    enabled = config.getBool(config_prefix + ".enabled", false);
    reschedule_interval_sec = config.getUInt64(config_prefix + ".interval_sec", DEFAULT_RESCHEDULE_INTERVAL_SEC);
    metadata_request_batch = config.getUInt64(config_prefix + ".metadata_request_size", DEFAULT_METADATA_REQUEST_SIZE);
    replication_tasks_pool.setMaxThreads(config.getUInt64(config_prefix + ".threads_count", DEFAULT_THREADS_COUNT));

    LOG_INFO(log, "Applying new settings: Enabled: {}, Started: {}", enabled.load(), started.load());

    if (enabled && started)
        task->activateAndSchedule();
    else
        task->deactivate();
}

}
