#include <Storages/MergeTree/ExportPartitionTaskScheduler.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Storages/MergeTree/MergeTreePartExportManifest.h"
#include "Formats/FormatFactory.h"
#include <Core/Settings.h>
#include <limits>

namespace ProfileEvents
{
    extern const Event ExportPartitionZooKeeperRequests;
    extern const Event ExportPartitionZooKeeperGet;
    extern const Event ExportPartitionZooKeeperGetChildren;
    extern const Event ExportPartitionZooKeeperCreate;
    extern const Event ExportPartitionZooKeeperSet;
    extern const Event ExportPartitionZooKeeperRemove;
    extern const Event ExportPartitionZooKeeperMulti;
    extern const Event ExportPartsRejectedByMemoryLimit;
}


namespace DB
{

namespace Setting
{
    extern const SettingsMergeTreePartExportFileAlreadyExistsPolicy export_merge_tree_part_file_already_exists_policy;
}

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Capped exponential back-off, matching the standard ClickHouse convention
    /// (see ZooKeeperRetriesControl): delay = min(initial << (retry_count - 1), max).
    /// `retry_count` is the number of failures so far (>= 1 when a retry is pending).
    /// The shift is guarded against overflow by saturating to `max_backoff_seconds`.
    size_t computeRetryBackoffSeconds(size_t retry_count, size_t initial_backoff_seconds, size_t max_backoff_seconds)
    {
        const size_t initial = std::min(initial_backoff_seconds, max_backoff_seconds);

        if (retry_count <= 1 || initial == 0)
            return initial;

        const size_t shift = retry_count - 1;

        /// If shifting would overflow size_t, the result is certainly clamped to the cap.
        static constexpr size_t bits = sizeof(size_t) * 8;
        if (shift >= bits)
            return max_backoff_seconds;

        const size_t headroom = std::numeric_limits<size_t>::max() >> shift;
        if (initial > headroom)
            return max_backoff_seconds;

        return std::min(initial << shift, max_backoff_seconds);
    }
}

ExportPartitionTaskScheduler::ExportPartitionTaskScheduler(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
}

std::optional<time_t> ExportPartitionTaskScheduler::run()
{
    std::optional<time_t> earliest_backoff_retry;

    const auto available_move_executors = storage.background_moves_assignee.getAvailableMoveExecutors();

    /// this is subject to TOCTOU - but for now we choose to live with it.
    if (available_move_executors == 0)
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: No available move executors, skipping");
        return earliest_backoff_retry;
    }

    /// Respect the background memory soft-limit: refuse to schedule new export-part tasks when
    /// background tasks are already pressing the limit. The task is rescheduled by the parent
    /// background pool a few seconds later, so this just defers work without losing it.
    if (!canEnqueueBackgroundTask())
    {
        ProfileEvents::increment(ProfileEvents::ExportPartsRejectedByMemoryLimit);
        LOG_TRACE(storage.log,
            "ExportPartition scheduler task: Reached memory limit for the background tasks ({}), "
            "so won't select new parts to export. Current background tasks memory usage: {}.",
            formatReadableSizeWithBinarySuffix(background_memory_tracker.getSoftLimit()),
            formatReadableSizeWithBinarySuffix(background_memory_tracker.get()));
        return earliest_backoff_retry;
    }

    LOG_DEBUG(storage.log, "ExportPartition scheduler task: Available move executors: {}", available_move_executors);

    std::size_t scheduled_exports_count = 0;

    const uint32_t seed = uint32_t(std::hash<std::string>{}(storage.replica_name)) ^ uint32_t(scheduled_exports_count);
    pcg64_fast rng(seed);

    /// Hold the published snapshot for the whole pass and iterate it directly (sorted by
    /// create_time). It is immutable and the shared_ptr copy never blocks the writer. The scheduler
    /// is a pure reader; status converges via the status watch -> handleStatusChanges and poll().
    const auto model = storage.export_partition_manifests.get();
    if (!model)
        return earliest_backoff_retry;

    auto zk = storage.getZooKeeper();

    pruneLocalBackoff(model->get<ExportPartitionTaskEntryTagByTransactionId>());

    // Iterate sorted by create_time
    for (const auto & entry : model->get<ExportPartitionTaskEntryTagByCreateTime>())
    {
        if (scheduled_exports_count >= available_move_executors)
        {
            LOG_DEBUG(storage.log, "ExportPartition scheduler task: Scheduled exports count is greater than available move executors, skipping");
            break;
        }

        /// No need to query zk for status if the local one is not PENDING
        if (entry.status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            LOG_DEBUG(storage.log, "ExportPartition scheduler task: Skipping... Local status is {}", magic_enum::enum_name(entry.status).data());
            continue;
        }

        const auto & manifest = entry.manifest;
        const auto key = entry.getCompositeKey();
        const auto database = storage.getContext()->resolveDatabase(manifest.destination_database);
        const auto & table = manifest.destination_table;

        const auto destination_storage_id = StorageID(QualifiedTableName {database, table});

        const auto destination_storage = DatabaseCatalog::instance().tryGetTable(destination_storage_id, storage.getContext());

        if (!destination_storage)
        {
            LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to reconstruct destination storage: {}, skipping", destination_storage_id.getNameForLogs());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        std::string status_in_zk_string;
        if (!zk->tryGet(fs::path(storage.zookeeper_path) / "exports" / key / "status", status_in_zk_string))
        {
            LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to get status, skipping");
            continue;
        }

        const auto status_in_zk = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_in_zk_string);

        if (!status_in_zk)
        {
            LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to get status from zk, skipping");
            continue;
        }

        if (status_in_zk.value() != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            LOG_DEBUG(storage.log, "ExportPartition scheduler task: Skipping {}... Status from zk is {}", key, magic_enum::enum_name(status_in_zk.value()).data());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> parts_in_processing_or_pending;

        if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(storage.zookeeper_path) / "exports" / key / "processing", parts_in_processing_or_pending))
        {
            LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to get parts in processing or pending, skipping");
            continue;
        }


        if (parts_in_processing_or_pending.empty())
        {
            LOG_DEBUG(storage.log, "ExportPartition scheduler task: No parts in processing or pending, skipping");
            continue;
        }

        /// shuffle the parts to reduce the risk of lock collisions
        std::shuffle(parts_in_processing_or_pending.begin(), parts_in_processing_or_pending.end(), rng);

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> locked_parts;

        if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(storage.zookeeper_path) / "exports" / key / "locks", locked_parts))
        {
            LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to get locked parts, skipping");
            continue;
        }

        std::unordered_set<std::string> locked_parts_set(locked_parts.begin(), locked_parts.end());

        const auto now = time(nullptr);

        for (const auto & zk_part_name : parts_in_processing_or_pending)
        {
            if (scheduled_exports_count >= available_move_executors)
            {
                LOG_DEBUG(storage.log, "ExportPartition scheduler task: Scheduled exports count is greater than available move executors, skipping");
                break;
            }

            if (locked_parts_set.contains(zk_part_name))
            {
                LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} is locked, skipping", zk_part_name);
                continue;
            }

            if (shouldBackOff(entry.getTransactionId(), zk_part_name, now, earliest_backoff_retry))
            {
                continue;
            }

            const auto part = storage.getPartIfExists(zk_part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
            if (!part)
            {
                LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} not found locally, skipping", zk_part_name);
                continue;
            }

            LOG_INFO(storage.log, "ExportPartition scheduler task: Scheduling part export: {}", zk_part_name);

            auto context = ExportPartitionUtils::getContextCopyWithTaskSettings(storage.getContext(), manifest);

            try
            {
                LOG_DEBUG(storage.log, "ExportPartition scheduler task: Exporting part to table");

                LOG_INFO(storage.log, "ExportPartition scheduler task: Attempting to lock part: {}", zk_part_name);

                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperCreate);
                if (Coordination::Error::ZOK != zk->tryCreate(fs::path(storage.zookeeper_path) / "exports" / key / "locks" / zk_part_name, storage.replica_name, zkutil::CreateMode::Ephemeral))
                {
                    LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to lock part {}, skipping", zk_part_name);
                    continue;
                }

                LOG_INFO(storage.log, "ExportPartition scheduler task: Locked part: {}", zk_part_name);

                storage.exportPartToTable(
                    part->name,
                    destination_storage_id,
                    manifest.transaction_id,
                    context,
                    manifest.iceberg_metadata_json,
                    /*allow_outdated_parts*/ true,
                    [this, key, zk_part_name, manifest, destination_storage]
                    (MergeTreePartExportManifest::CompletionCallbackResult result)
                    {
                        handlePartExportCompletion(key, zk_part_name, manifest, destination_storage, result);
                    });

                scheduled_exports_count++;
            }
            catch (const Exception &)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRemove);
                zk->tryRemove(fs::path(storage.zookeeper_path) / "exports" / key / "locks" / zk_part_name);
                /// Dispatch-time failure (e.g. Keeper node full). We do not arm the local
                /// back-off here: the export never started, so the part stays immediately
                /// eligible for this or another replica on the next tick.
            }
        }
    }

    return earliest_backoff_retry;
}

bool ExportPartitionTaskScheduler::shouldBackOff(
    const std::string & transaction_id,
    const std::string & part_name,
    time_t now,
    std::optional<time_t> & earliest_backoff_retry) const
{
    std::lock_guard lock(local_backoff_mutex);
    const auto task_it = local_backoff.find(transaction_id);
    if (task_it == local_backoff.end())
        return false;

    const auto part_it = task_it->second.find(part_name);
    if (part_it == task_it->second.end() || now >= part_it->second.next_retry_time)
        return false;

    const auto next_retry_time = part_it->second.next_retry_time;
    LOG_TRACE(storage.log, "ExportPartition scheduler task: Part {} is backing off locally, next retry at {} (now {}), skipping", part_name, next_retry_time, now);
    earliest_backoff_retry = earliest_backoff_retry
        ? std::min(*earliest_backoff_retry, next_retry_time) : next_retry_time;
    return true;
}

time_t ExportPartitionTaskScheduler::registerLocalBackoff(
    const std::string & transaction_id,
    const std::string & part_name,
    const ExportReplicatedMergeTreePartitionManifest & manifest)
{
    std::lock_guard lock(local_backoff_mutex);

    /// First retryable failure for (transaction_id, part_name): create the map entries.
    auto & parts = local_backoff.try_emplace(transaction_id).first->second;
    auto & backoff = parts.try_emplace(part_name).first->second;

    ++backoff.attempts;
    const auto backoff_seconds = computeRetryBackoffSeconds(
        backoff.attempts, manifest.retry_initial_backoff_seconds, manifest.retry_max_backoff_seconds);
    const auto now = time(nullptr);
    /// Clamp so a huge configured back-off cannot overflow time_t (now is a normal wall-clock value).
    const size_t headroom = static_cast<size_t>(std::numeric_limits<time_t>::max() - now);
    backoff.next_retry_time = now + static_cast<time_t>(std::min(backoff_seconds, headroom));
    return backoff.next_retry_time;
}

void ExportPartitionTaskScheduler::clearLocalBackoff(const std::string & transaction_id, const std::string & part_name)
{
    std::lock_guard lock(local_backoff_mutex);
    if (const auto task_it = local_backoff.find(transaction_id); task_it != local_backoff.end())
    {
        task_it->second.erase(part_name);
        if (task_it->second.empty())
            local_backoff.erase(task_it);
    }
}

void ExportPartitionTaskScheduler::pruneLocalBackoff(const ExportPartitionTaskEntriesContainer::index<ExportPartitionTaskEntryTagByTransactionId>::type & model)
{
    std::lock_guard lock(local_backoff_mutex);
    for (auto it = local_backoff.begin(); it != local_backoff.end();)
    {
        const auto found = model.find(it->first);
        if (found != model.end() && found->status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            ++it;
            continue;
        }

        it = local_backoff.erase(it);
    }
}

ExportPartitionTaskScheduler::LocalBackoffMap ExportPartitionTaskScheduler::getLocalBackoffSnapshot() const
{
    LocalBackoffMap snapshot;

    std::lock_guard lock(local_backoff_mutex);
    snapshot.reserve(local_backoff.size());
    for (const auto & [transaction_id, parts] : local_backoff)
    {
        auto & out_parts = snapshot[transaction_id];
        out_parts.reserve(parts.size());
        for (const auto & [part_name, backoff] : parts)
            out_parts.emplace(part_name, LocalBackoff{backoff.attempts, backoff.next_retry_time});
    }

    return snapshot;
}

void ExportPartitionTaskScheduler::handlePartExportCompletion(
    const std::string & export_key,
    const std::string & part_name,
    const ExportReplicatedMergeTreePartitionManifest & manifest,
    const StoragePtr & destination_storage,
    const MergeTreePartExportManifest::CompletionCallbackResult & result)
{
    /// Invoked from MergeTreeBackgroundExecutor threads, so the component is not inherited from selectPartsToExport.
    auto component_guard = Coordination::setCurrentComponent("ExportPartitionTaskScheduler::handlePartExportCompletion");

    const auto export_path = fs::path(storage.zookeeper_path) / "exports" / export_key;
    const auto processing_parts_path = export_path / "processing";
    const auto processed_part_path = export_path / "processed" / part_name;
    const auto zk = storage.getZooKeeper();

    if (result.success)
    {
        handlePartExportSuccess(manifest, destination_storage, processing_parts_path, processed_part_path, part_name, export_path, zk, result.relative_paths_in_destination_storage);
    }
    else
    {
        handlePartExportFailure(part_name, export_path, zk, result.exception, manifest);
    }
}

void ExportPartitionTaskScheduler::handlePartExportSuccess(
    const ExportReplicatedMergeTreePartitionManifest & manifest,
    const StoragePtr & destination_storage,
    const std::filesystem::path & processing_parts_path,
    const std::filesystem::path & processed_part_path,
    const std::string & part_name,
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<String> & relative_paths_in_destination_storage
)
{
    LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} exported successfully, paths size: {}", part_name, relative_paths_in_destination_storage.size());

    for (const auto & relative_path_in_destination_storage : relative_paths_in_destination_storage)
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: {}", relative_path_in_destination_storage);
    }

    if (!tryToMovePartToProcessed(export_path, processing_parts_path, processed_part_path, part_name, relative_paths_in_destination_storage, zk))
    {
        LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to move part to processed, will not commit export partition");
        return;
    }

    /// Part is done on this replica; drop any local back-off state we held for it.
    clearLocalBackoff(manifest.transaction_id, part_name);

    LOG_INFO(storage.log, "ExportPartition scheduler task: Marked part export {} as completed", part_name);

    if (!areAllPartsProcessed(export_path, zk))
    {
        return;
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: All parts are processed, will try to commit export partition");

    try
    {
        auto context = ExportPartitionUtils::getContextCopyWithTaskSettings(storage.getContext(), manifest);
        ExportPartitionUtils::commit(manifest, destination_storage, zk, storage.log.load(), export_path, context, storage, storage.replica_name);
    }
    catch (const Exception & e)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Caught exception while committing export partition, {}", e.message());

        /// Classify the commit failure: a non-retryable error (e.g. schema/spec mismatch)
        /// transitions the task to FAILED immediately; a retryable one (transient catalog or
        /// destination outage) only records the exception and leaves the task PENDING so the
        /// commit is retried until the absolute task timeout.
        /// The exception is recorded in <export_path>/last_exception via appendExceptionOps
        /// inside the same multi as the (possible) FAILED set.
        const bool became_failed = ExportPartitionUtils::handleCommitFailure(
            zk,
            export_path,
            e.code(),
            storage.replica_name,
            e.message(),
            storage.log.load());

        if (became_failed)
        {
            LOG_WARNING(storage.log,
                "ExportPartition scheduler task: Commit for {} transitioned to FAILED due to non-retryable error (code {})",
                export_path.string(), e.code());
        }
    }
}

void ExportPartitionTaskScheduler::handlePartExportFailure(
    const std::string & part_name,
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk,
    const std::optional<Exception> & exception,
    const ExportReplicatedMergeTreePartitionManifest & manifest
)
{
    LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} export failed", part_name);

    if (!exception)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExportPartition scheduler task: No exception provided for error handling. Sounds like a bug");
    }

    Coordination::Stat locked_by_stat;
    std::string locked_by;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    if (!zk->tryGet(export_path / "locks" / part_name, locked_by, &locked_by_stat))
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} is not locked by any replica, will not increment error counts", part_name);
        return;
    }

    if (locked_by != storage.replica_name)
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} is locked by another replica, will not increment error counts", part_name);
        return;
    }

    /// Early exit if the query was cancelled - no need to increment error counts
    if (exception->code() == ErrorCodes::QUERY_WAS_CANCELLED)
    {
        /// Releasing the lock is important because a query can be cancelled due to SYSTEM STOP MOVES. If this is the case,
        /// other replicas should still be able to export this individual part. That's why there is a retry loop here.
        /// It is very unlikely this will be a problem in practice. The lock is ephemeral, which means it is automatically released
        /// if ClickHouse loses connection to ZooKeeper
        std::size_t retry_count = 0;
        static constexpr std::size_t max_lock_release_retries = 3;
        while (retry_count < max_lock_release_retries)
        {
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRemove);

            const auto removal_code = zk->tryRemove(export_path / "locks" / part_name, locked_by_stat.version);

            if (Coordination::Error::ZOK == removal_code)
            {
                break;
            }

            if (Coordination::Error::ZBADVERSION == removal_code)
            {
                LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} lock version mismatch, will not increment error counts", part_name);
                break;
            }

            retry_count++;
        }

        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} export was cancelled, skipping error handling", part_name);
        return;
    }

    const std::string status_path = export_path / "status";
    Coordination::Stat status_stat;
    std::string current_status;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    if (!zk->tryGet(status_path, current_status, &status_stat))
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: /status missing for {}, skipping failure bookkeeping", export_path.string());
        return;
    }

    const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(current_status);
    if (!status || *status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: /status for {} is {} (not PENDING), skipping failure bookkeeping", export_path.string(), current_status);
        return;
    }

    const bool non_retryable = ExportPartitionUtils::isNonRetryableExportError(exception->code());

    Coordination::Requests ops;

    ops.emplace_back(zkutil::makeRemoveRequest(export_path / "locks" / part_name, locked_by_stat.version));

    if (non_retryable)
    {
        /// Deterministic failure (e.g. schema/type incompatibility): retrying cannot help,
        /// so fail the whole task immediately instead of waiting for the absolute timeout.
        ops.emplace_back(zkutil::makeSetRequest(
            status_path,
            String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::FAILED)).data(),
            status_stat.version));
        LOG_WARNING(storage.log, "ExportPartition scheduler task: Part {} failed with non-retryable error (code {}), failing the entire task", part_name, exception->code());
    }
    else
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} failed with retryable error (code {}), will back off and retry until the task timeout", part_name, exception->code());
    }

    ExportPartitionUtils::appendExceptionOps(
        ops, zk, export_path, storage.replica_name, part_name,
        exception->message(), storage.log.load());

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
    Coordination::Responses responses;
    if (Coordination::Error::ZOK != zk->tryMulti(ops, responses))
    {
        LOG_WARNING(storage.log, "ExportPartition scheduler task: All failure mechanism failed, will not try to update it");
        return;
    }

    /// Only after the lock release + exception record committed do we arm the local back-off,
    /// so a Keeper failure above does not leave this replica skipping the part for no reason.
    if (!non_retryable)
    {
        const auto next_retry_time = registerLocalBackoff(manifest.transaction_id, part_name, manifest);
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} backing off locally, next retry at {}", part_name, next_retry_time);
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: Successfully recorded failure for part {}", part_name);
}

bool ExportPartitionTaskScheduler::tryToMovePartToProcessed(
    const std::filesystem::path & export_path,
    const std::filesystem::path & processing_parts_path,
    const std::filesystem::path & processed_part_path,
    const std::string & part_name,
    const std::vector<String> & relative_paths_in_destination_storage,
    const zkutil::ZooKeeperPtr & zk
)
{
    Coordination::Stat locked_by_stat;
    std::string locked_by;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    if (!zk->tryGet(export_path / "locks" / part_name, locked_by, &locked_by_stat))
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} is not locked by any replica, will not commit or set it as completed", part_name);
        return false;
    }

    /// Is this a good idea? what if the file we just pushed to s3 ends up triggering an exception in the replica that actually locks the part and it does not commit?
    /// I guess we should not throw if file already exists for export partition, hard coded.
    if (locked_by != storage.replica_name)
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: Part {} is locked by another replica, will not commit or set it as completed", part_name);
        return false;
    }

    Coordination::Requests requests;

    ExportReplicatedMergeTreePartitionProcessedPartEntry processed_part_entry;
    processed_part_entry.part_name = part_name;
    processed_part_entry.paths_in_destination = relative_paths_in_destination_storage;
    processed_part_entry.finished_by = storage.replica_name;

    requests.emplace_back(zkutil::makeRemoveRequest(processing_parts_path / part_name, -1));
    requests.emplace_back(zkutil::makeCreateRequest(processed_part_path, processed_part_entry.toJsonString(), zkutil::CreateMode::Persistent));
    requests.emplace_back(zkutil::makeRemoveRequest(export_path / "locks" / part_name, locked_by_stat.version));

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
    Coordination::Responses responses;
    if (Coordination::Error::ZOK != zk->tryMulti(requests, responses))
    {

        /// todo  arthur remember what to do here
        LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to update export path, skipping");
        return false;
    }

    return true;
}

bool ExportPartitionTaskScheduler::areAllPartsProcessed(
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk)
{
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
    Strings parts_in_processing_or_pending;
    if (Coordination::Error::ZOK != zk->tryGetChildren(export_path / "processing", parts_in_processing_or_pending))
    {
        LOG_WARNING(storage.log, "ExportPartition scheduler task: Failed to get parts in processing or pending, will not try to commit export partition");
        return false;
    }

    if (!parts_in_processing_or_pending.empty())
    {
        LOG_DEBUG(storage.log, "ExportPartition scheduler task: There are still parts in processing or pending, will not try to commit export partition");
        return false;
    }

    return true;
}

}
