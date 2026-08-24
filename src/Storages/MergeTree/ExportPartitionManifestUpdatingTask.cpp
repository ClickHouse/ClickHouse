#include <Storages/MergeTree/ExportPartitionManifestUpdatingTask.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Common/logger_useful.h"
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/DatabaseCatalog.h>
#include <fmt/format.h>
#include <optional>

namespace ProfileEvents
{
    extern const Event ExportPartitionZooKeeperRequests;
    extern const Event ExportPartitionZooKeeperGet;
    extern const Event ExportPartitionZooKeeperGetChildren;
    extern const Event ExportPartitionZooKeeperGetChildrenWatch;
    extern const Event ExportPartitionZooKeeperGetWatch;
    extern const Event ExportPartitionZooKeeperRemoveRecursive;
    extern const Event ExportPartitionZooKeeperMulti;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char export_partition_status_change_throw[];
    extern const char export_partition_processed_paths_sync_fail[];
}

namespace
{
    /// Value published into destination_file_paths when a processed/ Keeper refresh
    /// is incomplete (or a leaf is unreadable), so system.replicated_partition_exports
    /// can show that the in-memory mirror failed to sync instead of silently under-counting.
    constexpr std::string_view zk_sync_failed_marker = "<failed to read from zk>";

    /// Describes pending commits
    struct CommitRecoveryWork
    {
        ExportReplicatedMergeTreePartitionManifest metadata;
        std::string entry_path;
        StoragePtr destination_storage;
        ContextPtr context;
    };

    /// Fetch all per-replica last_exception leaves under <entry_path>/last_exception and build
    /// a fresh map keyed by replica name.
    std::optional<std::map<String, LastExceptionEntry>> readLastExceptionPerReplica(
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & log_key,
        const LoggerPtr & log)
    {
        std::map<String, LastExceptionEntry> out;

        const auto container_path = entry_path / "last_exception";

        Strings children;
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        if (Coordination::Error::ZOK != zk->tryGetChildren(container_path, children))
        {
            LOG_WARNING(log, "ExportPartition Manifest Updating Task: failed to list last_exception leaves for {}, leaving in-memory copy untouched", log_key);
            return std::nullopt;
        }

        if (children.empty())
            return out;

        std::vector<std::string> paths;
        paths.reserve(children.size());
        for (const auto & child : children)
            paths.emplace_back(container_path / child);

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet, paths.size());
        auto responses = zk->tryGet(paths);
        responses.waitForResponses();

        for (size_t i = 0; i < paths.size(); ++i)
        {
            Coordination::GetResponse response;
            try
            {
                /// MultiTryGetResponse::operator[] swallows ZNONODE but rethrows on
                /// other errors; treat any unexpected Keeper error as "skip this
                /// leaf, retry on the next poll". Matches the lenient semantics of
                /// the previous per-leaf tryGet implementation.
                response = responses[i];
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: ZK error fetching last_exception leaf {} for {}, skipping", children[i], log_key);
                continue;
            }

            if (response.error != Coordination::Error::ZOK)
                continue; /// ZNONODE: child concurrently removed (recursive cleanup race).

            try
            {
                auto entry = LastExceptionEntry::fromJsonString(response.data);
                String replica = entry.replica.empty() ? unescapeForFileName(children[i]) : entry.replica;
                out.emplace(std::move(replica), std::move(entry));
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: malformed last_exception JSON for {} (leaf {}), ignoring", log_key, children[i]);
            }
        }

        return out;
    }

    std::map<String, std::vector<String>> readDestinationFilePathsPerPart(
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & log_key,
        const LoggerPtr & log)
    {
        std::map<String, std::vector<String>> out;

        const auto container_path = entry_path / "processed";

        Strings children;
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        if (Coordination::Error::ZOK != zk->tryGetChildren(container_path, children))
        {
            LOG_INFO(log, "ExportPartition Manifest Updating Task: failed to list processed leaves for {}, publishing sync-failed marker", log_key);
            out.emplace(String(zk_sync_failed_marker), std::vector<String>{String(zk_sync_failed_marker)});
            return out;
        }

        if (children.empty())
            return out;

        std::vector<std::string> paths;
        paths.reserve(children.size());
        for (const auto & child : children)
            paths.emplace_back(container_path / child);

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet, paths.size());
        auto responses = zk->tryGet(paths);
        responses.waitForResponses();

        for (size_t i = 0; i < paths.size(); ++i)
        {
            Coordination::GetResponse response;
            try
            {
                /// Simulate a non-ZNONODE multi-get failure so the catch path below
                /// publishes the sync-failed marker (same shape as operator[] rethrow).
                fiu_do_on(FailPoints::export_partition_processed_paths_sync_fail,
                {
                    throw zkutil::KeeperException(Coordination::Error::ZCONNECTIONLOSS);
                });
                response = responses[i];
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: ZK error fetching processed leaf {} for {}, publishing sync-failed marker", children[i], log_key);
                out.emplace(children[i], std::vector<String>{String(zk_sync_failed_marker)});
                continue;
            }

            if (response.error != Coordination::Error::ZOK)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: could not read processed leaf {} for {} (error {}), publishing sync-failed marker", children[i], log_key, response.error);
                out.emplace(children[i], std::vector<String>{String(zk_sync_failed_marker)});
                continue;
            }

            try
            {
                auto entry = ExportReplicatedMergeTreePartitionProcessedPartEntry::fromJsonString(response.data);
                out.emplace(std::move(entry.part_name), std::move(entry.paths_in_destination));
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: malformed processed JSON for {} (leaf {}), publishing sync-failed marker", log_key, children[i]);
                out.emplace(children[i], std::vector<String>{String(zk_sync_failed_marker)});
            }
        }

        return out;
    }

    /// True when the cached `/processed` mirror carries a `zk_sync_failed_marker` sentinel,
    /// published whenever a listing or a leaf read/parse failed. Such a mirror is incomplete
    /// and must be refreshed again on the next poll.
    bool destinationFilePathsMirrorHasSyncFailure(const std::map<String, std::vector<String>> & cached_paths)
    {
        for (const auto & [part_name, destination_paths] : cached_paths)
        {
            if (part_name == zk_sync_failed_marker)
                return true;
            for (const auto & destination_path : destination_paths)
                if (destination_path == zk_sync_failed_marker)
                    return true;
        }
        return false;
    }

    bool skipReadingDestinationFilePaths(
        ExportReplicatedMergeTreePartitionTaskEntry::Status status,
        const std::map<String, std::vector<String>> & cached_paths,
        size_t number_of_parts)
    {
        if (status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
            return false;
        if (destinationFilePathsMirrorHasSyncFailure(cached_paths))
            return false;
        if (status == ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)
            return cached_paths.size() == number_of_parts;
        return true;
    }

    /// Read the optional <entry_path>/commit_info znode and return the parsed entry.
    /// Returns nullopt when the znode is absent (task has not committed yet, peer
    /// crashed before writing it, or transient ZK error). Callers should treat
    /// nullopt as "leave the in-memory copy untouched".
    std::optional<ExportReplicatedMergeTreePartitionCommitInfoEntry> readCommitInfo(
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & log_key,
        const LoggerPtr & log)
    {
        const auto commit_info_path = entry_path / "commit_info";

        std::string data;
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        if (!zk->tryGet(commit_info_path, data))
            return std::nullopt;

        try
        {
            return ExportReplicatedMergeTreePartitionCommitInfoEntry::fromJsonString(data);
        }
        catch (...)
        {
            LOG_WARNING(log, "ExportPartition Manifest Updating Task: malformed commit_info JSON for {}, ignoring", log_key);
            return std::nullopt;
        }
    }

    /// collects pending commits and kills tasks that have timed out
    void tryCleanup(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        const LoggerPtr & log,
        const ContextPtr & storage_context,
        StorageReplicatedMergeTree & storage,
        const ExportReplicatedMergeTreePartitionManifest & metadata,
        const time_t now,
        const bool is_pending,
        std::vector<CommitRecoveryWork> & deferred_commits
    )
    {
        bool task_timed_out = is_pending
            && metadata.task_timeout_seconds > 0
            && metadata.create_time + static_cast<time_t>(metadata.task_timeout_seconds) < now;

        if (task_timed_out)
        {
            /// Serialize against commit(): don't kill a task whose commit is in progress.
            auto commit_lock = zkutil::EphemeralNodeHolder::tryCreate(
                fs::path(entry_path) / "commit_lock", *zk, storage.getReplicaName());
            if (!commit_lock)
            {
                LOG_DEBUG(log, "ExportPartition Manifest Updating Task: commit in progress for {}, skipping timeout kill", entry_path);
                return;
            }

            const std::string status_path = fs::path(entry_path) / "status";

            Coordination::Stat status_stat;
            std::string status_string;

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            if (!zk->tryGet(status_path, status_string, &status_stat))
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Failed to read status for {} while enforcing task timeout, skipping", entry_path);
                return;
            }

            const auto current_status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_string);
            if (!current_status || *current_status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
            {
                LOG_DEBUG(log, "ExportPartition Manifest Updating Task: Task {} is not PENDING, can't set to KILLED, skipping", entry_path);
                return;
            }

            const auto timeout_message = fmt::format(
                "Export partition task timed out: exceeded export_merge_tree_partition_task_timeout_seconds={} (created at {}, now {})",
                metadata.task_timeout_seconds, metadata.create_time, now);

            const auto killed_name = String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::KILLED));

            Coordination::Requests ops;
            ExportPartitionUtils::appendExceptionOps(
                ops, zk, fs::path(entry_path), storage.getReplicaName(),
                /*part_name=*/"", timeout_message, log);

            ops.emplace_back(zkutil::makeSetRequest(status_path, killed_name, status_stat.version));

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);

            Coordination::Responses responses;
            const auto rc = zk->tryMulti(ops, responses);

            if (rc == Coordination::Error::ZOK)
            {
                LOG_WARNING(log,
                    "ExportPartition Manifest Updating Task: task {} exceeded task_timeout_seconds={}s, "
                    "transitioned PENDING -> KILLED (atomic with exception record)",
                    entry_path, metadata.task_timeout_seconds);
            }
            else
            {
                /// ZBADVERSION (status changed), ZNODEEXISTS (lazy-create race with the scheduler),
                /// counter race, or ZNONODE (entry concurrently removed). In all cases the batch
                /// was rolled back atomically and the task will be re-evaluated on the next poll.
                LOG_DEBUG(log,
                    "ExportPartition Manifest Updating Task: atomic kill for {} failed (rc={}); "
                    "status was concurrently updated or a ZK op conflicted, will retry on next poll",
                    entry_path, rc);
            }

            /// The entry remains in entries_by_key; the status watch will drive
            /// handleStatusChanges -> killExportPart on every replica, mirroring user-initiated KILL.
            return;
        }
        else if (is_pending)
        {
            auto context = ExportPartitionUtils::getContextCopyWithTaskSettings(storage_context, metadata);

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
            std::vector<std::string> parts_in_processing_or_pending;
            if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(entry_path) / "processing", parts_in_processing_or_pending))
            {

                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Failed to get parts in processing or pending, skipping");
                return;
            }

            if (parts_in_processing_or_pending.empty())
            {
                LOG_DEBUG(log, "ExportPartition Manifest Updating Task: Cleanup found PENDING for {} with all parts exported, deferring commit recovery to post-lock phase", entry_path);

                const auto destination_storage_id = StorageID(QualifiedTableName {metadata.destination_database, metadata.destination_table});
                const auto destination_storage = DatabaseCatalog::instance().tryGetTable(destination_storage_id, context);
                if (!destination_storage)
                {
                    LOG_WARNING(log, "ExportPartition Manifest Updating Task: Failed to reconstruct destination storage: {}, skipping", destination_storage_id.getNameForLogs());
                    return;
                }

                /// A replica exported the last part but the commit never landed
                deferred_commits.push_back(CommitRecoveryWork{
                    .metadata = metadata,
                    .entry_path = entry_path,
                    .destination_storage = destination_storage,
                    .context = context,
                });
            }
        }
    }
}

ExportPartitionManifestUpdatingTask::ExportPartitionManifestUpdatingTask(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
}

std::vector<ReplicatedPartitionExportInfo> ExportPartitionManifestUpdatingTask::getPartitionExportsInfo() const
{
    const auto model = storage.export_partition_manifests.get();

    if (!model)
        return {};

    const auto backoff = storage.export_merge_tree_partition_task_scheduler->getLocalBackoffSnapshot();

    std::vector<ReplicatedPartitionExportInfo> infos;
    infos.reserve(model->size());

    for (const auto & entry : model->get<ExportPartitionTaskEntryTagByCompositeKey>())
    {
        const auto & manifest = entry.manifest;

        ReplicatedPartitionExportInfo info;

        info.destination_database = manifest.destination_database;
        info.destination_table = manifest.destination_table;
        info.partition_id = manifest.partition_id;
        info.transaction_id = manifest.transaction_id;
        info.query_id = manifest.query_id;
        info.create_time = manifest.create_time;
        info.source_replica = manifest.source_replica;
        info.parts_count = manifest.number_of_parts;
        info.parts_to_do = manifest.parts.size();
        info.parts = manifest.parts;
        info.status = magic_enum::enum_name(entry.status);

        info.last_exception_per_replica.reserve(entry.last_exception_per_replica.size());
        size_t total_exception_count = 0;
        for (const auto & [_, ex] : entry.last_exception_per_replica)
        {
            total_exception_count += ex.count;
            info.last_exception_per_replica.push_back(ex);
        }
        info.exception_count = total_exception_count;

        info.destination_file_paths_per_part = entry.destination_file_paths_per_part;

        if (entry.commit_info)
        {
            info.committed_metadata_file = entry.commit_info->iceberg_metadata_file;
            info.committed_manifest_list = entry.commit_info->iceberg_manifest_list;
            info.committed_manifest_file = entry.commit_info->iceberg_manifest_file;
            info.committed_marker_file = entry.commit_info->commit_marker_file;
        }

        if (const auto it = backoff.find(entry.getTransactionId()); it != backoff.end())
        {
            info.backoff_per_part.reserve(it->second.size());
            for (const auto & [part_name, state] : it->second)
                info.backoff_per_part.push_back({part_name, state.attempts, state.next_retry_time});
        }

        infos.emplace_back(std::move(info));
    }

    return infos;
}

void ExportPartitionManifestUpdatingTask::poll()
{
    /// Commit-recovery work collected while the storage-wide mutex is held.
    /// Executed AFTER the mutex is released - committing to Iceberg/REST-catalog can take
    /// many seconds (up to MAX_TRANSACTION_RETRIES=100 catalog round-trips) and blocking
    /// `system.replicated_partition_exports` for that long is what we are fixing here.
    std::vector<CommitRecoveryWork> deferred_commits;

    auto zk = storage.getZooKeeper();
    const auto log = storage.log.load();

    const std::string exports_path = fs::path(storage.zookeeper_path) / "exports";
    const std::string cleanup_lock_path = fs::path(storage.zookeeper_path) / "exports_cleanup_lock";

    /// The `exports_cleanup_lock` is an ephemeral ZK node that serializes cleanup work
    /// across replicas: only the replica holding it walks `tryCleanup` (task-timeout
    /// enforcement + commit recovery). It MUST outlive the deferred-commit loop below; otherwise a peer
    /// replica's next poll() could acquire it and race us on the same commit-recovery work,
    /// duplicating REST-catalog round-trips and snapshot writes.
    auto cleanup_lock = zkutil::EphemeralNodeHolder::tryCreate(cleanup_lock_path, *zk, storage.replica_name);
    if (cleanup_lock)
    {
        LOG_DEBUG(log, "ExportPartition Manifest Updating Task: Cleanup lock acquired, will remove stale entries");
    }

    {
        /// M_task: serializes poll() vs handleStatusChanges(). We copy the current read-model into a
        /// private mutable container, mutate that copy across the ZooKeeper reads below, and publish
        /// it atomically via export_read_model.set() at the end. Readers never see partial updates.
        std::lock_guard task_guard(background_task_serialization_mutex);

        const auto current_model = storage.export_partition_manifests.get();

        auto working_model = current_model
            ? std::make_unique<ExportPartitionTaskEntriesContainer>(*current_model)
            : std::make_unique<ExportPartitionTaskEntriesContainer>();

        auto & entries_by_key = working_model->get<ExportPartitionTaskEntryTagByCompositeKey>();

        LOG_DEBUG(log, "ExportPartition Manifest Updating Task: Polling for new entries for table {}. Current number of entries: {}", storage.getStorageID().getNameForLogs(), entries_by_key.size());

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildrenWatch);

        Coordination::Stat stat;
        const auto children = zk->getChildrenWatch(exports_path, &stat, storage.export_merge_tree_partition_watch_callback);
        const std::unordered_set<std::string> zk_children(children.begin(), children.end());

        const auto now = time(nullptr);

        /// Load new entries
        /// If we have the cleanup lock, also remove stale entries from zk and local
        /// Upload dangling commit files if any
        for (const auto & key : zk_children)
        {
            const std::string entry_path = fs::path(exports_path) / key;

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            std::string metadata_json;
            if (!zk->tryGet(fs::path(entry_path) / "metadata.json", metadata_json))
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Skipping {}: missing metadata.json", key);
                continue;
            }

            ExportReplicatedMergeTreePartitionManifest metadata;
            try
            {
                metadata = ExportReplicatedMergeTreePartitionManifest::fromJsonString(metadata_json);
            }
            catch (...)
            {
                /// A single unparseable metadata.json (e.g. genuinely corrupt, or written by a
                /// future incompatible format) must not abort the whole poll and stall discovery,
                /// cleanup and status convergence for every other task. Skip just this entry.
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Skipping {}: could not parse metadata.json", key);
                continue;
            }

            auto last_exception_per_replica = readLastExceptionPerReplica(
                zk, fs::path(entry_path), key, log);

            /// If the zk entry has been replaced with export_merge_tree_partition_force_export, checking only for the export key is not enough
            /// we need to make sure it is the same transaction id. If it is not, it needs to be replaced.
            const auto local_entry = entries_by_key.find(key);
            const bool has_local_entry = local_entry != entries_by_key.end()
                && local_entry->manifest.transaction_id == metadata.transaction_id;

            std::string status_string;

            /// In theory, we should be notified when the status changes by the status watch
            /// but in practice, the watch is not always reliable (e.g. if the ZooKeeper session is lost)
            /// so we need to read the status from the ZK node directly.
            if (has_local_entry)
            {
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);

                zk->tryGet(fs::path(entry_path) / "status", status_string);
            }
            else
            {
                /// If we don't have a local entry, we need to arm a status watch to be notified when the status changes
                std::weak_ptr<ExportPartitionManifestUpdatingTask> weak_manifest_updater = storage.export_merge_tree_partition_manifest_updater;
                auto status_watch_callback = std::make_shared<Coordination::WatchCallback>([weak_manifest_updater, key](const Coordination::WatchResponse &)
                {
                    /// If the table is dropped but the watch is not removed, we need to prevent use after free
                    /// below code assumes that if manifest updater is still alive, the status handling task is also alive
                    if (auto manifest_updater = weak_manifest_updater.lock())
                    {
                        manifest_updater->addStatusChange(key);
                        manifest_updater->storage.export_merge_tree_partition_status_handling_task->schedule();
                    }
                });

                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetWatch);
    
                zk->tryGetWatch(fs::path(entry_path) / "status", status_string, nullptr, status_watch_callback);
            }

            if (status_string.empty())
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Skipping {}: missing status", key);
                continue;
            }

            const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_string);
            if (!status)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Invalid status {} for task {}, skipping", status_string, key);
                continue;
            }

            const bool skip_processed_refresh =
                has_local_entry
                && skipReadingDestinationFilePaths(*status, local_entry->destination_file_paths_per_part, metadata.number_of_parts);

            std::optional<std::map<String, std::vector<String>>> destination_file_paths_per_part;
            if (!skip_processed_refresh)
                destination_file_paths_per_part = readDestinationFilePathsPerPart(
                    zk, fs::path(entry_path), key, log);

            /// If we hold the cleanup lock, enforce the task timeout and recover uncommitted exports.
            /// Entries are never removed here, so we always fall through to refresh / addTask below.
            if (cleanup_lock)
            {
                tryCleanup(
                    zk,
                    entry_path,
                    log,
                    storage.getContext(),
                    storage,
                    metadata,
                    now,
                    *status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING,
                    deferred_commits);
            }

            if (!has_local_entry)
            {
                addTask(
                    metadata,
                    *status,
                    last_exception_per_replica ? std::move(*last_exception_per_replica) : std::map<String, LastExceptionEntry>{},
                    destination_file_paths_per_part ? std::move(*destination_file_paths_per_part) : std::map<String, std::vector<String>>{},
                    readCommitInfo(zk, fs::path(entry_path), key, log),
                    key,
                    entries_by_key);
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Added new entry for task {}", key);
                continue;
            }

            if (!local_entry->commit_info && *status == ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)
            {
                local_entry->commit_info = readCommitInfo(zk, fs::path(entry_path), key, log);
            }

            /// If we already have the local entry, we need to update it
            if (last_exception_per_replica)
                local_entry->last_exception_per_replica = std::move(*last_exception_per_replica);
            if (destination_file_paths_per_part)
                local_entry->destination_file_paths_per_part = std::move(*destination_file_paths_per_part);

            const bool status_changed = local_entry->status != *status;
            if (status_changed)
            {
                local_entry->status = *status;
                if (local_entry->status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
                {
                    /// terminal now - we no longer need to keep the data parts alive
                    local_entry->part_references.clear();

                    /// looks like we missed a status change event, we should kill local operations.
                    if (local_entry->status == ExportReplicatedMergeTreePartitionTaskEntry::Status::KILLED)
                    {
                        storage.killExportPart(local_entry->manifest.transaction_id);
                    }
                }
            }

            LOG_DEBUG(log, "ExportPartition Manifest Updating Task: Skipping {}: already exists", key);
            
        }

        removeStaleEntries(zk_children, entries_by_key);

        const auto entries_count = entries_by_key.size();

        /// Publish the updated copy atomically. `working_model` is moved out here, so
        /// `entries_by_key` (a reference into it) must not be used afterwards.
        storage.export_partition_manifests.set(std::move(working_model));

        LOG_DEBUG(log, "ExportPartition Manifest Updating task: finished polling for new entries. Number of entries: {}", entries_count);
    }

    /// Execute pending commits
    for (const auto & work : deferred_commits)
    {
        /// A replica exported the last part but the commit never landed. Try to fix it.
        try
        {
            ExportPartitionUtils::commit(work.metadata, work.destination_storage, zk, log, work.entry_path, work.context, storage, storage.getReplicaName());
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log,
                "ExportPartition Manifest Updating Task: "
                "Caught exception while committing export for {}: {}",
                work.entry_path, e.message());

            const bool became_failed = ExportPartitionUtils::handleCommitFailure(
                zk,
                work.entry_path,
                e.code(),
                storage.getReplicaName(),
                e.message(),
                log);

            if (became_failed)
            {
                LOG_WARNING(log,
                    "ExportPartition Manifest Updating Task: "
                    "Commit for {} transitioned to FAILED due to non-retryable error (code {})",
                    work.entry_path, e.code());
            }
        }
    }

    storage.export_merge_tree_partition_select_task->schedule();
}

void ExportPartitionManifestUpdatingTask::addTask(
    const ExportReplicatedMergeTreePartitionManifest & metadata,
    ExportReplicatedMergeTreePartitionTaskEntry::Status status,
    std::map<String, LastExceptionEntry> last_exception_per_replica,
    std::map<String, std::vector<String>> destination_file_paths_per_part,
    std::optional<ExportReplicatedMergeTreePartitionCommitInfoEntry> commit_info,
    const std::string & key,
    auto & entries_by_key
)
{
    std::vector<DataPartPtr> part_references;

    /// If the status is PENDING, we grab references to the data parts to prevent them from being deleted from the disk
    /// Otherwise, the operation has already been completed and there is no need to keep the data parts alive
    /// You might also ask: why bother adding tasks that have already been completed (i.e, status != PENDING)?
    /// The reason is the `replicated_partition_exports` table might miss entries if they are not added here.
    if (status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
    {
        for (const auto & part_name : metadata.parts)
        {
            if (const auto part = storage.getPartIfExists(part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated}))
            {
                part_references.push_back(part);
            }
        }
    }

    /// Called from poll() under M_task (sole mutator), so no extra locking is required.
    ExportReplicatedMergeTreePartitionTaskEntry entry {
        metadata,
        status,
        std::move(part_references),
        std::move(last_exception_per_replica),
        std::move(destination_file_paths_per_part),
        std::move(commit_info)};

    auto it = entries_by_key.find(key);
    if (it != entries_by_key.end())
    {
        if (!entries_by_key.replace(it, entry))
            LOG_ERROR(storage.log,
                "ExportPartition Manifest Updating Task: failed to replace in-memory entry for {} (transaction_id {}). "
                "This most likely means another export already holds the same transaction_id (id collision); "
                "this export will be missing from system.replicated_partition_exports.",
                key, entry.getTransactionId());
    }
    else if (!entries_by_key.insert(entry).second)
    {
        LOG_ERROR(storage.log,
            "ExportPartition Manifest Updating Task: failed to insert in-memory entry for {} (transaction_id {}). "
            "Another entry already holds this transaction_id (id collision); "
            "this export will be invisible in system.replicated_partition_exports.",
            key, entry.getTransactionId());
    }
}

void ExportPartitionManifestUpdatingTask::removeStaleEntries(
    const std::unordered_set<std::string> & zk_children,
    auto & entries_by_key
)
{
    for (auto it = entries_by_key.begin(); it != entries_by_key.end();)
    {
        const auto key = it->getCompositeKey();
        if (zk_children.contains(key))
        {
            ++it;
            continue;
        }

        LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Export task {} was deleted, calling killExportPartition for transaction {}", key, it->manifest.transaction_id);

        try
        {
            storage.killExportPart(it->manifest.transaction_id);
        }
        catch (...)
        {
            tryLogCurrentException(storage.log, __PRETTY_FUNCTION__);
        }

        it = entries_by_key.erase(it);
    }
}

void ExportPartitionManifestUpdatingTask::addStatusChange(const std::string & key)
{
    std::lock_guard lock(status_changes_mutex);
    status_changes.emplace(key);
}

void ExportPartitionManifestUpdatingTask::handleStatusChanges()
{
    /// copy the events to a local queue to avoid holding status_changes_mutex under M_task
    std::queue<std::string> local_status_changes;
    {
        std::lock_guard lock(status_changes_mutex);
        std::swap(status_changes, local_status_changes);
    }

    /// Take a snapshot of all status changes. If an exception is thrown, we will requeue the whole batch.
    const std::queue<std::string> batch = local_status_changes;
    const auto log = storage.log.load();

    try
    {
        /// M_task: serializes this against poll(). We copy the current read-model into a private
        /// mutable container, apply this batch's status transitions to that copy across the ZooKeeper
        /// reads below, and publish it atomically via export_read_model.set() at the end. Readers
        /// never see partial updates.
        std::lock_guard task_guard(background_task_serialization_mutex);
        auto zk = storage.getZooKeeper();

        const bool had_changes = !local_status_changes.empty();

        LOG_DEBUG(log, "ExportPartition Manifest Updating task: handling status changes. Number of status changes: {}", local_status_changes.size());

        const auto current_model = storage.export_partition_manifests.get();
        auto working_model = current_model
            ? std::make_unique<ExportPartitionTaskEntriesContainer>(*current_model)
            : std::make_unique<ExportPartitionTaskEntriesContainer>();
        auto & entries_by_key = working_model->get<ExportPartitionTaskEntryTagByCompositeKey>();

        while (!local_status_changes.empty())
        {
            const auto & key = local_status_changes.front();
            LOG_INFO(log, "ExportPartition Manifest Updating task: handling status change for task {}", key);

            fiu_do_on(FailPoints::export_partition_status_change_throw,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED,
                    "Failpoint: simulating exception during status change handling for key {}", key);
            });

            const auto it = entries_by_key.find(key);
            if (it == entries_by_key.end())
            {
                local_status_changes.pop();
                continue;
            }

            const auto export_path = fs::path(storage.zookeeper_path) / "exports" / key;

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            /// get new status from zk
            std::string new_status_string;
            if (!zk->tryGet(export_path / "status", new_status_string))
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Failed to get new status for task {}, skipping", key);
                local_status_changes.pop();
                continue;
            }

            const auto new_status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(new_status_string);
            if (!new_status)
            {
                LOG_WARNING(log, "ExportPartition Manifest Updating Task: Invalid status {} for task {}, skipping", new_status_string, key);
                local_status_changes.pop();
                continue;
            }

            LOG_INFO(log, "ExportPartition Manifest Updating task: status changed for task {}. New status: {}", key, magic_enum::enum_name(*new_status).data());

            auto fetched = readLastExceptionPerReplica(
                zk, export_path, key, log);

            if (!skipReadingDestinationFilePaths(*new_status, it->destination_file_paths_per_part, it->manifest.number_of_parts))
            {
                auto destination_file_paths_per_part = readDestinationFilePathsPerPart(
                    zk, export_path, key, log);
                it->destination_file_paths_per_part = std::move(destination_file_paths_per_part);
            }

            if (*new_status == ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)
            {
                if (auto fetched_commit_info = readCommitInfo(zk, export_path, key, log))
                    it->commit_info = std::move(fetched_commit_info);
            }

            /// If status changed to KILLED, cancel local export operations
            if (*new_status == ExportReplicatedMergeTreePartitionTaskEntry::Status::KILLED)
            {
                try
                {
                    LOG_INFO(log, "ExportPartition Manifest Updating task: killing export partition for task {}", key);
                    storage.killExportPart(it->manifest.transaction_id);
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                }
            }

            /// Apply the in-memory updates directly (poll() cannot run concurrently under M_task).
            if (fetched)
                it->last_exception_per_replica = std::move(*fetched);

            it->status = *new_status;

            if (it->status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
            {
                /// we no longer need to keep the data parts alive
                it->part_references.clear();
            }

            local_status_changes.pop();
        }

        /// Publish this batch's status transitions to readers. `working_model` is moved out here,
        /// so `entries_by_key` (a reference into it) must not be used afterwards.
        if (had_changes)
            storage.export_partition_manifests.set(std::move(working_model));
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        LOG_WARNING(log, "ExportPartition Manifest Updating task: exception thrown while handling status changes; nothing was published, requeuing the whole batch. Batch size: {}", batch.size());

        std::lock_guard lock(status_changes_mutex);

        /// upon exception, requeue the whole batch
        if (!batch.empty())
        {
            std::queue<std::string> requeued = batch;
            while (!status_changes.empty())
            {
                requeued.push(std::move(status_changes.front()));
                status_changes.pop();
            }

            std::swap(status_changes, requeued);
        }

        LOG_DEBUG(log, "ExportPartition Manifest Updating task: pending status changes after requeue: {}", status_changes.size());

        throw;
    }
}

}
