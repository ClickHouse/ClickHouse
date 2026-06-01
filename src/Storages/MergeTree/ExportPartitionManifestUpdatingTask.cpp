#include <Storages/MergeTree/ExportPartitionManifestUpdatingTask.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Common/logger_useful.h"
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/DatabaseCatalog.h>
#include <fmt/format.h>

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
}

namespace
{
    /// Work item describing a commit-recovery attempt that has been deferred out of
    /// the `poll()` critical section. Captures everything by value so it can be
    /// executed safely after `export_merge_tree_partition_mutex` has been released.
    struct CommitRecoveryWork
    {
        ExportReplicatedMergeTreePartitionManifest metadata;
        std::string entry_path;
        StoragePtr destination_storage;
        ContextPtr context;
    };
    /// Fetch all per-replica last_exception leaves under <entry_path>/last_exception and build
    /// a fresh map keyed by replica name. The map key prefers the unescaped `replica` field
    /// embedded in the JSON payload; if it is missing or empty, the leaf name is unescaped as
    /// a fallback.
    ///
    /// An empty result means "nothing actionable": either the parent getChildren failed (ZK
    /// glitch), the container has no children yet (no replica has reported), or every leaf
    /// fetch came back ZNONODE / malformed. Callers MUST skip the assignment in that case to
    /// preserve the in-memory mirror across transient errors. This is safe because per-replica
    /// leaves are never individually removed — the entire entry path is wiped recursively when
    /// a task is cleaned up, which is handled separately by removeStaleEntries.
    std::map<String, LastExceptionEntry> readLastExceptionPerReplica(
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
            LOG_INFO(log, "ExportPartition Manifest Updating Task: failed to list last_exception leaves for {}, leaving in-memory copy untouched", log_key);
            return out;
        }

        if (children.empty())
            return out;

        std::vector<std::string> paths;
        paths.reserve(children.size());
        for (const auto & child : children)
            paths.emplace_back(container_path / child);

        /// One MULTI_READ when supported, parallel async gets otherwise. See
        /// ZooKeeper::multiRead in src/Common/ZooKeeper/ZooKeeper.h.
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

    /*
        Remove expired entries and fix non-committed exports that have already exported all parts.

        Return values:
        - true: the cleanup was successful, the entry is removed from the entries_by_key container and the function returns true. Proceed to the next entry.
        - false: the cleanup was not successful, the entry is not removed from the entries_by_key container and the function returns false.

        Side outputs:
        - `deferred_commits`: when a PENDING entry has all parts processed but the export was
          never committed, this function appends a CommitRecoveryWork item to be executed by
          the caller after releasing the storage-wide mutex. The actual commit() call (which
          performs network I/O to the destination catalog and S3) MUST NOT run under the lock.
          The function still returns `false` in that case so the outer poll() loop falls through
          to `addTask`, keeping the in-memory entry consistent regardless of whether the
          deferred commit ultimately succeeds.
    */
    bool tryCleanup(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        const LoggerPtr & log,
        const ContextPtr & storage_context,
        StorageReplicatedMergeTree & storage,
        const std::string & key,
        const ExportReplicatedMergeTreePartitionManifest & metadata,
        const time_t now,
        const bool is_pending,
        auto & entries_by_key,
        std::vector<CommitRecoveryWork> & deferred_commits
    )
    {
        bool has_expired = metadata.create_time < now - static_cast<time_t>(metadata.ttl_seconds);

        bool task_timed_out = is_pending
            && metadata.task_timeout_seconds > 0
            && metadata.create_time + static_cast<time_t>(metadata.task_timeout_seconds) < now;

        if (has_expired && !is_pending)
        {
            zk->tryRemoveRecursive(fs::path(entry_path));
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRemoveRecursive);
            auto it = entries_by_key.find(key);
            if (it != entries_by_key.end())
                entries_by_key.erase(it);
            LOG_INFO(log, "ExportPartition Manifest Updating Task: Removed {}: expired", key);

            return true;
        }
        else if (task_timed_out)
        {
            const std::string status_path = fs::path(entry_path) / "status";

            Coordination::Stat status_stat;
            std::string status_string;

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            if (!zk->tryGet(status_path, status_string, &status_stat))
            {
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Failed to read status for {} while enforcing task timeout, skipping", entry_path);
                return false;
            }

            const auto current_status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_string);
            if (!current_status || *current_status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
            {
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Task {} is not PENDING, can't set to KILLED, skipping", entry_path);
                return false;
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
                LOG_INFO(log,
                    "ExportPartition Manifest Updating Task: atomic kill for {} failed (rc={}); "
                    "status was concurrently updated or a ZK op conflicted, will retry on next poll",
                    entry_path, rc);
            }

            /// Return false so the entry remains in entries_by_key; the status watch will drive
            /// handleStatusChanges -> killExportPart on every replica, mirroring user-initiated KILL.
            return false;
        }
        else if (is_pending)
        {
            auto context = ExportPartitionUtils::getContextCopyWithTaskSettings(storage_context, metadata);

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
            std::vector<std::string> parts_in_processing_or_pending;
            if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(entry_path) / "processing", parts_in_processing_or_pending))
            {

                LOG_INFO(log, "ExportPartition Manifest Updating Task: Failed to get parts in processing or pending, skipping");
                return false;
            }

            if (parts_in_processing_or_pending.empty())
            {
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Cleanup found PENDING for {} with all parts exported, deferring commit recovery to post-lock phase", entry_path);

                const auto destination_storage_id = StorageID(QualifiedTableName {metadata.destination_database, metadata.destination_table});
                const auto destination_storage = DatabaseCatalog::instance().tryGetTable(destination_storage_id, context);
                if (!destination_storage)
                {
                    LOG_INFO(log, "ExportPartition Manifest Updating Task: Failed to reconstruct destination storage: {}, skipping", destination_storage_id.getNameForLogs());
                    return false;
                }

                /// A replica exported the last part but the commit never landed. Capture everything
                /// needed to run commit() outside `export_merge_tree_partition_mutex`. The
                /// commit path performs network I/O (REST catalog + S3) with up to
                /// MAX_TRANSACTION_RETRIES = 100 retries; holding the storage-wide mutex across
                /// that work is what caused `system.replicated_partition_exports` to hang.
                ///
                /// Returning false here keeps the outer poll() loop on the normal path: it will
                /// call addTask() so the in-memory container reflects the PENDING entry. The
                /// status watch registered by poll() will transition the local entry to
                /// COMPLETED/FAILED once the deferred commit (or a peer's commit) updates
                /// /status in ZooKeeper.
                deferred_commits.push_back(CommitRecoveryWork{
                    .metadata = metadata,
                    .entry_path = entry_path,
                    .destination_storage = destination_storage,
                    .context = context,
                });
                return false;
            }
        }

        return false;
    }
}

ExportPartitionManifestUpdatingTask::ExportPartitionManifestUpdatingTask(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
}

std::vector<ReplicatedPartitionExportInfo> ExportPartitionManifestUpdatingTask::getPartitionExportsInfo() const
{
    /// Snapshot just the fields we need under the lock, then build the public
    /// ReplicatedPartitionExportInfo vector after releasing it. This keeps the critical
    /// section O(entries) of cheap struct copies (manifest + enum) rather than also
    /// covering the formatting / Array construction performed by the caller.
    struct EntrySnapshot
    {
        ExportReplicatedMergeTreePartitionManifest manifest;
        ExportReplicatedMergeTreePartitionTaskEntry::Status status;
        std::map<String, LastExceptionEntry> last_exception_per_replica;
    };

    std::vector<EntrySnapshot> snapshots;

    {
        std::lock_guard lock(storage.export_merge_tree_partition_mutex);

        snapshots.reserve(storage.export_merge_tree_partition_task_entries_by_key.size());
        for (const auto & entry : storage.export_merge_tree_partition_task_entries_by_key)
            snapshots.push_back(EntrySnapshot{entry.manifest, entry.status, entry.last_exception_per_replica});
    }

    std::vector<ReplicatedPartitionExportInfo> infos;
    infos.reserve(snapshots.size());

    for (auto & snapshot : snapshots)
    {
        ReplicatedPartitionExportInfo info;

        info.destination_database = snapshot.manifest.destination_database;
        info.destination_table = snapshot.manifest.destination_table;
        info.partition_id = snapshot.manifest.partition_id;
        info.transaction_id = snapshot.manifest.transaction_id;
        info.query_id = snapshot.manifest.query_id;
        info.create_time = snapshot.manifest.create_time;
        info.source_replica = snapshot.manifest.source_replica;
        info.parts_count = snapshot.manifest.number_of_parts;
        info.parts_to_do = snapshot.manifest.parts.size();
        info.parts = std::move(snapshot.manifest.parts);
        info.status = magic_enum::enum_name(snapshot.status);

        info.last_exception_per_replica.reserve(snapshot.last_exception_per_replica.size());
        size_t total_exception_count = 0;
        for (const auto & [_, ex] : snapshot.last_exception_per_replica)
        {
            total_exception_count += ex.count;
            info.last_exception_per_replica.push_back(ex);
        }
        info.exception_count = total_exception_count;

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

    const std::string exports_path = fs::path(storage.zookeeper_path) / "exports";
    const std::string cleanup_lock_path = fs::path(storage.zookeeper_path) / "exports_cleanup_lock";

    /// The `exports_cleanup_lock` is an ephemeral ZK node that serializes cleanup work
    /// across replicas: only the replica holding it walks `tryCleanup` (entry expiry +
    /// commit recovery). It MUST outlive the deferred-commit loop below; otherwise a peer
    /// replica's next poll() could acquire it and race us on the same commit-recovery work,
    /// duplicating REST-catalog round-trips and snapshot writes. The EphemeralNodeHolder
    /// destructor removes the node, so we declare it at function scope and let it die
    /// at the end of poll() - after all deferred commits have completed.
    /// Acquired here (no mutex needed - it is just a ZK ephemeral create).
    auto cleanup_lock = zkutil::EphemeralNodeHolder::tryCreate(cleanup_lock_path, *zk, storage.replica_name);
    if (cleanup_lock)
    {
        LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Cleanup lock acquired, will remove stale entries");
    }

    {
        std::lock_guard lock(storage.export_merge_tree_partition_mutex);

        LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Polling for new entries for table {}. Current number of entries: {}", storage.getStorageID().getNameForLogs(), storage.export_merge_tree_partition_task_entries_by_key.size());

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildrenWatch);

        Coordination::Stat stat;
        const auto children = zk->getChildrenWatch(exports_path, &stat, storage.export_merge_tree_partition_watch_callback);
        const std::unordered_set<std::string> zk_children(children.begin(), children.end());

        const auto now = time(nullptr);

        auto & entries_by_key = storage.export_merge_tree_partition_task_entries_by_key;

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
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: missing metadata.json", key);
                continue;
            }

            const auto metadata = ExportReplicatedMergeTreePartitionManifest::fromJsonString(metadata_json);

            /// Read last_exception leaves (no watch). Surfacing exceptions in the system table relies
            /// on this read being part of every poll cycle: per-part failures during PENDING do not
            /// trigger a status watch, so the only refresh path while the task is still in-flight is
            /// the periodic poll. An empty result collapses every "nothing actionable" case
            /// (transient ZK error, no children, all leaves ZNONODE/malformed) into a no-op so the
            /// in-memory copy stays intact.
            auto last_exception_per_replica = readLastExceptionPerReplica(
                zk, fs::path(entry_path), key, storage.log.load());

            const auto local_entry = entries_by_key.find(key);

            /// If the zk entry has been replaced with export_merge_tree_partition_force_export, checking only for the export key is not enough
            /// we need to make sure it is the same transaction id. If it is not, it needs to be replaced.
            bool has_local_entry_and_is_up_to_date = local_entry != entries_by_key.end()
                && local_entry->manifest.transaction_id == metadata.transaction_id;

            /// If the entry is up to date and we don't have the cleanup lock, refresh the in-memory
            /// last_exception (surfaced by system.replicated_partition_exports) and early exit.
            /// Direct mutation of the `mutable` field is safe under export_merge_tree_partition_mutex,
            /// which is held throughout poll().
            if (!cleanup_lock && has_local_entry_and_is_up_to_date)
            {
                if (!last_exception_per_replica.empty())
                    local_entry->last_exception_per_replica = std::move(last_exception_per_replica);
                continue;
            }

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
            std::string status_string;
            if (!zk->tryGetWatch(fs::path(entry_path) / "status", status_string, nullptr, status_watch_callback))
            {
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: missing status", key);
                continue;
            }

            const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_string);
            if (!status)
            {
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Invalid status {} for task {}, skipping", status_string, key);
                continue;
            }

            /// if we have the cleanup lock, try to cleanup
            /// if we successfully cleaned it up, early exit
            if (cleanup_lock)
            {
                bool cleanup_successful = tryCleanup(
                    zk,
                    entry_path,
                    storage.log.load(),
                    storage.getContext(),
                    storage,
                    key,
                    metadata,
                    now,
                    *status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING,
                    entries_by_key,
                    deferred_commits);

                if (cleanup_successful)
                    continue;
            }

            if (has_local_entry_and_is_up_to_date)
            {
                /// Same refresh as the early-exit branch above; we also reach this point when
                /// holding the cleanup lock (cleanup did not consume the entry).
                if (!last_exception_per_replica.empty())
                    local_entry->last_exception_per_replica = std::move(last_exception_per_replica);
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: already exists", key);
                continue;
            }

            addTask(metadata, *status, std::move(last_exception_per_replica), key, entries_by_key);
        }

        /// Remove entries that were deleted by someone else
        removeStaleEntries(zk_children, entries_by_key);

        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: finished polling for new entries. Number of entries: {}", entries_by_key.size());
    }
    /// `export_merge_tree_partition_mutex` released here. Everything below runs without it
    /// so concurrent readers of `system.replicated_partition_exports` and other writers are
    /// not blocked by the (potentially slow) catalog round-trips below.
    ///
    /// `cleanup_lock` (the ZK ephemeral node) is INTENTIONALLY still held here and is only
    /// destructed at end of function. This preserves the existing cross-replica invariant:
    /// at any moment only one replica is performing commit recovery for a given table, so
    /// peer replicas will not race us on the same `commit()` calls below.
    ///
    /// Shutdown safety: this function runs on a BackgroundSchedulePool task that
    /// `StorageReplicatedMergeTree::shutdown()` deactivates before clearing the entry
    /// container. Deactivation waits for the currently-running invocation (this very call)
    /// to return before proceeding, so the deferred commits below complete (or throw) before
    /// any teardown observes empty `export_merge_tree_partition_task_entries`. All work
    /// items capture their inputs by value, so they are independent from container state.

    const auto log_ptr = storage.log.load();

    for (const auto & work : deferred_commits)
    {
        /// A replica exported the last part but the commit never landed. Try to fix it.
        try
        {
            ExportPartitionUtils::commit(work.metadata, work.destination_storage, zk, log_ptr, work.entry_path, work.context, storage, storage.getReplicaName());
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log_ptr,
                "ExportPartition Manifest Updating Task: "
                "Caught exception while committing export for {}: {}",
                work.entry_path, e.message());

            /// Bump commit-attempts counter; transition to FAILED once the budget is exhausted.
            /// This is the primary retry path for the commit phase — handlePartExportSuccess
            /// only fires once (on the last part's completion); subsequent retries come from here.
            const bool became_failed = ExportPartitionUtils::handleCommitFailure(
                zk,
                work.entry_path,
                work.metadata.max_retries,
                storage.getReplicaName(),
                e.message(),
                log_ptr);

            if (became_failed)
            {
                LOG_WARNING(log_ptr,
                    "ExportPartition Manifest Updating Task: "
                    "Commit for {} transitioned to FAILED after exhausting max_retries={}",
                    work.entry_path, work.metadata.max_retries);
            }
        }
    }

    storage.export_merge_tree_partition_select_task->schedule();
}

void ExportPartitionManifestUpdatingTask::addTask(
    const ExportReplicatedMergeTreePartitionManifest & metadata,
    ExportReplicatedMergeTreePartitionTaskEntry::Status status,
    std::map<String, LastExceptionEntry> last_exception_per_replica,
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

    /// Insert or update entry. The multi_index container automatically maintains both indexes.
    ExportReplicatedMergeTreePartitionTaskEntry entry {metadata, status, std::move(part_references), std::move(last_exception_per_replica)};
    auto it = entries_by_key.find(key);
    if (it != entries_by_key.end())
        entries_by_key.replace(it, entry);
    else
        entries_by_key.insert(entry);
}

void ExportPartitionManifestUpdatingTask::removeStaleEntries(
    const std::unordered_set<std::string> & zk_children,
    auto & entries_by_key
)
{
    for (auto it = entries_by_key.begin(); it != entries_by_key.end();)
    {
        const auto & key = it->getCompositeKey();
        if (zk_children.contains(key))
        {
            ++it;
            continue;
        }

        const auto & transaction_id = it->manifest.transaction_id;
        LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Export task {} was deleted, calling killExportPartition for transaction {}", key, transaction_id);
        
        try
        {
            storage.killExportPart(transaction_id);
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
    /// copy the events to a local queue to avoid holding the status_changes_mutex while also holding export_merge_tree_partition_mutex
    std::queue<std::string> local_status_changes;
    {
        std::lock_guard lock(status_changes_mutex);
        std::swap(status_changes, local_status_changes);
    }

    try
    {
        std::lock_guard task_entries_lock(storage.export_merge_tree_partition_mutex);
        auto zk = storage.getZooKeeper();

        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: handling status changes. Number of status changes: {}", local_status_changes.size());

        while (!local_status_changes.empty())
        {
            const auto & key = local_status_changes.front();
            LOG_INFO(storage.log, "ExportPartition Manifest Updating task: handling status change for task {}", key);

            fiu_do_on(FailPoints::export_partition_status_change_throw,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED,
                    "Failpoint: simulating exception during status change handling for key {}", key);
            });

            auto it = storage.export_merge_tree_partition_task_entries_by_key.find(key);
            if (it == storage.export_merge_tree_partition_task_entries_by_key.end())
            {
                local_status_changes.pop();
                continue;
            }

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            /// get new status from zk
            std::string new_status_string;
            if (!zk->tryGet(fs::path(storage.zookeeper_path) / "exports" / key / "status", new_status_string))
            {
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Failed to get new status for task {}, skipping", key);
                local_status_changes.pop();
                continue;
            }

            const auto new_status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(new_status_string);
            if (!new_status)
            {
                LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Invalid status {} for task {}, skipping", new_status_string, key);
                local_status_changes.pop();
                continue;
            }

            LOG_INFO(storage.log, "ExportPartition Manifest Updating task: status changed for task {}. New status: {}", key, magic_enum::enum_name(*new_status).data());

            /// Refresh last_exception leaves too. Status transitions to FAILED (via commit budget)
            /// and KILLED (via timeout) atomically write a per-replica leaf in the same multi, so
            /// reading them here ensures the system table surfaces the cause together with the
            /// visible state change. No new watch is added — this piggybacks on the existing
            /// status watch. An empty result means "nothing actionable" and leaves the previous
            /// snapshot intact.
            if (auto fetched = readLastExceptionPerReplica(
                    zk, fs::path(storage.zookeeper_path) / "exports" / key, key, storage.log.load());
                !fetched.empty())
            {
                it->last_exception_per_replica = std::move(fetched);
            }

            /// If status changed to KILLED, cancel local export operations
            if (*new_status == ExportReplicatedMergeTreePartitionTaskEntry::Status::KILLED)
            {
                try
                {
                    LOG_INFO(storage.log, "ExportPartition Manifest Updating task: killing export partition for task {}", key);
                    storage.killExportPart(it->manifest.transaction_id);
                }
                catch (...)
                {
                    tryLogCurrentException(storage.log, __PRETTY_FUNCTION__);
                }
            }

            it->status = *new_status;

            if (it->status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
            {
                /// we no longer need to keep the data parts alive
                it->part_references.clear();
            }

            local_status_changes.pop();
        }
    }
    catch (...)
    {
        tryLogCurrentException(storage.log, __PRETTY_FUNCTION__);

        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: exception thrown while handling status changes, enqueuing remaining status changes back to the status_changes queue. Number of remaining status changes: {}", local_status_changes.size());

        std::lock_guard lock(status_changes_mutex);

        /// It is possible that an exception is thrown while handling the status. In this scenario
        /// we need to enqueue the remaining status changes back to the status_changes queue not to lose them.
        /// The other solution to this problem would be to ignore it and schedule a poll - maybe it is simpler?
        if (!local_status_changes.empty())
        {
            // Prepend remaining items before any newly-arrived items
            while (!status_changes.empty())
            {
                local_status_changes.push(std::move(status_changes.front()));
                status_changes.pop();
            }

            std::swap(status_changes, local_status_changes);
        }

        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: The new number of pending status after enqueueing unprocessed ones is {}", status_changes.size());

        throw;
    }
}

}
