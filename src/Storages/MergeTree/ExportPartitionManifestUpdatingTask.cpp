#include <Storages/MergeTree/ExportPartitionManifestUpdatingTask.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Common/logger_useful.h"
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{
namespace
{
    /*
        Remove expired entries and fix non-committed exports that have already exported all parts.

        Return values:
        - true: the cleanup was successful, the entry is removed from the entries_by_key container and the function returns true. Proceed to the next entry.
        - false: the cleanup was not successful, the entry is not removed from the entries_by_key container and the function returns false.
    */
    bool tryCleanup(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        const LoggerPtr & log,
        const ContextPtr & context,
        const std::string & key,
        const ExportReplicatedMergeTreePartitionManifest & metadata,
        const time_t now,
        const bool is_pending,
        auto & entries_by_key
    )
    {
        bool has_expired = metadata.create_time < now - static_cast<time_t>(metadata.ttl_seconds);

        if (has_expired && !is_pending)
        {
            zk->tryRemoveRecursive(fs::path(entry_path));
            auto it = entries_by_key.find(key);
            if (it != entries_by_key.end())
                entries_by_key.erase(it);
            LOG_INFO(log, "ExportPartition Manifest Updating Task: Removed {}: expired", key);

            return true;
        }
        else if (is_pending)
        {
            std::vector<std::string> parts_in_processing_or_pending;
            if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(entry_path) / "processing", parts_in_processing_or_pending))
            {
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Failed to get parts in processing or pending, skipping");
                return false;
            }

            if (parts_in_processing_or_pending.empty())
            {
                LOG_INFO(log, "ExportPartition Manifest Updating Task: Cleanup found PENDING for {} with all parts exported, try to fix it by committing the export", entry_path);
    
                const auto destination_storage_id = StorageID(QualifiedTableName {metadata.destination_database, metadata.destination_table});
                const auto destination_storage = DatabaseCatalog::instance().tryGetTable(destination_storage_id, context);
                if (!destination_storage)
                {
                    LOG_INFO(log, "ExportPartition Manifest Updating Task: Failed to reconstruct destination storage: {}, skipping", destination_storage_id.getNameForLogs());
                    return false;
                }

                /// it sounds like a replica exported the last part, but was not able to commit the export. Try to fix it
                ExportPartitionUtils::commit(metadata, destination_storage, zk, log, entry_path, context);

                return true;
            }
        }

        return false;
    }
}

ExportPartitionManifestUpdatingTask::ExportPartitionManifestUpdatingTask(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
}

void ExportPartitionManifestUpdatingTask::poll()
{
    std::lock_guard lock(storage.export_merge_tree_partition_mutex);

    LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Polling for new entries for table {}. Current number of entries: {}", storage.getStorageID().getNameForLogs(), storage.export_merge_tree_partition_task_entries_by_key.size());

    auto zk = storage.getZooKeeper();
    
    const std::string exports_path = fs::path(storage.zookeeper_path) / "exports";
    const std::string cleanup_lock_path = fs::path(storage.zookeeper_path) / "exports_cleanup_lock";

    auto cleanup_lock = zkutil::EphemeralNodeHolder::tryCreate(cleanup_lock_path, *zk, storage.replica_name);
    if (cleanup_lock)
    {
        LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Cleanup lock acquired, will remove stale entries");
    }

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

        std::string metadata_json;
        if (!zk->tryGet(fs::path(entry_path) / "metadata.json", metadata_json))
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: missing metadata.json", key);
            continue;
        }

        const auto metadata = ExportReplicatedMergeTreePartitionManifest::fromJsonString(metadata_json);

        const auto local_entry = entries_by_key.find(key);

        /// If the zk entry has been replaced with export_merge_tree_partition_force_export, checking only for the export key is not enough
        /// we need to make sure it is the same transaction id. If it is not, it needs to be replaced.
        bool has_local_entry_and_is_up_to_date = local_entry != entries_by_key.end()
            && local_entry->manifest.transaction_id == metadata.transaction_id;

        /// If the entry is up to date and we don't have the cleanup lock, early exit, nothing to be done.
        if (!cleanup_lock && has_local_entry_and_is_up_to_date)
            continue;

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

        std::string status;
        if (!zk->tryGetWatch(fs::path(entry_path) / "status", status, nullptr, status_watch_callback))
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: missing status", key);
            continue;
        }

        bool is_pending = status == "PENDING";

        /// if we have the cleanup lock, try to cleanup
        /// if we successfully cleaned it up, early exit
        if (cleanup_lock)
        {
            bool cleanup_successful = tryCleanup(
                zk,
                entry_path,
                storage.log.load(),
                storage.getContext(),
                key,
                metadata,
                now,
                is_pending, entries_by_key);

            if (cleanup_successful)
                continue;
        }

        if (!is_pending)
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: status is not PENDING", key);
            continue;
        }

        if (has_local_entry_and_is_up_to_date)
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: already exists", key);
            continue;
        }

        addTask(metadata, key, entries_by_key);
    }

    /// Remove entries that were deleted by someone else
    removeStaleEntries(zk_children, entries_by_key);

    LOG_INFO(storage.log, "ExportPartition Manifest Updating task: finished polling for new entries. Number of entries: {}", entries_by_key.size());

    storage.export_merge_tree_partition_select_task->schedule();
}

void ExportPartitionManifestUpdatingTask::addTask(
    const ExportReplicatedMergeTreePartitionManifest & metadata,
    const std::string & key,
    auto & entries_by_key
)
{
    std::vector<DataPartPtr> part_references;

    for (const auto & part_name : metadata.parts)
    {
        if (const auto part = storage.getPartIfExists(part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated}))
        {
            part_references.push_back(part);
        }
    }

    /// Insert or update entry. The multi_index container automatically maintains both indexes.
    auto entry = ExportReplicatedMergeTreePartitionTaskEntry {metadata, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, std::move(part_references)};
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
    std::lock_guard lock(status_changes_mutex);
    std::lock_guard task_entries_lock(storage.export_merge_tree_partition_mutex);
    auto zk = storage.getZooKeeper();

    LOG_INFO(storage.log, "ExportPartition Manifest Updating task: handling status changes. Number of status changes: {}", status_changes.size());

    while (!status_changes.empty())
    {
        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: handling status change for task {}", status_changes.front());
        const auto key = status_changes.front();
        status_changes.pop();

        auto it = storage.export_merge_tree_partition_task_entries_by_key.find(key);
        if (it == storage.export_merge_tree_partition_task_entries_by_key.end())
            continue;

        /// get new status from zk
        std::string new_status_string;
        if (!zk->tryGet(fs::path(storage.zookeeper_path) / "exports" / key / "status", new_status_string))
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Failed to get new status for task {}, skipping", key);
            continue;
        }

        const auto new_status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(new_status_string);
        if (!new_status)
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Invalid status {} for task {}, skipping", new_status_string, key);
            continue;
        }

        LOG_INFO(storage.log, "ExportPartition Manifest Updating task: status changed for task {}. New status: {}", key, magic_enum::enum_name(*new_status).data());

        /// If status changed to KILLED, cancel local export operations
        if (*new_status == ExportReplicatedMergeTreePartitionTaskEntry::Status::KILLED)
        {
            try
            {
                storage.killExportPart(it->manifest.transaction_id);
            }
            catch (...)
            {
                tryLogCurrentException(storage.log, __PRETTY_FUNCTION__);
            }
        }

        it->status = *new_status;
    }
}

}
