#include <Storages/MergeTree/ExportPartitionManifestUpdatingTask.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Common/logger_useful.h"
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/DatabaseCatalog.h>

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
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRemoveRecursive);
            auto it = entries_by_key.find(key);
            if (it != entries_by_key.end())
                entries_by_key.erase(it);
            LOG_INFO(log, "ExportPartition Manifest Updating Task: Removed {}: expired", key);

            return true;
        }
        else if (is_pending)
        {
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

std::vector<ReplicatedPartitionExportInfo> ExportPartitionManifestUpdatingTask::getPartitionExportsInfo() const
{
    std::vector<ReplicatedPartitionExportInfo> infos;
    const auto zk = storage.getZooKeeper();

    const auto exports_path = fs::path(storage.zookeeper_path) / "exports";

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);

    std::vector<std::string> children;
    if (Coordination::Error::ZOK != zk->tryGetChildren(exports_path, children))
    {
        LOG_INFO(storage.log, "Failed to get children from exports path, returning empty export info list");
        return infos;
    }

    if (children.empty())
        return infos;

    /// Batch all metadata.json, status gets, and getChildren operations in a single multi request
    Coordination::Requests requests;
    requests.reserve(children.size() * 4); // metadata, status, processing, exceptions_per_replica

    // Track response indices for each child
    struct ChildResponseIndices
    {
        size_t metadata_idx;
        size_t status_idx;
        size_t processing_idx;
        size_t exceptions_per_replica_idx;
    };
    std::vector<ChildResponseIndices> response_indices;
    response_indices.reserve(children.size());

    for (const auto & child : children)
    {
        const auto export_partition_path = fs::path(exports_path) / child;
        
        ChildResponseIndices indices;
        indices.metadata_idx = requests.size();
        requests.push_back(zkutil::makeGetRequest(export_partition_path / "metadata.json"));
        
        indices.status_idx = requests.size();
        requests.push_back(zkutil::makeGetRequest(export_partition_path / "status"));
        
        indices.processing_idx = requests.size();
        requests.push_back(zkutil::makeListRequest(export_partition_path / "processing"));
        
        indices.exceptions_per_replica_idx = requests.size();
        requests.push_back(zkutil::makeListRequest(export_partition_path / "exceptions_per_replica"));
        
        response_indices.push_back(indices);
    }

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);

    Coordination::Responses responses;
    Coordination::Error code = zk->tryMulti(requests, responses);

    if (code != Coordination::Error::ZOK)
    {
        LOG_INFO(storage.log, "Failed to execute multi request for export partition info, error: {}", code);
        return infos;
    }

    // Helper to extract GetResponse data
    auto getGetResponseData = [&responses](size_t idx) -> std::pair<Coordination::Error, std::string>
    {
        if (idx >= responses.size())
            return {Coordination::Error::ZRUNTIMEINCONSISTENCY, ""};
        
        const auto * get_response = dynamic_cast<const Coordination::GetResponse *>(responses[idx].get());
        if (!get_response)
            return {Coordination::Error::ZRUNTIMEINCONSISTENCY, ""};
        
        return {get_response->error, get_response->data};
    };

    // Helper to extract ListResponse data
    auto getListResponseData = [&responses](size_t idx) -> std::pair<Coordination::Error, Strings>
    {
        if (idx >= responses.size())
            return {Coordination::Error::ZRUNTIMEINCONSISTENCY, Strings{}};
        
        const auto * list_response = dynamic_cast<const Coordination::ListResponse *>(responses[idx].get());
        if (!list_response)
            return {Coordination::Error::ZRUNTIMEINCONSISTENCY, Strings{}};
        
        return {list_response->error, list_response->names};
    };

    // Create response wrappers matching the MultiTryGetResponse/MultiTryGetChildrenResponse interface
    struct ResponseWrapper
    {
        Coordination::Error error;
        std::string data;
        Strings names;
        
        ResponseWrapper(Coordination::Error err, const std::string & d, const Strings & n) 
            : error(err), data(d), names(n) {}
    };

    std::vector<ResponseWrapper> metadata_responses_wrapper;
    std::vector<ResponseWrapper> status_responses_wrapper;
    std::vector<ResponseWrapper> processing_responses_wrapper;
    std::vector<ResponseWrapper> exceptions_per_replica_responses_wrapper;

    metadata_responses_wrapper.reserve(children.size());
    status_responses_wrapper.reserve(children.size());
    processing_responses_wrapper.reserve(children.size());
    exceptions_per_replica_responses_wrapper.reserve(children.size());

    for (size_t child_idx = 0; child_idx < children.size(); ++child_idx)
    {
        const auto & indices = response_indices[child_idx];
        
        // Extract metadata response
        auto [metadata_error, metadata_data] = getGetResponseData(indices.metadata_idx);
        metadata_responses_wrapper.emplace_back(metadata_error, metadata_data, Strings{});
        
        // Extract status response
        auto [status_error, status_data] = getGetResponseData(indices.status_idx);
        status_responses_wrapper.emplace_back(status_error, status_data, Strings{});
        
        // Extract processing response
        auto [processing_error, processing_names] = getListResponseData(indices.processing_idx);
        processing_responses_wrapper.emplace_back(processing_error, "", processing_names);
        
        // Extract exceptions_per_replica response
        auto [exceptions_error, exceptions_names] = getListResponseData(indices.exceptions_per_replica_idx);
        exceptions_per_replica_responses_wrapper.emplace_back(exceptions_error, "", exceptions_names);
    }

    // Use wrapper vectors directly - they match the interface expected by the code below
    auto & metadata_responses = metadata_responses_wrapper;
    auto & status_responses = status_responses_wrapper;
    auto & processing_responses = processing_responses_wrapper;
    auto & exceptions_per_replica_responses = exceptions_per_replica_responses_wrapper;

    /// Collect all exception replica paths for batching
    struct ExceptionReplicaPath
    {
        size_t child_idx;
        std::string replica;
        std::string count_path;
        std::string exception_path;
        std::string part_path;
    };

    std::vector<ExceptionReplicaPath> exception_replica_paths;
    for (size_t child_idx = 0; child_idx < children.size(); ++child_idx)
    {
        const auto & child = children[child_idx];
        const auto export_partition_path = fs::path(exports_path) / child;
        /// Check if we got valid responses
        if (metadata_responses[child_idx].error != Coordination::Error::ZOK)
        {
            LOG_INFO(storage.log, "Skipping {}: missing metadata.json", child);
            continue;
        }
        if (status_responses[child_idx].error != Coordination::Error::ZOK)
        {
            LOG_INFO(storage.log, "Skipping {}: missing status", child);
            continue;
        }
        if (processing_responses[child_idx].error != Coordination::Error::ZOK)
        {
            LOG_INFO(storage.log, "Skipping {}: missing processing parts", child);
            continue;
        }
        if (exceptions_per_replica_responses[child_idx].error != Coordination::Error::ZOK)
        {
            LOG_INFO(storage.log, "Skipping {}: missing exceptions_per_replica", export_partition_path);
            continue;
        }
        const auto exceptions_per_replica_path = export_partition_path / "exceptions_per_replica";
        const auto & exception_replicas = exceptions_per_replica_responses[child_idx].names;
        for (const auto & replica : exception_replicas)
        {
            const auto last_exception_path = exceptions_per_replica_path / replica / "last_exception";
            exception_replica_paths.push_back({
                child_idx,
                replica,
                (exceptions_per_replica_path / replica / "count").string(),
                (last_exception_path / "exception").string(),
                (last_exception_path / "part").string()
            });
        }
    }
    /// Batch get all exception data in a single multi request
    std::map<size_t, std::vector<std::tuple<std::string, std::string, std::string, std::string>>> exception_data_by_child;

    if (!exception_replica_paths.empty())
    {
        Coordination::Requests exception_requests;
        exception_requests.reserve(exception_replica_paths.size() * 3); // count, exception, part for each
        
        // Track response indices for each exception replica path
        struct ExceptionResponseIndices
        {
            size_t count_idx;
            size_t exception_idx;
            size_t part_idx;
        };
        std::vector<ExceptionResponseIndices> exception_response_indices;
        exception_response_indices.reserve(exception_replica_paths.size());
        
        for (const auto & erp : exception_replica_paths)
        {
            ExceptionResponseIndices indices;
            indices.count_idx = exception_requests.size();
            exception_requests.push_back(zkutil::makeGetRequest(erp.count_path));
            
            indices.exception_idx = exception_requests.size();
            exception_requests.push_back(zkutil::makeGetRequest(erp.exception_path));
            
            indices.part_idx = exception_requests.size();
            exception_requests.push_back(zkutil::makeGetRequest(erp.part_path));
            
            exception_response_indices.push_back(indices);
        }

        // Execute single multi request for all exception data
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
        
        Coordination::Responses exception_responses;
        Coordination::Error exception_code = zk->tryMulti(exception_requests, exception_responses);

        if (exception_code != Coordination::Error::ZOK)
        {
            LOG_INFO(storage.log, "Failed to execute multi request for exception data, error: {}", exception_code);
        }
        else
        {
            // Parse exception responses
            for (size_t exception_path_idx = 0; exception_path_idx < exception_replica_paths.size(); ++exception_path_idx)
            {
                const auto & erp = exception_replica_paths[exception_path_idx];
                const auto & indices = exception_response_indices[exception_path_idx];

                std::string count_str;
                std::string exception_str;
                std::string part_str;

                // Extract count response
                if (indices.count_idx < exception_responses.size())
                {
                    const auto * count_response = dynamic_cast<const Coordination::GetResponse *>(exception_responses[indices.count_idx].get());
                    if (count_response && count_response->error == Coordination::Error::ZOK)
                        count_str = count_response->data;
                }

                // Extract exception response
                if (indices.exception_idx < exception_responses.size())
                {
                    const auto * exception_response = dynamic_cast<const Coordination::GetResponse *>(exception_responses[indices.exception_idx].get());
                    if (exception_response && exception_response->error == Coordination::Error::ZOK)
                        exception_str = exception_response->data;
                }

                // Extract part response
                if (indices.part_idx < exception_responses.size())
                {
                    const auto * part_response = dynamic_cast<const Coordination::GetResponse *>(exception_responses[indices.part_idx].get());
                    if (part_response && part_response->error == Coordination::Error::ZOK)
                        part_str = part_response->data;
                }

                exception_data_by_child[erp.child_idx].emplace_back(erp.replica, count_str, exception_str, part_str);
            }
        }
    }

    /// Build the result
    for (size_t child_idx = 0; child_idx < children.size(); ++child_idx)
    {
        /// Skip if we already determined this child is invalid
        if (metadata_responses[child_idx].error != Coordination::Error::ZOK
            || status_responses[child_idx].error != Coordination::Error::ZOK
            || processing_responses[child_idx].error != Coordination::Error::ZOK
            || exceptions_per_replica_responses[child_idx].error != Coordination::Error::ZOK)
        {
            continue;
        }

        ReplicatedPartitionExportInfo info;
        const auto metadata_json = metadata_responses[child_idx].data;
        const auto status = status_responses[child_idx].data;
        const auto processing_parts = processing_responses[child_idx].names;
        const auto parts_to_do = processing_parts.size();
        std::string exception_replica;
        std::string last_exception;
        std::string exception_part;
        std::size_t exception_count = 0;
        /// Process exception data
        auto exception_data_it = exception_data_by_child.find(child_idx);
        if (exception_data_it != exception_data_by_child.end())
        {
            for (const auto & [replica, count_str, exception_str, part_str] : exception_data_it->second)
            {
                if (!count_str.empty())
                {
                    exception_count += parse<size_t>(count_str);
                }
                if (last_exception.empty() && !exception_str.empty() && !part_str.empty())
                {
                    exception_replica = replica;
                    last_exception = exception_str;
                    exception_part = part_str;
                }
            }
        }

        const auto metadata = ExportReplicatedMergeTreePartitionManifest::fromJsonString(metadata_json);

        info.destination_database = metadata.destination_database;
        info.destination_table = metadata.destination_table;
        info.partition_id = metadata.partition_id;
        info.transaction_id = metadata.transaction_id;
        info.query_id = metadata.query_id;
        info.create_time = metadata.create_time;
        info.source_replica = metadata.source_replica;
        info.parts_count = metadata.number_of_parts;
        info.parts_to_do = parts_to_do;
        info.parts = metadata.parts;
        info.status = status;
        info.exception_replica = exception_replica;
        info.last_exception = last_exception;
        info.exception_part = exception_part;
        info.exception_count = exception_count;
        infos.emplace_back(std::move(info));
    }

    return infos;
}

std::vector<ReplicatedPartitionExportInfo> ExportPartitionManifestUpdatingTask::getPartitionExportsInfoLocal() const
{
    std::lock_guard lock(storage.export_merge_tree_partition_mutex);

    std::vector<ReplicatedPartitionExportInfo> infos;

    for (const auto & entry : storage.export_merge_tree_partition_task_entries_by_key)
    {
        ReplicatedPartitionExportInfo info;

        info.destination_database = entry.manifest.destination_database;
        info.destination_table = entry.manifest.destination_table;
        info.partition_id = entry.manifest.partition_id;
        info.transaction_id = entry.manifest.transaction_id;
        info.query_id = entry.manifest.query_id;
        info.create_time = entry.manifest.create_time;
        info.source_replica = entry.manifest.source_replica;
        info.parts_count = entry.manifest.number_of_parts;
        info.parts_to_do = entry.manifest.parts.size();
        info.parts = entry.manifest.parts;
        info.status = magic_enum::enum_name(entry.status);

        infos.emplace_back(std::move(info));
    }

    return infos;
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
                key,
                metadata,
                now,
                *status == ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING,
                entries_by_key);

            if (cleanup_successful)
                continue;
        }

        if (has_local_entry_and_is_up_to_date)
        {
            LOG_INFO(storage.log, "ExportPartition Manifest Updating Task: Skipping {}: already exists", key);
            continue;
        }

        addTask(metadata, *status, key, entries_by_key);
    }

    /// Remove entries that were deleted by someone else
    removeStaleEntries(zk_children, entries_by_key);

    LOG_INFO(storage.log, "ExportPartition Manifest Updating task: finished polling for new entries. Number of entries: {}", entries_by_key.size());

    storage.export_merge_tree_partition_select_task->schedule();
}

void ExportPartitionManifestUpdatingTask::addTask(
    const ExportReplicatedMergeTreePartitionManifest & metadata,
    ExportReplicatedMergeTreePartitionTaskEntry::Status status,
    const std::string & key,
    auto & entries_by_key
)
{
    std::vector<DataPartPtr> part_references;

    /// If the status is PENDING, we grab references to the data parts to prevent them from being deleted from the disk
    /// Otherwise, the operation has already been completed and there is no need to keep the data parts alive
    /// You might also ask: why bother adding tasks that have already been completed (i.e, status != PENDING)?
    /// The reason is the `replicated_partition_exports` table in the local only mode might miss entries if they are not added here.
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
    auto entry = ExportReplicatedMergeTreePartitionTaskEntry {metadata, status, std::move(part_references)};
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

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
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
    }
}

}
