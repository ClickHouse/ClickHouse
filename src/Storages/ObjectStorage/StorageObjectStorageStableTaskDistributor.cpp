#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>
#include <Common/SipHash.h>
#include <consistent_hashing.h>
#include <optional>

namespace ProfileEvents
{
    extern const Event ObjectStorageClusterSentToMatchedReplica;
    extern const Event ObjectStorageClusterSentToNonMatchedReplica;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
};

StorageObjectStorageStableTaskDistributor::StorageObjectStorageStableTaskDistributor(
    std::shared_ptr<IObjectIterator> iterator_,
    std::vector<std::string> && ids_of_nodes_,
    bool send_over_whole_archive_,
    uint64_t lock_object_storage_task_distribution_ms_,
    bool iceberg_read_optimization_enabled_)
    : iterator(std::move(iterator_))
    , send_over_whole_archive(send_over_whole_archive_)
    , connection_to_files(ids_of_nodes_.size())
    , ids_of_nodes(std::move(ids_of_nodes_))
    , lock_object_storage_task_distribution_us(lock_object_storage_task_distribution_ms_ * 1000)
    , iterator_exhausted(false)
    , iceberg_read_optimization_enabled(iceberg_read_optimization_enabled_)
{
    Poco::Timestamp now;
    size_t nodes = ids_of_nodes.size();
    for (size_t i = 0; i < nodes; ++i)
    {
        replica_to_files_to_be_processed[i] = std::list<ObjectInfoPtr>{};
        last_node_activity[i] = now;
    }
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "Received request from replica {} looking for a file", number_of_current_replica);

    saveLastNodeActivity(number_of_current_replica);

    {
        std::lock_guard lock(mutex);
        auto processed_file_list_ptr = replica_to_files_to_be_processed.find(number_of_current_replica);
        if (processed_file_list_ptr == replica_to_files_to_be_processed.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Replica number {} was marked as lost, can't set task for it anymore",
                number_of_current_replica
            );
    }

    // 1. Check pre-queued files first
    auto file = getPreQueuedFile(number_of_current_replica);
    // 2. Try to find a matching file from the iterator
    if (!file)
        file = getMatchingFileFromIterator(number_of_current_replica);
    // 3. Process unprocessed files if iterator is exhausted
    if (!file)
        file = getAnyUnprocessedFile(number_of_current_replica);

    if (file)
    {
        std::lock_guard lock(mutex);
        auto processed_file_list_ptr = replica_to_files_to_be_processed.find(number_of_current_replica);
        if (processed_file_list_ptr == replica_to_files_to_be_processed.end())
        { // It is possible that replica was lost after check in the begining of the method
            auto file_identifier = getFileIdentifier(file);
            auto file_replica_idx = getReplicaForFile(file_identifier);
            unprocessed_files.emplace(file_identifier, std::make_pair(file, file_replica_idx));
            connection_to_files[file_replica_idx].push_back(file);
        }
        else
            processed_file_list_ptr->second.push_back(file);
    }

    return file;
}

size_t StorageObjectStorageStableTaskDistributor::getReplicaForFile(const String & file_path)
{
    size_t nodes_count = ids_of_nodes.size();

    /// Trivial case
    if (nodes_count < 2)
        return 0;

    /// Rendezvous hashing
    auto replica = replica_to_files_to_be_processed.begin();
    if (replica == replica_to_files_to_be_processed.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "No active replicas, can't find best replica for file {}",
            file_path
        );

    size_t best_id = replica->first;
    UInt64 best_weight = sipHash64(ids_of_nodes[best_id] + file_path);
    ++replica;
    while (replica != replica_to_files_to_be_processed.end())
    {
        size_t id = replica->first;
        UInt64 weight = sipHash64(ids_of_nodes[id] + file_path);
        if (weight > best_weight)
        {
            best_weight = weight;
            best_id = id;
        }
        ++replica;
    }
    return best_id;
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getPreQueuedFile(size_t number_of_current_replica)
{
    std::lock_guard lock(mutex);

    if (connection_to_files.size() <= number_of_current_replica)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Replica number {} is out of range. Expected range: [0, {})",
            number_of_current_replica,
            connection_to_files.size()
        );

    auto & files = connection_to_files[number_of_current_replica];

    while (!files.empty())
    {
        auto next_file = files.back();
        files.pop_back();

        auto file_identifier = getFileIdentifier(next_file);
        auto it = unprocessed_files.find(file_identifier);
        if (it == unprocessed_files.end())
            continue;

        unprocessed_files.erase(it);

        LOG_TRACE(
            log,
            "Assigning pre-queued file {} to replica {}",
            file_identifier,
            number_of_current_replica
        );

        ProfileEvents::increment(ProfileEvents::ObjectStorageClusterSentToMatchedReplica);
        return next_file;
    }

    return {};
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getMatchingFileFromIterator(size_t number_of_current_replica)
{
    {
        std::lock_guard lock(mutex);
        if (iterator_exhausted)
            return {};
    }

    while (true)
    {
        ObjectInfoPtr object_info;

        {
            std::lock_guard lock(mutex);
            object_info = iterator->next(0);

            if (!object_info)
            {
                LOG_TEST(log, "Iterator is exhausted");
                iterator_exhausted = true;
                break;
            }
        }

        String file_identifier = getFileIdentifier(object_info, true);

        if (iceberg_read_optimization_enabled)
        {
            auto file_meta_info = object_info->relative_path_with_metadata.getFileMetaInfo();
            if (file_meta_info.has_value())
            {
                auto file_path = send_over_whole_archive ? object_info->getPathOrPathToArchiveIfArchive() : object_info->getPath();
                object_info->relative_path_with_metadata.command.setFilePath(file_path);
                object_info->relative_path_with_metadata.command.setFileMetaInfo(file_meta_info.value());
            }
        }

        size_t file_replica_idx;

        {
            std::lock_guard lock(mutex);
            file_replica_idx = getReplicaForFile(file_identifier);
        }

        if (file_replica_idx == number_of_current_replica)
        {
            LOG_TRACE(
                log, "Found file {} for replica {}",
                file_identifier, number_of_current_replica
            );

            ProfileEvents::increment(ProfileEvents::ObjectStorageClusterSentToMatchedReplica);
            return object_info;
        }
        LOG_TEST(
            log,
            "Found file {} for replica {} (number of current replica: {})",
            file_identifier,
            file_replica_idx,
            number_of_current_replica
        );

        // Queue file for its assigned replica
        {
            std::lock_guard lock(mutex);
            unprocessed_files.emplace(file_identifier, std::make_pair(object_info, file_replica_idx));
            connection_to_files[file_replica_idx].push_back(object_info);
        }
    }

    return {};
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(size_t number_of_current_replica)
{
    /// Limit time of node activity to keep task in queue
    Poco::Timestamp activity_limit;
    Poco::Timestamp oldest_activity;
    if (lock_object_storage_task_distribution_us > 0)
        activity_limit -= lock_object_storage_task_distribution_us;

    std::lock_guard lock(mutex);

    if (!unprocessed_files.empty())
    {
        auto it = unprocessed_files.begin();

        while (it != unprocessed_files.end())
        {
            auto number_of_matched_replica = it->second.second;
            auto last_activity = last_node_activity.find(number_of_matched_replica);
            if (lock_object_storage_task_distribution_us <= 0 // file deferring is turned off
                || it->second.second == number_of_current_replica // file is matching with current replica
                || last_activity == last_node_activity.end() // msut never be happen, last_activity is filled for each replica on start
                || activity_limit > last_activity->second) // matched replica did not ask for a new files for a while
            {
                auto next_file = it->second.first;
                unprocessed_files.erase(it);

                auto file_path = getFileIdentifier(next_file);
                LOG_TRACE(
                    log,
                    "Iterator exhausted. Assigning unprocessed file {} to replica {} from matched replica {}",
                    file_path,
                    number_of_current_replica,
                    number_of_matched_replica
                );

                ProfileEvents::increment(ProfileEvents::ObjectStorageClusterSentToNonMatchedReplica);
                return next_file;
            }

            oldest_activity = std::min(oldest_activity, last_activity->second);
            ++it;
        }

        LOG_TRACE(
            log,
            "No unprocessed file for replica {}, need to retry after {} us",
            number_of_current_replica,
            oldest_activity - activity_limit
        );

        /// All unprocessed files owned by alive replicas with recenlty activity
        /// Need to retry after (oldest_activity - activity_limit) microseconds
        RelativePathWithMetadata::CommandInTaskResponse response;
        response.setRetryAfterUs(oldest_activity - activity_limit);
        return std::make_shared<ObjectInfo>(response.toString());
    }

    return {};
}

void StorageObjectStorageStableTaskDistributor::saveLastNodeActivity(size_t number_of_current_replica)
{
    Poco::Timestamp now;
    std::lock_guard lock(mutex);
    last_node_activity[number_of_current_replica] = now;
}

void StorageObjectStorageStableTaskDistributor::rescheduleTasksFromReplica(size_t number_of_current_replica)
{
    LOG_INFO(log, "Replica {} is marked as lost, tasks are returned to queue", number_of_current_replica);
    std::lock_guard lock(mutex);

    auto processed_file_list_ptr = replica_to_files_to_be_processed.find(number_of_current_replica);
    if (processed_file_list_ptr == replica_to_files_to_be_processed.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Replica number {} was marked as lost already",
            number_of_current_replica
        );

    if (replica_to_files_to_be_processed.size() < 2)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "All replicas were marked as lost"
        );

    auto files = std::move(processed_file_list_ptr->second);
    replica_to_files_to_be_processed.erase(number_of_current_replica);
    for (const auto & file : files)
    {
        auto file_identifier = getFileIdentifier(file);
        auto file_replica_idx = getReplicaForFile(file_identifier);
        unprocessed_files.emplace(file_identifier, std::make_pair(file, file_replica_idx));
        connection_to_files[file_replica_idx].push_back(file);
    }
}

String StorageObjectStorageStableTaskDistributor::getFileIdentifier(ObjectInfoPtr file_object, bool write_to_log) const
{
    if (send_over_whole_archive && file_object->isArchive())
    {
        auto file_identifier = file_object->getPathOrPathToArchiveIfArchive();
        if (write_to_log)
        {
            LOG_TEST(log, "Will send over the whole archive {} to replicas. "
                        "This will be suboptimal, consider turning on "
                        "cluster_function_process_archive_on_multiple_nodes setting", file_identifier);
        }
        return file_identifier;
    }
    return file_object->getIdentifier();
}

}
