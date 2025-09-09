#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>
#include <Common/SipHash.h>
#include <consistent_hashing.h>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageObjectStorageStableTaskDistributor::StorageObjectStorageStableTaskDistributor(
    std::shared_ptr<IObjectIterator> iterator_,
    std::vector<std::string> ids_of_nodes_,
    uint64_t lock_object_storage_task_distribution_ms_)
    : iterator(std::move(iterator_))
    , connection_to_files(ids_of_nodes_.size())
    , ids_of_nodes(ids_of_nodes_)
    , lock_object_storage_task_distribution_us(lock_object_storage_task_distribution_ms_ * 1000)
    , iterator_exhausted(false)
{
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "Received request from replica {} looking for a file", number_of_current_replica);

    if (connection_to_files.size() <= number_of_current_replica)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Received request with invalid replica number {}, max possible replica number {}",
            number_of_current_replica,
            connection_to_files.size() - 1);

    saveLastNodeActivity(number_of_current_replica);

    // 1. Check pre-queued files first
    if (auto file = getPreQueuedFile(number_of_current_replica))
        return file;

    // 2. Try to find a matching file from the iterator
    if (auto file = getMatchingFileFromIterator(number_of_current_replica))
        return file;

    // 3. Process unprocessed files if iterator is exhausted
    return getAnyUnprocessedFile(number_of_current_replica);
}

size_t StorageObjectStorageStableTaskDistributor::getReplicaForFile(const String & file_path)
{
    size_t nodes_count = ids_of_nodes.size();

    /// Trivial case
    if (nodes_count < 2)
        return 0;

    /// Rendezvous hashing
    size_t best_id = 0;
    UInt64 best_weight = sipHash64(ids_of_nodes[0] + file_path);
    for (size_t id = 1; id < nodes_count; ++id)
    {
        UInt64 weight = sipHash64(ids_of_nodes[id] + file_path);
        if (weight > best_weight)
        {
            best_weight = weight;
            best_id = id;
        }
    }
    return best_id;
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getPreQueuedFile(size_t number_of_current_replica)
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
        String next_file = files.back();
        files.pop_back();

        auto it = unprocessed_files.find(next_file);
        if (it == unprocessed_files.end())
            continue;

        unprocessed_files.erase(it);

        LOG_TRACE(
            log,
            "Assigning pre-queued file {} to replica {}",
            next_file,
            number_of_current_replica
        );

        return next_file;
    }

    return std::nullopt;
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getMatchingFileFromIterator(size_t number_of_current_replica)
{
    {
        std::lock_guard lock(mutex);
        if (iterator_exhausted)
            return std::nullopt;
    }

    while (true)
    {
        ObjectInfoPtr object_info;

        {
            std::lock_guard lock(mutex);
            object_info = iterator->next(0);

            if (!object_info)
            {
                iterator_exhausted = true;
                break;
            }
        }

        String file_path;

        auto archive_object_info = std::dynamic_pointer_cast<StorageObjectStorageSource::ArchiveIterator::ObjectInfoInArchive>(object_info);
        if (archive_object_info)
        {
            file_path = archive_object_info->getPathToArchive();
        }
        else
        {
            file_path = object_info->getPath();
        }

        size_t file_replica_idx = getReplicaForFile(file_path);
        if (file_replica_idx == number_of_current_replica)
        {
            LOG_TRACE(
                log,
                "Found file {} for replica {}",
                file_path,
                number_of_current_replica
            );

            return file_path;
        }

        // Queue file for its assigned replica
        {
            std::lock_guard lock(mutex);
            unprocessed_files[file_path] = number_of_current_replica;
            connection_to_files[file_replica_idx].push_back(file_path);
        }
    }

    return std::nullopt;
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(size_t number_of_current_replica)
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
            auto last_activity = last_node_activity.find(it->second);
            if (lock_object_storage_task_distribution_us <= 0
                || last_activity == last_node_activity.end()
                || activity_limit > last_activity->second)
            {
                String next_file = it->first;
                unprocessed_files.erase(it);

                LOG_TRACE(
                    log,
                    "Iterator exhausted. Assigning unprocessed file {} to replica {}",
                    next_file,
                    number_of_current_replica
                );

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
        response.set_retry_after_us(oldest_activity - activity_limit);
        return response.to_string();
    }

    return std::nullopt;
}

void StorageObjectStorageStableTaskDistributor::saveLastNodeActivity(size_t number_of_current_replica)
{
    Poco::Timestamp now;
    std::lock_guard lock(mutex);
    last_node_activity[number_of_current_replica] = now;
}

}
