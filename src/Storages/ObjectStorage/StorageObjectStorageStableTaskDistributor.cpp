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
    size_t number_of_replicas_,
    bool send_over_whole_archive_)
    : iterator(std::move(iterator_))
    , send_over_whole_archive(send_over_whole_archive_)
    , connection_to_files(number_of_replicas_)
    , iterator_exhausted(false)
{
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "Received request from replica {} looking for a file", number_of_current_replica);

    if (connection_to_files.size() <= number_of_current_replica)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Received request with invalid replica number {}, max possible replica number {}",
            number_of_current_replica,
            connection_to_files.size() - 1);

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
    return ConsistentHashing(sipHash64(file_path), connection_to_files.size());
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getPreQueuedFile(size_t number_of_current_replica)
{
    std::lock_guard lock(mutex);

    auto & files = connection_to_files[number_of_current_replica];

    while (!files.empty())
    {
        auto next_file = files.back();
        files.pop_back();

        auto file_path = send_over_whole_archive ? next_file->getPathOrPathToArchiveIfArchive() : next_file->getPath();
        auto it = unprocessed_files.find(file_path);
        if (it == unprocessed_files.end())
            continue;

        unprocessed_files.erase(it);

        LOG_TRACE(
            log,
            "Assigning pre-queued file {} to replica {}",
            file_path,
            number_of_current_replica
        );

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

        String file_path;
        if (send_over_whole_archive && object_info->isArchive())
        {
            file_path = object_info->getPathOrPathToArchiveIfArchive();
            LOG_TEST(log, "Will send over the whole archive {} to replicas. "
                     "This will be suboptimal, consider turning on "
                     "cluster_function_process_archive_on_multiple_nodes setting", file_path);
        }
        else
        {
            file_path = object_info->getPath();
        }

        size_t file_replica_idx = getReplicaForFile(file_path);
        if (file_replica_idx == number_of_current_replica)
        {
            LOG_TRACE(
                log, "Found file {} for replica {}",
                file_path, number_of_current_replica
            );

            return object_info;
        }
        LOG_TEST(
            log,
            "Found file {} for replica {} (number of current replica: {})",
            file_path,
            file_replica_idx,
            number_of_current_replica
        );

        // Queue file for its assigned replica
        {
            std::lock_guard lock(mutex);
            unprocessed_files.emplace(file_path, object_info);
            connection_to_files[file_replica_idx].push_back(object_info);
        }
    }

    return {};
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(size_t number_of_current_replica)
{
    std::lock_guard lock(mutex);

    if (!unprocessed_files.empty())
    {
        auto it = unprocessed_files.begin();
        auto next_file = it->second;
        unprocessed_files.erase(it);

        auto file_path = send_over_whole_archive ? next_file->getPathOrPathToArchiveIfArchive() : next_file->getPath();
        LOG_TRACE(
            log,
            "Iterator exhausted. Assigning unprocessed file {} to replica {}",
            file_path,
            number_of_current_replica
        );

        return next_file;
    }

    return {};
}

}
