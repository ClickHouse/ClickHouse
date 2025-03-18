#include "StorageObjectStorageStableTaskDistributor.h"
#include <Common/SipHash.h>
#include <consistent_hashing.h>
#include <optional>

namespace DB
{

StorageObjectStorageStableTaskDistributor::StorageObjectStorageStableTaskDistributor(
    std::shared_ptr<IObjectIterator> iterator_)
    : iterator(std::move(iterator_))
    , iterator_exhausted(false)
    , log(getLogger("StorageObjectStorageStableTaskDistributor"))
{
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica, size_t number_of_replicas)
{
    LOG_TRACE(
        log,
        "Received a new connection from replica {} looking for a file",
        number_of_current_replica
    );

    // 1. Check pre-queued files first
    if (auto file = getPreQueuedFile(number_of_current_replica))
        return file;

    // 2. Try to find a matching file from the iterator
    if (auto file = getMatchingFileFromIterator(number_of_current_replica, number_of_replicas))
        return file;

    // 3. Process unprocessed files if iterator is exhausted
    return getAnyUnprocessedFile(number_of_current_replica);
}

size_t StorageObjectStorageStableTaskDistributor::getReplicaForFile(const String & file_path, size_t number_of_replicas)
{
    if (number_of_replicas == 0)
        return 0;

    return ConsistentHashing(sipHash64(file_path), number_of_replicas);
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getPreQueuedFile(size_t number_of_current_replica)
{
    std::lock_guard lock(mutex);

    auto & files = connection_to_files[number_of_current_replica];

    while (!files.empty())
    {
        String next_file = files.back();
        files.pop_back();

        if (!unprocessed_files.contains(next_file))
            continue;

        unprocessed_files.erase(next_file);

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

std::optional<String> StorageObjectStorageStableTaskDistributor::getMatchingFileFromIterator(size_t number_of_current_replica, size_t number_of_replicas)
{
    while (!iterator_exhausted)
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

        size_t file_replica_idx = getReplicaForFile(file_path, number_of_replicas);
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
            unprocessed_files.insert(file_path);
            connection_to_files[file_replica_idx].push_back(file_path);
        }
    }

    return std::nullopt;
}

std::optional<String> StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(size_t number_of_current_replica)
{
    std::lock_guard lock(mutex);

    if (!unprocessed_files.empty())
    {
        auto it = unprocessed_files.begin();
        String next_file = *it;
        unprocessed_files.erase(it);

        LOG_TRACE(
            log,
            "Iterator exhausted. Assigning unprocessed file {} to replica {}",
            next_file,
            number_of_current_replica
        );

        return next_file;
    }

    return std::nullopt;
}

}
