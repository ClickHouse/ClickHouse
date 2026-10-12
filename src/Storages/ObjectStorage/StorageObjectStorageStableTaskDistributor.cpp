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

namespace
{

String getSchedulingIdentifier(const ObjectInfoPtr & object_info, bool send_over_whole_archive)
{
    if (send_over_whole_archive && object_info->isArchive())
        return object_info->getIdentifierForPath(object_info->getPathToArchive());

    return object_info->getIdentifier();
}

}

StorageObjectStorageStableTaskDistributor::StorageObjectStorageStableTaskDistributor(
    std::shared_ptr<IObjectIterator> iterator_,
    std::vector<std::string> && ids_of_nodes_,
    bool send_over_whole_archive_,
    std::chrono::milliseconds wait_for_owner_)
    : iterator(std::move(iterator_))
    , send_over_whole_archive(send_over_whole_archive_)
    , wait_for_owner(wait_for_owner_)
    , connection_to_files(ids_of_nodes_.size())
    , ids_of_nodes(std::move(ids_of_nodes_))
    , replica_seen(ids_of_nodes.size(), false)
    , iterator_exhausted(false)
{
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "Received request from replica {} looking for a file", number_of_current_replica);

    {
        std::lock_guard lock(mutex);
        if (number_of_current_replica < replica_seen.size() && !replica_seen[number_of_current_replica])
        {
            replica_seen[number_of_current_replica] = true;
            replica_seen_cv.notify_all();
        }
    }

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

        auto file_identifier = getSchedulingIdentifier(next_file, send_over_whole_archive);
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

        String file_identifier;
        if (send_over_whole_archive && object_info->isArchive())
        {
            file_identifier = getSchedulingIdentifier(object_info, send_over_whole_archive);
            LOG_TEST(log, "Will send over the whole archive {} to replicas. "
                     "This will be suboptimal, consider turning on "
                     "cluster_function_process_archive_on_multiple_nodes setting", file_identifier);
        }
        else
        {
            file_identifier = object_info->getIdentifier();
        }

        size_t file_replica_idx = getReplicaForFile(file_identifier);
        if (file_replica_idx == number_of_current_replica)
        {
            LOG_TRACE(
                log, "Found file {} for replica {}",
                file_identifier, number_of_current_replica
            );

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
    std::unique_lock lock(mutex);

    if (!stealing_allowed_since)
        stealing_allowed_since = std::chrono::steady_clock::now() + wait_for_owner;

    while (!unprocessed_files.empty())
    {
        const bool deadline_passed = std::chrono::steady_clock::now() >= *stealing_allowed_since;

        std::vector<size_t> remaining_per_replica(ids_of_nodes.size(), 0);
        for (const auto & [_, file_and_owner] : unprocessed_files)
            ++remaining_per_replica[file_and_owner.second];

        auto best = unprocessed_files.end();
        for (auto it = unprocessed_files.begin(); it != unprocessed_files.end(); ++it)
        {
            const size_t owner = it->second.second;
            if (!deadline_passed && owner != number_of_current_replica && !replica_seen[owner])
                continue;
            if (best == unprocessed_files.end() || remaining_per_replica[owner] > remaining_per_replica[best->second.second])
                best = it;
        }

        if (best != unprocessed_files.end())
        {
            auto next_file = best->second.first;
            const size_t owner = best->second.second;
            unprocessed_files.erase(best);

            LOG_TRACE(
                log,
                "Iterator exhausted. Assigning unprocessed file {} of replica {} to replica {}",
                getSchedulingIdentifier(next_file, send_over_whole_archive),
                owner,
                number_of_current_replica);

            return next_file;
        }

        replica_seen_cv.wait_until(lock, *stealing_allowed_since);
    }

    return {};
}

}
