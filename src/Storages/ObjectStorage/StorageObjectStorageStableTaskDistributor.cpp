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

size_t getTaskWeight(const ObjectInfoPtr & object_info)
{
    if (auto size = object_info->getFileSizeHint())
        return std::max<size_t>(1, *size);
    if (auto metadata = object_info->getObjectMetadata(); metadata && metadata->is_size_known && metadata->size_bytes)
        return metadata->size_bytes;
    return 1;
}

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
    , remaining_bytes(ids_of_nodes_.size(), 0)
    , ids_of_nodes(std::move(ids_of_nodes_))
    , replica_seen(ids_of_nodes.size(), false)
    , replica_finished(ids_of_nodes.size(), false)
    , iterator_exhausted(false)
{
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "Received request from replica {} looking for a file", number_of_current_replica);

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

    if (!replica_seen[number_of_current_replica])
    {
        replica_seen[number_of_current_replica] = true;
        replica_seen_cv.notify_all();
    }

    auto next_file = takeQueuedFile(number_of_current_replica);
    if (next_file)
        LOG_TRACE(
            log,
            "Assigning pre-queued file {} to replica {}",
            getSchedulingIdentifier(next_file, send_over_whole_archive),
            number_of_current_replica
        );

    return next_file;
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::takeQueuedFile(size_t replica)
{
    auto & files = connection_to_files[replica];

    while (!files.empty())
    {
        auto next_file = files.back();
        files.pop_back();

        auto it = unprocessed_files.find(getSchedulingIdentifier(next_file, send_over_whole_archive));
        if (it == unprocessed_files.end())
            continue;

        remaining_bytes[replica] -= getTaskWeight(it->second);
        unprocessed_files.erase(it);
        return next_file;
    }

    return {};
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getMatchingFileFromIterator(size_t number_of_current_replica)
{
    /// Keep task extraction, assignment, and queuing in one critical section.
    /// Otherwise another requester can observe iterator EOF while a task is
    /// between `iterator->next` and insertion into `unprocessed_files`, return
    /// EOF to a worker, and permanently reduce the set of workers able to drain it.
    std::lock_guard lock(mutex);
    if (iterator_exhausted)
        return {};

    while (true)
    {
        auto object_info = iterator->next(0);
        if (!object_info)
        {
            LOG_TEST(log, "Iterator is exhausted");
            iterator_exhausted = true;
            break;
        }

        String file_identifier;
        String placement_identifier;
        if (send_over_whole_archive && object_info->isArchive())
        {
            file_identifier = getSchedulingIdentifier(object_info, send_over_whole_archive);
            placement_identifier = file_identifier;
            LOG_TEST(log, "Will send over the whole archive {} to replicas. "
                     "This will be suboptimal, consider turning on "
                     "cluster_function_process_archive_on_multiple_nodes setting", file_identifier);
        }
        else
        {
            file_identifier = object_info->getIdentifier();
            placement_identifier = object_info->getIdentifier(/*include_file_bucket_info=*/ false);
        }

        size_t file_replica_idx = getReplicaForFile(placement_identifier);
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

        // Queue file for its assigned replica.
        if (unprocessed_files.emplace(file_identifier, object_info).second)
            remaining_bytes[file_replica_idx] += getTaskWeight(object_info);
        connection_to_files[file_replica_idx].push_back(object_info);
    }

    return {};
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::getAnyUnprocessedFile(size_t number_of_current_replica)
{
    std::unique_lock lock(mutex);

    if (!stealing_allowed_since)
    {
        /// Saturates instead of overflowing: the deadline is never earlier than requested.
        const auto now = std::chrono::steady_clock::now();
        if (wait_for_owner < std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::time_point::max() - now))
            stealing_allowed_since = now + wait_for_owner;
        else
            stealing_allowed_since = std::chrono::steady_clock::time_point::max();
    }

    /// Every file is assigned to a replica by rendezvous hashing, so that the same file is read by the same replica
    /// in every query, and is read from its cache. Taking a file of another replica breaks this, so it is done only
    /// when needed. A replica that has asked for tasks keeps asking until it receives an empty task, so its files
    /// will be processed anyway. If they were given to any replica that asks, they would go to random replicas,
    /// depending on the timing of the requests, and the caches would be different in every query. That's why the
    /// files of such a replica are taken only to balance the work, when it has much more of it left than the others.
    /// If such a replica fails in the middle of the query, the result is incomplete anyway, because the files
    /// that it was reading are lost.
    ///
    /// Note that the requests are served synchronously by the threads of the initiator, so waiting here for
    /// a replica that has already asked for tasks could block the threads that serve the requests of this replica.
    ///
    /// The loop exits in one of two ways:
    /// - returns a file: of the current replica if it has any; otherwise of the replica with the most remaining bytes
    ///   among the replicas whose files can be taken:
    ///   - replicas that have received an empty task and will not ask again (their files could have been queued
    ///     by other threads that took them from the iterator before it was exhausted);
    ///   - replicas that haven't asked for tasks yet, once `wait_for_owner` has passed;
    ///   - replicas that have asked for tasks and have more than twice the average remaining bytes of the other replicas;
    /// - returns nothing when no unprocessed files are left, or when the files that are left will be processed
    ///   by their replicas.
    /// Otherwise, when there are files of replicas that haven't asked for tasks yet, it waits until a new replica
    /// asks for tasks or until `wait_for_owner` passes, and then checks again.
    while (!unprocessed_files.empty())
    {
        if (auto next_file = takeQueuedFile(number_of_current_replica))
        {
            LOG_TRACE(
                log,
                "Iterator exhausted. Assigning unprocessed file {} to its replica {}",
                getSchedulingIdentifier(next_file, send_over_whole_archive),
                number_of_current_replica);

            return next_file;
        }

        const bool deadline_passed = std::chrono::steady_clock::now() >= *stealing_allowed_since;

        UInt128 total_remaining_bytes = 0;
        for (size_t bytes : remaining_bytes)
            total_remaining_bytes += bytes;

        std::optional<size_t> owner;
        bool should_wait = false;
        for (size_t replica = 0; replica < remaining_bytes.size(); ++replica)
        {
            if (!remaining_bytes[replica])
                continue;

            bool can_take = false;
            if (replica_finished[replica])
            {
                can_take = true;
            }
            else if (!replica_seen[replica])
            {
                can_take = deadline_passed;
                should_wait = should_wait || !deadline_passed;
            }
            else
            {
                /// More than twice the average of the other replicas. There are at least two replicas here,
                /// because the current replica doesn't have queued files.
                UInt128 bytes = remaining_bytes[replica];
                can_take = bytes * (remaining_bytes.size() - 1) > (total_remaining_bytes - bytes) * 2;
            }

            if (can_take && (!owner || remaining_bytes[replica] > remaining_bytes[*owner]))
                owner = replica;
        }

        if (owner)
        {
            auto next_file = takeQueuedFile(*owner);
            if (!next_file)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Replica {} has {} bytes left but no queued files", *owner, remaining_bytes[*owner]);

            LOG_TRACE(
                log,
                "Iterator exhausted. Assigning unprocessed file {} of replica {} to replica {}",
                getSchedulingIdentifier(next_file, send_over_whole_archive),
                *owner,
                number_of_current_replica);

            return next_file;
        }

        if (!should_wait)
            break;

        replica_seen_cv.wait_until(lock, *stealing_allowed_since);
    }

    LOG_TRACE(log, "No more tasks for replica {}", number_of_current_replica);
    replica_finished[number_of_current_replica] = true;
    return {};
}

}
