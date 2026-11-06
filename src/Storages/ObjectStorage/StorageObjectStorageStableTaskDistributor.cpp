#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>
#include <Common/SipHash.h>
#include <base/scope_guard.h>

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
    , ids_of_nodes(std::move(ids_of_nodes_))
    , owner_queues(ids_of_nodes.size())
    , stealer_queues(ids_of_nodes.size())
    , remaining_bytes(ids_of_nodes.size(), 0)
    , remaining_count(ids_of_nodes.size(), 0)
    , replica_seen(ids_of_nodes.size(), false)
{
}

ObjectInfos StorageObjectStorageStableTaskDistributor::getNextTasks(size_t number_of_current_replica, size_t max_tasks)
{
    if (number_of_current_replica >= ids_of_nodes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Replica number {} is out of range. Expected range: [0, {})",
            number_of_current_replica,
            ids_of_nodes.size());

    if (max_tasks == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replica {} asked for zero tasks", number_of_current_replica);

    LOG_TRACE(log, "Received request from replica {} looking for up to {} tasks", number_of_current_replica, max_tasks);

    ObjectInfos result;
    {
        std::lock_guard lock(mutex);
        if (!replica_seen[number_of_current_replica])
        {
            replica_seen[number_of_current_replica] = true;
            replica_seen_cv.notify_all();
        }
        takeOwnTasks(number_of_current_replica, max_tasks, result);
    }

    if (result.size() < max_tasks)
        takeTasksFromIterator(number_of_current_replica, max_tasks, result);

    if (result.empty())
        stealTasks(number_of_current_replica, max_tasks, result);

    LOG_TRACE(log, "Assigning {} tasks to replica {}", result.size(), number_of_current_replica);
    return result;
}

StorageObjectStorageStableTaskDistributor::Placement StorageObjectStorageStableTaskDistributor::getPlacement(const String & identifier) const
{
    Placement placement;
    if (ids_of_nodes.size() < 2)
        return placement;

    UInt64 best_weight = 0;
    UInt64 second_weight = 0;
    for (size_t id = 0; id < ids_of_nodes.size(); ++id)
    {
        const UInt64 weight = sipHash64(ids_of_nodes[id] + identifier);
        if (id == 0 || weight > best_weight)
        {
            if (id != 0)
            {
                placement.stealer = placement.owner;
                second_weight = best_weight;
            }
            placement.owner = id;
            best_weight = weight;
        }
        else if (!placement.stealer || weight > second_weight)
        {
            placement.stealer = id;
            second_weight = weight;
        }
    }
    return placement;
}

void StorageObjectStorageStableTaskDistributor::takeOwnTasks(size_t replica, size_t max_tasks, ObjectInfos & result)
{
    auto & queue = owner_queues[replica];
    while (result.size() < max_tasks && !queue.empty())
    {
        auto task = std::move(queue.front());
        queue.pop_front();
        if (!task->taken)
            result.push_back(take(task));
    }
}

void StorageObjectStorageStableTaskDistributor::takeTasksFromIterator(size_t replica, size_t max_tasks, ObjectInfos & result)
{
    std::unique_lock lock(mutex);
    while (true)
    {
        takeOwnTasks(replica, max_tasks, result);
        if (result.size() >= max_tasks || iterator_exhausted)
            return;
        if (!iterator_in_use)
            break;
        if (!result.empty())
            return;
        iterator_cv.wait(lock);
    }

    iterator_in_use = true;
    SCOPE_EXIT({
        if (!lock.owns_lock())
            lock.lock();
        iterator_in_use = false;
        iterator_cv.notify_all();
    });

    while (result.size() < max_tasks)
    {
        lock.unlock();

        auto object = iterator->next(0);

        String identifier;
        Placement placement;
        if (object)
        {
            identifier = getSchedulingIdentifier(object, send_over_whole_archive);
            if (send_over_whole_archive && object->isArchive())
            {
                LOG_TEST(log, "Will send over the whole archive {} to replicas. "
                         "This will be suboptimal, consider turning on "
                         "cluster_function_process_archive_on_multiple_nodes setting", identifier);
                placement = getPlacement(identifier);
            }
            else
            {
                placement = getPlacement(object->getIdentifier(/*include_file_bucket_info=*/ false));
            }
        }

        lock.lock();
        if (!object)
        {
            LOG_TEST(log, "Iterator is exhausted");
            iterator_exhausted = true;
            return;
        }

        if (placement.owner == replica)
        {
            LOG_TRACE(log, "Found file {} for replica {}", identifier, replica);
            result.push_back(std::move(object));
        }
        else
        {
            LOG_TEST(log, "Found file {} for replica {} (number of current replica: {})", identifier, placement.owner, replica);
            enqueue(std::move(object), std::move(identifier), placement);
            iterator_cv.notify_all();
        }
    }
}

void StorageObjectStorageStableTaskDistributor::stealTasks(size_t replica, size_t max_tasks, ObjectInfos & result)
{
    std::unique_lock lock(mutex);

    if (!stealing_allowed_since)
        stealing_allowed_since = std::chrono::steady_clock::now() + wait_for_owner;

    std::unordered_map<size_t, size_t> allowed_to_steal;
    auto can_steal_from = [&](size_t owner, bool deadline_passed)
    {
        if (!deadline_passed && owner != replica && !replica_seen[owner])
            return false;
        auto [it, _] = allowed_to_steal.try_emplace(owner, std::max<size_t>(1, remaining_count[owner] / 2));
        return it->second > 0;
    };

    while (!unprocessed_files.empty())
    {
        takeOwnTasks(replica, max_tasks, result);

        const bool deadline_passed = std::chrono::steady_clock::now() >= *stealing_allowed_since;

        auto & stealer_queue = stealer_queues[replica];
        for (auto it = stealer_queue.end(); it != stealer_queue.begin() && result.size() < max_tasks;)
        {
            --it;
            const auto & task = *it;
            if (task->taken)
            {
                it = stealer_queue.erase(it);
                continue;
            }
            if (!can_steal_from(task->owner, deadline_passed))
                continue;

            --allowed_to_steal[task->owner];
            LOG_TRACE(log, "Iterator exhausted. Assigning file {} of replica {} to its stealer {}", task->identifier, task->owner, replica);
            result.push_back(take(task));
            it = stealer_queue.erase(it);
        }

        while (result.size() < max_tasks)
        {
            std::optional<size_t> owner;
            for (size_t candidate = 0; candidate < remaining_count.size(); ++candidate)
            {
                if (!remaining_count[candidate] || !can_steal_from(candidate, deadline_passed))
                    continue;
                if (!owner || remaining_bytes[candidate] > remaining_bytes[*owner])
                    owner = candidate;
            }

            if (!owner)
                break;

            auto & queue = owner_queues[*owner];
            while (!queue.empty() && queue.back()->taken)
                queue.pop_back();
            if (queue.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Replica {} has {} files left but no queued files", *owner, remaining_count[*owner]);

            auto task = std::move(queue.back());
            queue.pop_back();
            --allowed_to_steal[*owner];
            LOG_TRACE(log, "Iterator exhausted. Assigning file {} of replica {} to replica {}", task->identifier, *owner, replica);
            result.push_back(take(task));
        }

        if (!result.empty())
            return;

        replica_seen_cv.wait_until(lock, *stealing_allowed_since);
    }
}

void StorageObjectStorageStableTaskDistributor::enqueue(ObjectInfoPtr object, String identifier, const Placement & placement)
{
    auto task = std::make_shared<QueuedTask>();
    task->identifier = std::move(identifier);
    task->object = std::move(object);
    task->owner = placement.owner;
    task->weight = getTaskWeight(task->object);

    if (!unprocessed_files.emplace(task->identifier, task).second)
        return;

    remaining_bytes[task->owner] += task->weight;
    ++remaining_count[task->owner];
    owner_queues[task->owner].push_back(task);
    if (placement.stealer)
        stealer_queues[*placement.stealer].push_back(task);
}

ObjectInfoPtr StorageObjectStorageStableTaskDistributor::take(const QueuedTaskPtr & task)
{
    if (task->taken)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} is taken twice", task->identifier);

    task->taken = true;
    remaining_bytes[task->owner] -= task->weight;
    --remaining_count[task->owner];
    unprocessed_files.erase(task->identifier);
    return task->object;
}

}
