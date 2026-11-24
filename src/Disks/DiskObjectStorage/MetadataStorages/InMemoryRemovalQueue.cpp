#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>

namespace DB
{

void InMemoryRemovalQueue::submitForRemoval(const StoredObjects & blobs)
{
    for (const auto & blob : blobs)
    {
        auto [it, inserted] = index.try_emplace(blob, next_slot);
        if (!inserted)
            continue;

        /// Keep `queue` and `index` in sync if the second insertion throws: a blob left only in one of them
        /// would either never be handed out or never be forgotten.
        try
        {
            queue.emplace(next_slot, blob);
        }
        catch (...)
        {
            index.erase(it);
            throw;
        }
        ++next_slot;
    }
}

StoredObjects InMemoryRemovalQueue::takeFirst(int64_t max_count) const
{
    StoredObjects result;
    for (const auto & [slot, blob] : queue)
    {
        if (max_count > 0 && std::ssize(result) >= max_count)
            break;

        result.push_back(blob);
    }

    return result;
}

int64_t InMemoryRemovalQueue::markAsRemoved(const StoredObjects & blobs)
{
    int64_t count = 0;
    for (const auto & blob : blobs)
    {
        if (auto it = index.find(blob); it != index.end())
        {
            queue.erase(it->second);
            index.erase(it);
            ++count;
        }
    }

    return count;
}

bool InMemoryRemovalQueue::containsAny(const StoredObjects & blobs) const
{
    for (const auto & blob : blobs)
        if (index.contains(blob))
            return true;

    return false;
}

}
