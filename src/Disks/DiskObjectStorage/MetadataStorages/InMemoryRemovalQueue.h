#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <map>
#include <unordered_map>

namespace DB
{

class InMemoryRemovalQueue
{
public:
    /// Enqueue blobs that are not already present.
    void submitForRemoval(const StoredObjects & blobs);

    /// Return up to `max_count` oldest blobs (0 means unlimited).
    StoredObjects takeFirst(int64_t max_count) const;

    /// Remove blobs from the queue. Returns the number actually removed.
    int64_t markAsRemoved(const StoredObjects & blobs);

    /// Check if any of the given blobs are still in the queue.
    bool containsAny(const StoredObjects & blobs) const;

private:
    int64_t next_slot = 0;
    std::map<int64_t, StoredObject> queue;
    std::unordered_map<StoredObject, int64_t> index;
};

}
