#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <memory>

namespace DB
{

class IRemovalQueue
{
public:
    virtual ~IRemovalQueue() = default;

    virtual void load() = 0;

    /// Enqueue blobs that are not already present.
    virtual void submitForRemoval(const StoredObjects & blobs) = 0;

    /// Return up to `max_count` oldest blobs (0 means unlimited).
    [[nodiscard]] virtual StoredObjects takeFirst(int64_t max_count) const = 0;

    /// Remove blobs from the queue. Returns the number actually removed.
    virtual int64_t markAsRemoved(const StoredObjects & blobs) = 0;

    /// Return current number of entries in the queue.
    [[nodiscard]] virtual size_t size() const = 0;
};

using RemovalQueuePtr = std::unique_ptr<IRemovalQueue>;

}
