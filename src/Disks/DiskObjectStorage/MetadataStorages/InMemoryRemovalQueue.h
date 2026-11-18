#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IRemovalQueue.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <map>
#include <unordered_map>

namespace DB
{

class InMemoryRemovalQueue final : public IRemovalQueue
{
public:
    void load() override {}

    void submitForRemoval(const StoredObjects & blobs) override;

    StoredObjects takeFirst(int64_t max_count) const override;

    int64_t markAsRemoved(const StoredObjects & blobs) override;

    size_t size() const override { return index.size(); }

private:
    int64_t next_slot = 0;
    std::map<int64_t, StoredObject> queue;
    std::unordered_map<StoredObject, int64_t> index;
};

}
