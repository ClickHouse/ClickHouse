#pragma once
#include "Common/ZooKeeper/IKeeper.h"
#include "Disks/ObjectStorages/StoredObject.h"
#include "base/types.h"

namespace DB
{
static constexpr auto VFS_LOG_ITEM = "/vfs_log/log-";

struct VFSTransactionLogItem
{
    enum class Type
    {
        CreateInode,
        Link,
        Unlink
    } type;
    String object_storage_path;

    String serialize() const;
    void deserialize(std::string_view str);
};

// For every object in objects, add a Keeper log entry create request with corresponding type to ops
void getStoredObjectsVFSLogOps( //NOLINT
    VFSTransactionLogItem::Type type,
    const StoredObjects & objects,
    Coordination::Requests & ops);
}
