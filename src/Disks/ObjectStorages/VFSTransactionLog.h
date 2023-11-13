#pragma once
#include "Common/ZooKeeper/IKeeper.h"
#include "Disks/ObjectStorages/StoredObject.h"
#include "base/types.h"

namespace DB
{
// TODO myrrc not sure this one belongs here, used for locking only
static constexpr auto VFS_BASE_NODE = "/vfs_log";
static const String VFS_LOG_BASE_NODE = "/vfs_log/ops";
static constexpr auto VFS_LOG_ITEM = "/vfs_log/ops/log-";

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

struct VFSSnapshot
{
    size_t end_logpointer{0};
    std::unordered_map<String /*object_storage_path*/, size_t /*links*/> items;

    void add(const VFSTransactionLogItem & item);
    String serializeItems() const;
};
}
