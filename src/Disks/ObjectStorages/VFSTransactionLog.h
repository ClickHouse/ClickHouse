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

struct VFSTransactionLogItem : StoredObject
{
    // For Link and Unlink only remote_path (of StoredObject) is filled
    enum class Type
    {
        CreateInode,
        Link,
        Unlink
    } type;

    String serialize() const;
    VFSTransactionLogItem & deserialize(std::string_view str);
};

// For every object in objects, add a Keeper log entry create request with corresponding type to ops
void getStoredObjectsVFSLogOps( //NOLINT
    VFSTransactionLogItem::Type type,
    const StoredObjects & objects,
    Coordination::Requests & ops);

struct VFSSnapshot
{
    using ObsoleteObjects = StoredObjects;
    using ObjectWithRefcount = std::pair<size_t/*links*/, StoredObject>;

    std::unordered_map<String /*object_storage_path*/, ObjectWithRefcount> items;

    // Update snapshot with logs, return a list of objects that have zero references
    ObsoleteObjects update(const std::vector<VFSTransactionLogItem> & logs);
    VFSSnapshot & deserialize(std::string_view str);
    String serialize() const;
};

struct VFSSnapshotWithObsoleteObjects
{
    VFSSnapshot snapshot;
    VFSSnapshot::ObsoleteObjects obsolete_objects;
};
}

template <>
struct fmt::formatter<DB::VFSTransactionLogItem>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSTransactionLogItem & item, auto & ctx)
    {
        return fmt::format_to(ctx.out(), "LogItem({})", item.serialize());
    }
};
