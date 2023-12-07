#pragma once
#include "Common/ZooKeeper/IKeeper.h"
#include "Disks/ObjectStorages/StoredObject.h"
#include "VFSTraits.h"
#include "base/types.h"

namespace DB
{
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
    static VFSTransactionLogItem deserialize(std::string_view str);
};

// For every object in objects, add a Keeper log entry create request with corresponding type to ops
void getStoredObjectsVFSLogOps(
    VFSTransactionLogItem::Type type, const StoredObjects & objects, Coordination::Requests & ops, const VFSTraits & traits);

struct VFSSnapshot
{
    using ObsoleteObjects = StoredObjects;
    struct ObjectWithRefcount
    {
        StoredObject obj;
        size_t links;
    };

    std::unordered_map<String /*object_storage_path*/, ObjectWithRefcount> items;

    // Update snapshot with logs, return a list of objects that have zero references
    ObsoleteObjects update(const std::vector<VFSTransactionLogItem> & logs);
    static VFSSnapshot deserialize(std::string_view str);
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
    constexpr auto format(const DB::VFSTransactionLogItem & item, auto & ctx) { return fmt::format_to(ctx.out(), "{}", item.serialize()); }
};

template <>
struct fmt::formatter<DB::VFSSnapshot>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSSnapshot & snapshot, auto & ctx)
    {
        fmt::format_to(ctx.out(), "VFSSnapshot(\n");

        for (const auto & [_, obj_pair] : snapshot.items)
            fmt::format_to(ctx.out(), "Item({}, links={})\n", obj_pair.obj, obj_pair.links);

        return fmt::format_to(ctx.out(), ")");
    }
};
