#include "VFSLog.h"

#include <Disks/ObjectStorages/VFS/JSONSerializer.h>
#include <Common/Concepts.h>


namespace DB
{

VFSLog::VFSLog(const String & log_dir) : wal(log_dir) // TODO: read settings from config
{
}


UInt64 VFSLog::write(const VFSEvent & event)
{
    return wal.append(JSONSerializer::serialize(event));
}

VFSLogItems VFSLog::read(size_t count) const
{
    VFSLogItems vfs_items;
    WAL::Entries wal_items = wal.readFront(count);
    auto wal_id = wal.getID(); // TODO: It makes sense to return wal id from readFront
    vfs_items.reserve(wal_items.size());

    std::ranges::transform(
        wal_items,
        std::back_inserter(vfs_items),
        [&wal_id](auto && wal_item) -> VFSLogItem
        {
            VFSLogItem item;
            item.event = JSONSerializer::deserialize(wal_item.data);
            item.wal.index = wal_item.index;
            item.wal.id = wal_id;

            return item;
        });

    return vfs_items;
}

size_t VFSLog::dropUpTo(UInt64 index)
{
    return wal.dropUpTo(index);
}

size_t VFSLog::size() const
{
    return wal.size();
}

}
