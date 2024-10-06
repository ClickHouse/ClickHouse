#include "VFSLog.h"

#include <Disks/ObjectStorages/VFS/JSONSerializer.h>
#include <Common/Concepts.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>


namespace DB
{

VFSLog::VFSLog(const String & log_dir) : wal(log_dir) // TODO: read settings from config
{
}

void VFSLog::link(const String & remote_path, const String & local_path)
{
    VFSEvent event {remote_path, local_path, {}, {}, VFSAction::LINK};
    write(event);
    LOG_DEBUG(getLogger("VFSLog"), "Link remote_path: {} local_path: {}", remote_path, local_path);
}

void VFSLog::link(const DiskObjectStorageMetadata & metadata)
{
    for (const auto & key_with_meta : metadata.getKeysWithMeta())    
        link(key_with_meta.key.serialize(), metadata.getMetadataFilePath());    
}

void VFSLog::unlink(const String & remote_path, const String & local_path)
{
    VFSEvent event {remote_path, local_path, {}, {}, VFSAction::UNLINK};
    write(event);
    LOG_DEBUG(getLogger("VFSLog"), "Unlink remote_path: {} local_path: {}", remote_path, local_path);
}

void VFSLog::unlink(const DiskObjectStorageMetadata & metadata)
{
    for (const auto & key_with_meta : metadata.getKeysWithMeta())    
        unlink(key_with_meta.key.serialize(), metadata.getMetadataFilePath());    
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
