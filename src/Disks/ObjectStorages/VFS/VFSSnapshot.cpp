#include "VFSSnapshot.h"
#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromEmptyFile.h>

#include "IO/ReadBufferFromFileBase.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"

namespace ProfileEvents
{
extern const Event VFSGcCumulativeSnapshotBytesRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool VFSSnapshotEntry::operator==(const VFSSnapshotEntry & entry) const
{
    return remote_path == entry.remote_path && link_count == entry.link_count;
}

std::optional<VFSSnapshotEntry> VFSSnapshotEntry::deserialize(ReadBuffer & buf)
{
    if (buf.eof())
        return std::nullopt;

    VFSSnapshotEntry entry;

    readStringUntilWhitespace(entry.remote_path, buf);
    checkChar(' ', buf);
    readIntTextUnsafe(entry.link_count, buf);
    checkChar('\n', buf);

    return entry;
}

void VFSSnapshotEntry::serialize(WriteBuffer & buf) const
{
    writeString(fmt::format("{} {}\n", remote_path, link_count), buf);
}


void VFSSnapshotDataBase::writeEntryInSnaphot(
    const VFSSnapshotEntry & entry, WriteBuffer & write_buffer, VFSSnapshotEntries & entries_to_remove)
{
    LOG_DEBUG(log, "Write entry in snapshot {} links: {}", entry.remote_path, entry.link_count);

    if (entry.link_count < 0)
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Broken links count for file with remote path {}.", entry.remote_path);
    }
    else if (entry.link_count == 0)
    {
        entries_to_remove.emplace_back(entry);
    }
    else if (entry.link_count > 0)
    {
        entry.serialize(write_buffer);
    }
}


SnapshotMetadata VFSSnapshotDataBase::mergeWithWals(VFSLogItems && wal_items, const SnapshotMetadata & old_snapshot_meta)
{
    /// For most of object stroges (like s3 or azure) we don't need the object path, it's generated randomly.
    /// But other ones reqiested to set it manually.
    std::unique_ptr<ReadBuffer> shapshot_read_buffer = getShapshotReadBuffer(old_snapshot_meta);
    auto [new_shapshot_write_buffer, new_object_key] = getShapshotWriteBufferAndSnaphotObject(old_snapshot_meta);

    LOG_DEBUG(log, "Going to merge WAL batch(size {}) with snapshot (key {})", wal_items.size(), old_snapshot_meta.object_storage_key);
    auto entires_to_remove = mergeWithWalsImpl(std::move(wal_items), *shapshot_read_buffer, *new_shapshot_write_buffer);
    new_shapshot_write_buffer->finalize();

    SnapshotMetadata new_snaphot_meta(new_object_key);
    
    LOG_DEBUG(log, "Merge snapshot have finished, going to remove {} from object storage", entires_to_remove.size());
    removeShapshotEntires(entires_to_remove);    
    return new_snaphot_meta;
}


VFSSnapshotEntries VFSSnapshotDataBase::mergeWithWalsImpl(VFSLogItems && wal_items_, ReadBuffer & read_buffer, WriteBuffer & write_buffer)
{
    VFSSnapshotEntries entries_to_remove;

    auto wal_items = std::move(wal_items_);
    std::ranges::sort(
        wal_items, [](const VFSLogItem & left, const VFSLogItem & right) { return left.event.remote_path < right.event.remote_path; });


    auto current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
    LOG_DEBUG(log, "Read entry from snapshot {} {}", current_snapshot_entry->remote_path, current_snapshot_entry->link_count);
    

    // Implementation similar to the merge operation:
    // Iterating thought 2 sorted vectors.
    // If the links count will be == 0, then add to entries_to_remove
    // Else perform sum and append into shapshot
    for (auto wal_iterator = wal_items.begin(); wal_iterator != wal_items.end();)
    {
        // Combine all wal items with the same remote_path into single one.
        VFSSnapshotEntry entry_to_merge = {wal_iterator->event.remote_path, 0};
        while (wal_iterator != wal_items.end() && wal_iterator->event.remote_path == entry_to_merge.remote_path)
        {
            int delta_link_count = 0; // TODO: remove delta and save local_path in the snapshot
            switch (wal_iterator->event.action)
            {
                case VFSAction::LINK:
                case VFSAction::REQUEST:
                    delta_link_count = 1;
                    break;
                case VFSAction::UNLINK:
                    delta_link_count = -1;
                    break;
            }

            entry_to_merge.link_count += delta_link_count;
            ++wal_iterator;
        }
        LOG_DEBUG(log, "Entry to merge {} {}", entry_to_merge.remote_path, entry_to_merge.link_count);

        // Write and skip entries from snaphot which we won't update
        while (current_snapshot_entry && current_snapshot_entry->remote_path < entry_to_merge.remote_path)
        {
            // auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
            // if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
            // {
            //     throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
            // }
            writeEntryInSnaphot(*current_snapshot_entry, write_buffer, entries_to_remove);
            current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
            LOG_DEBUG(log, "Read entry from snapshot {} {}", current_snapshot_entry->remote_path, current_snapshot_entry->link_count);
            //std::swap(current_snapshot_entry, next_snapshot_entry);
        }

        if (!current_snapshot_entry || current_snapshot_entry->remote_path > entry_to_merge.remote_path)
        {
            writeEntryInSnaphot(entry_to_merge, write_buffer, entries_to_remove);
            continue;
        }
        else if (current_snapshot_entry->remote_path == entry_to_merge.remote_path)
        {
            current_snapshot_entry->link_count += entry_to_merge.link_count;
            writeEntryInSnaphot(*current_snapshot_entry, write_buffer, entries_to_remove);
            current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
        }
        else
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable");
        }
    }

    while (current_snapshot_entry)
    {
        // current_snapshot_entry->serialize(write_buffer);
        writeEntryInSnaphot(*current_snapshot_entry, write_buffer, entries_to_remove);
        current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
        // auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);

        // if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
        // {
        //     throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
        // }
        // std::swap(current_snapshot_entry, next_snapshot_entry);
    }
    write_buffer.finalize();
    return entries_to_remove;
}

std::unique_ptr<ReadBuffer> VFSSnapshotDataFromObjectStorage::getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const
{
    if (!snapshot_meta.is_initial_snaphot)
    {
        StoredObject object(snapshot_meta.object_storage_key, "", snapshot_meta.total_size);
        // to do read settings.
        auto res = object_storage->readObject(object, {});
        return res;
    }
    return std::make_unique<ReadBufferFromEmptyFile>();
}

std::pair<std::unique_ptr<WriteBuffer>, String>
VFSSnapshotDataFromObjectStorage::getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata & snapshot_meta) const
{
    String new_object_path = fmt::format("/vfs_shapshots/shapshot_{}", snapshot_meta.znode_version + 1);
    auto new_object_key = object_storage->generateObjectKeyForPath(new_object_path);
    String new_key = String("snapshot/") + new_object_key.serialize();
    LOG_DEBUG(log, "New snapshot path: {}", new_key);
    StoredObject new_object(new_key);
    std::unique_ptr<WriteBuffer> new_shapshot_write_buffer = object_storage->writeObject(new_object, WriteMode::Rewrite);

    return {std::move(new_shapshot_write_buffer), new_key};
}

void VFSSnapshotDataFromObjectStorage::removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove)
{
    StoredObjects objects_to_remove;
    objects_to_remove.reserve(entires_to_remove.size());

    for (const auto & entry : entires_to_remove)
    {
        objects_to_remove.emplace_back(entry.remote_path);
    }
    object_storage->removeObjectsIfExist(objects_to_remove);
    LOG_DEBUG(log, "Removed {} objects", entires_to_remove.size());    
}

}
