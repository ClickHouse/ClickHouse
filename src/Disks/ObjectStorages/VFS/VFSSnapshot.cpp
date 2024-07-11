#include "VFSSnapshot.h"
#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/IObjectStorage.h>
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

/// tmp start

std::pair<WALItems, UInt64> getWalItems(WAL::AppendLog & alog, size_t batch_size)
{
    auto alog_entries = alog.readFront(batch_size);
    WALItems result;
    UInt64 max_index = 0;
    result.reserve(alog_entries.size());
    for (const auto & entry : alog_entries)
    {
        String data(entry.data.begin(), entry.data.end());
        result.emplace_back(WALItem::deserialize(data));
        max_index = std::max(max_index, entry.index);
    }
    return {result, max_index};
}
/// tmp end


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


void writeEntryInSnaphot(const VFSSnapshotEntry & entry, WriteBuffer & write_buffer, VFSSnapshotEntries & entries_to_remove)
{
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


VFSSnapshotEntries mergeWithWals(WALItems & wal_items, ReadBuffer & read_buffer, WriteBuffer & write_buffer)
{
    VFSSnapshotEntries entries_to_remove;

    std::sort(
        wal_items.begin(),
        wal_items.end(),
        [](const WALItem & left, const WALItem & right) { return left.remote_path < right.remote_path; });


    auto current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);

    // Implementation similar to the merge operation:
    // Iterating thought 2 sorted vectors.
    // If the links count will be == 0, then add to entries_to_remove
    // Else perform sum and append into shapshot
    for (auto wal_iterator = wal_items.begin(); wal_iterator != wal_items.end();)
    {
        // Combine all wal items with the same remote_path into single one.
        VFSSnapshotEntry entry_to_merge = {wal_iterator->remote_path, 0};
        while (wal_iterator != wal_items.end() && wal_iterator->remote_path == entry_to_merge.remote_path)
        {
            entry_to_merge.link_count += wal_iterator->delta_link_count;
            ++wal_iterator;
        }

        // Write and skip entries from snaphot which we won't update
        while (current_snapshot_entry && current_snapshot_entry->remote_path < entry_to_merge.remote_path)
        {
            auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
            if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
            {
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
            }
            current_snapshot_entry->serialize(write_buffer);
            std::swap(current_snapshot_entry, next_snapshot_entry);
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
        current_snapshot_entry->serialize(write_buffer);
        auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);

        if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
        }
        std::swap(current_snapshot_entry, next_snapshot_entry);
    }
    write_buffer.finalize();
    return entries_to_remove;
}
}
