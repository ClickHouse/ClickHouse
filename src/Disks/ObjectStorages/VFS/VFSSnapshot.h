#pragma once
#include "AppendLog.h"
#include "IO/ReadHelpers.h"

#include <fstream>
#include <optional>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <fmt/chrono.h>


namespace DB
{
class IObjectStorage;
class ReadBuffer;
class WriteBuffer;

/////// START OF TMP
struct WALItem
{
    String remote_path;
    Int64 delta_link_count;
    String status;
    time_t last_update_timestamp;
    UInt64 last_update_wal_pointer;
    UInt64 wal_uuid;
    WALItem(String path, Int64 delta)
        : remote_path(path), delta_link_count(delta), status(""), last_update_timestamp(0), last_update_wal_pointer(0), wal_uuid(0)
    {
    }
    WALItem() : remote_path(""), delta_link_count(0), status(""), last_update_timestamp(0), last_update_wal_pointer(0), wal_uuid(0) { }
    static WALItem deserialize(const String & str)
    {
        ReadBufferFromString rb(str);
        WALItem item;

        readStringUntilWhitespace(item.remote_path, rb);
        checkChar(' ', rb);
        readIntTextUnsafe(item.delta_link_count, rb);
        return item;
    }
    String serialize() const { return fmt::format("{} {}", remote_path, delta_link_count); }
};

using WALItems = std::vector<WALItem>;

std::pair<WALItems, UInt64> getWalItems(WAL::AppendLog & alog, size_t batch_size);

/////// END OF TMP

struct VFSSnapshotEntry
{
    String remote_path;
    Int32 link_count = 0;

    bool operator==(const VFSSnapshotEntry & entry) const;
    static std::optional<VFSSnapshotEntry> deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
};

using VFSSnapshotEntries = std::vector<VFSSnapshotEntry>;

VFSSnapshotEntries mergeWithWals(WALItems & wal_items, ReadBuffer & read_buffer, WriteBuffer & write_buffer);

}
