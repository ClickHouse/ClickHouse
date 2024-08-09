#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include "AppendLog.h"
#include "IO/ReadHelpers.h"
#include "VFSShapshotMetadata.h"
#include "VFSLog.h"

#include <fstream>
#include <optional>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <fmt/chrono.h>


namespace DB
{

struct VFSSnapshotEntry
{
    String remote_path;
    Int32 link_count = 0;

    bool operator==(const VFSSnapshotEntry & entry) const;
    static std::optional<VFSSnapshotEntry> deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
};

using VFSSnapshotEntries = std::vector<VFSSnapshotEntry>;

class VFSSnapshotDataBase
{
public:
    VFSSnapshotDataBase() : log(getLogger("VFSSnapshotDataBase(unnamed)")) { }
    virtual ~VFSSnapshotDataBase() = default;

    VFSSnapshotDataBase(const VFSSnapshotDataBase &) = delete;
    VFSSnapshotDataBase(VFSSnapshotDataBase &&) = delete;

    /// Apply wal on the current snapshot and returns metadata of the updated one.
    SnapshotMetadata mergeWithWals(VFSLogItems && wal_items, const SnapshotMetadata & old_snapshot_meta);

private:
    void writeEntryInSnaphot(const VFSSnapshotEntry & entry, WriteBuffer & write_buffer, VFSSnapshotEntries & entries_to_remove);
    VFSSnapshotEntries mergeWithWalsImpl(VFSLogItems && wal_items, ReadBuffer & read_buffer, WriteBuffer & write_buffer);

    LoggerPtr log;

protected:
    /// Methods with storage-specific logic.
    virtual void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) = 0;
    virtual std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const = 0;
    virtual std::pair<std::unique_ptr<WriteBuffer>, String>
    getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata & snapshot_meta) const = 0;
};

class VFSSnapshotDataFromObjectStorage : public VFSSnapshotDataBase
{
public:
    VFSSnapshotDataFromObjectStorage(ObjectStoragePtr object_storage_, const String & name_)
        : object_storage(object_storage_), log(getLogger(fmt::format("VFSSnapshotDataFromObjectStorage({})", name_)))
    {
    }

protected:
    void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) override;
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const override;
    std::pair<std::unique_ptr<WriteBuffer>, String>
    getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata & snapshot_meta) const override;

private:
    ObjectStoragePtr object_storage;
    LoggerPtr log;
};

}
