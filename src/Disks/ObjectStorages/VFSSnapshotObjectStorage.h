#pragma once
#include <Disks/ObjectStorages/VFSSettings.h>
#include <Disks/ObjectStorages/VFSSnapshotStorage.h>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

class VFSSnapshotReadStreamFromObjectStorage : public IVFSSnapshotReadStream
{
public:
    using IVFSSnapshotReadStream::entry_type;

    VFSSnapshotReadStreamFromObjectStorage(ObjectStoragePtr object_storage_, const StoredObject & snapshot_object_);

private:
    entry_type nextImpl() override;

    ObjectStoragePtr object_storage;
    StoredObject snapshot_object;
    std::optional<Lz4InflatingReadBuffer> read_buffer;
};

class VFSSnapshotWriteStreamFromObjectStorage : public IVFSSnapshotWriteStream
{
public:
    VFSSnapshotWriteStreamFromObjectStorage(
        ObjectStoragePtr object_storage_, const StoredObject & snapshot_object_, int snapshot_lz4_compression_level_);

private:
    void writeImpl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;
    WriteBuffer & getWriteBuffer();

    ObjectStoragePtr object_storage;
    StoredObject snapshot_object;
    // TODO myrrc research zstd dictionary builder or zstd for compression
    int snapshot_lz4_compression_level;
    std::optional<Lz4DeflatingWriteBuffer> write_buffer;
};

class VFSSnapshotObjectStorage : public IVFSSnapshotStorage
{
public:
    VFSSnapshotObjectStorage(ObjectStoragePtr object_storage_, const String & prefix_, const VFSSettings & settings);

    VFSSnapshotReadStreamPtr readSnapshot(const String & name) override;
    VFSSnapshotWriteStreamPtr writeSnapshot(const String & name) override;
    VFSSnapshotSortingWriteStreamPtr writeSnapshotWithSorting(const String & name) override;
    Strings listSnapshots() const override;
    size_t removeSnapshots(Strings names) override;

private:
    ObjectStoragePtr object_storage;
    String prefix;
    int snapshot_lz4_compression_level;
};

}
