#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class EmbeddedRocksDBBlockOutputStream : public IBlockOutputStream
{
public:
    explicit EmbeddedRocksDBBlockOutputStream(
        StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
};

}
