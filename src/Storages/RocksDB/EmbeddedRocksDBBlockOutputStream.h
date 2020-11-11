#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>


namespace DB
{

class EmbeddedRocksDBBlockOutputStream : public IBlockOutputStream
{
public:
    EmbeddedRocksDBBlockOutputStream(
        StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_);

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
};

}
