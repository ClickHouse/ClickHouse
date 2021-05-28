#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class StorageEmbeddedRocksDB;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

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
