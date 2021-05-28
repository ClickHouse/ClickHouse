#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace rocksdb
{
    class Iterator;
}

namespace DB
{

class StorageEmbeddedRocksDB;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class EmbeddedRocksDBBlockInputStream : public IBlockInputStream
{

public:
    EmbeddedRocksDBBlockInputStream(
        StorageEmbeddedRocksDB & storage_, const StorageMetadataPtr & metadata_snapshot_, size_t max_block_size_);

    String getName() const override { return "EmbeddedRocksDB"; }
    Block getHeader() const override { return sample_block; }
    Block readImpl() override;

private:
    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    const size_t max_block_size;

    Block sample_block;
    std::unique_ptr<rocksdb::Iterator> iterator;
    size_t primary_key_pos;
    bool finished = false;
};
}
