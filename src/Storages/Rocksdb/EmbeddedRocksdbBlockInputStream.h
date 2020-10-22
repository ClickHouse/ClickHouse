#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Rocksdb/StorageEmbeddedRocksdb.h>

#include <rocksdb/db.h>

namespace DB
{

class EmbeddedRocksdbBlockInputStream : public IBlockInputStream
{

public:
    EmbeddedRocksdbBlockInputStream(
        StorageEmbeddedRocksdb & storage_, const StorageMetadataPtr & metadata_snapshot_, size_t max_block_size_);

    String getName() const override { return storage.getName(); }
    Block getHeader() const override { return sample_block; }
    Block readImpl() override;

private:
    StorageEmbeddedRocksdb & storage;
    StorageMetadataPtr metadata_snapshot;
    const size_t max_block_size;

    Block sample_block;
    std::unique_ptr<rocksdb::Iterator> iterator;
    size_t primary_key_pos;
    bool finished = false;
};
}
