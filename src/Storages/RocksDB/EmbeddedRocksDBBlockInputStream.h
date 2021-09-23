#pragma once

#include <Processors/ISource.h>
#include <Processors/Sources/SourceWithProgress.h>


namespace rocksdb
{
    class Iterator;
}

namespace DB
{

class StorageEmbeddedRocksDB;

class EmbeddedRocksDBBlockInputStream : public SourceWithProgress
{

public:
    EmbeddedRocksDBBlockInputStream(
        StorageEmbeddedRocksDB & storage_,
        const Block & header,
        size_t max_block_size_);

    String getName() const override { return "EmbeddedRocksDB"; }

protected:
    Chunk generate() override;

private:
    StorageEmbeddedRocksDB & storage;
    const size_t max_block_size;

    std::unique_ptr<rocksdb::Iterator> iterator;
    size_t primary_key_pos;

};

}
