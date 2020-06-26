#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, size_t max_parts_per_block_, size_t in_memory_parts_timeout_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_parts_per_block(max_parts_per_block_)
        , in_memory_parts_timeout(in_memory_parts_timeout_)
    {
    }

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    size_t in_memory_parts_timeout;
};

}
