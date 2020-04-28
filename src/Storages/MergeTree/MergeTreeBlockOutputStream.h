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
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, StorageMetadataPtr metadata_, size_t max_parts_per_block_)
        : storage(storage_), metadata(metadata_), max_parts_per_block(max_parts_per_block_) {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata;
    size_t max_parts_per_block;
};

}
