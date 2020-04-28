#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, size_t max_parts_per_block_, UInt64 write_timestamp_)
        : storage(storage_), max_parts_per_block(max_parts_per_block_), write_timestamp(write_timestamp_) {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    size_t max_parts_per_block;
    UInt64 write_timestamp;
};

}
