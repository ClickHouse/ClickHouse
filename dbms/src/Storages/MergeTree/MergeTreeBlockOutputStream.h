#pragma once

#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, size_t max_parts_per_block)
        : storage(storage_), max_parts_per_block(max_parts_per_block) {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageMergeTree & storage;
    size_t max_parts_per_block;
};

}
