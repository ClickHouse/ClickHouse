#pragma once

#include <DataStreams/IBlockOutputStream.h>

#include <common/logger_useful.h>


namespace DB
{

class Block;
class StorageNextGenReplicatedMergeTree;


class NextGenReplicatedBlockOutputStream : public IBlockOutputStream
{
public:
    NextGenReplicatedBlockOutputStream(StorageNextGenReplicatedMergeTree & storage_);

    void write(const Block & block) override;

private:
    StorageNextGenReplicatedMergeTree & storage;
    Logger * log;
};

}
