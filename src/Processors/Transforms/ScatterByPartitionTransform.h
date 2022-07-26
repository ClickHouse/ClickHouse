#pragma once
#include "Common/WeakHash.h"
#include "Core/ColumnNumbers.h"
#include "Processors/IProcessor.h"

namespace DB
{

struct ScatterByPartitionTransform : IProcessor
{
    ScatterByPartitionTransform(Block header, size_t output_size_, ColumnNumbers key_columns_);

    String getName() const override { return "ScatterByPartitionTransform"; }

    Status prepare() override;
    void work() override;

private:

    void generateOutputChunks();

    size_t output_size;
    ColumnNumbers key_columns;

    bool has_data = false;
    bool all_outputs_processed = true;
    std::vector<char> was_output_processed;
    Chunk chunk;

    WeakHash32 hash;
    Chunks output_chunks;
};

}
