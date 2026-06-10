#pragma once
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Common/WeakHash.h>

namespace DB
{

struct ScatterByPartitionTransform : IProcessor
{
    /// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to
    /// before hashing. Casting is internal to routing; output rows are unchanged.
    ScatterByPartitionTransform(SharedHeader header, size_t output_size_, ColumnNumbers key_columns_, DataTypes hash_cast_types_ = {});

    String getName() const override { return "ScatterByPartitionTransform"; }

    Status prepare() override;
    void work() override;

private:

    void generateOutputChunks();

    size_t output_size;
    ColumnNumbers key_columns;
    DataTypes hash_input_types;
    DataTypes hash_cast_types;

    bool has_data = false;
    bool all_outputs_processed = true;
    std::vector<char> was_output_processed;
    Chunk chunk;

    WeakHash32 hash;
    Chunks output_chunks;
};

}
