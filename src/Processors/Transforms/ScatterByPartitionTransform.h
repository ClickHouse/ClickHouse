#pragma once
#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Common/PODArray.h>

namespace DB
{

struct ScatterByPartitionTransform : IProcessor
{
    /// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to
    /// before hashing. Casting is internal to routing; output rows are unchanged.
    ScatterByPartitionTransform(SharedHeader header, size_t output_size_, ColumnNumbers key_columns_, DataTypes hash_cast_types_ = {});

    /// Round-robin mode: each input chunk goes whole to the next output in turn, starting
    /// at `start_bucket`. For distribution without a placement requirement.
    static std::shared_ptr<ScatterByPartitionTransform> createRoundRobin(SharedHeader header, size_t output_size_, size_t start_bucket);

    String getName() const override { return "ScatterByPartitionTransform"; }

    Status prepare() override;
    void work() override;

private:

    void generateOutputChunks();

    size_t output_size;
    ColumnNumbers key_columns;
    DataTypes hash_input_types;
    DataTypes hash_cast_types;
    /// When set, chunks are routed round-robin starting from this output instead of by key hash.
    std::optional<size_t> round_robin_bucket;

    bool has_data = false;
    bool all_outputs_processed = true;
    std::vector<char> was_output_processed;
    Chunk chunk;

    PaddedPODArray<UInt32> hash;
    IColumn::Selector selector;
    Chunks output_chunks;
};

}
