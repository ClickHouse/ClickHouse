#pragma once
#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Distributes rows across outputs by dividing a UInt64 key column's value range
/// into equal-sized buckets: bucket = (value - min_value) * output_size / range.
/// Unlike ScatterByPartitionTransform (which uses hash), this preserves locality:
/// consecutive key values land in the same bucket.
class ScatterByRangeTransform : public IProcessor
{
public:
    ScatterByRangeTransform(
        SharedHeader header, size_t output_size_,
        size_t key_column_position_, UInt64 min_value_, UInt64 max_value_);

    String getName() const override { return "ScatterByRangeTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    size_t output_size;
    size_t key_column_position;
    UInt64 min_value;
    UInt64 range; /// max_value - min_value + 1

    bool has_data = false;
    bool all_outputs_processed = true;
    std::vector<char> was_output_processed;
    Chunk chunk;
    Chunks output_chunks;
};

}
