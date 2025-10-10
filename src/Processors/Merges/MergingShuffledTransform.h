#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/MergingShuffledAlgorithm.h>


namespace DB
{

/// Implementation of IMergingTransform via MergingShuffledAlgorithm.
class MergingShuffledTransform final : public IMergingTransform<MergingShuffledAlgorithm>
{
public:
    MergingShuffledTransform(
        SharedHeader header,
        size_t num_inputs,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false,
        bool apply_virtual_row_conversions = true,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingShuffledTransform"; }

protected:
    void onNewInput() override;
    void onFinish() override;
};

}
