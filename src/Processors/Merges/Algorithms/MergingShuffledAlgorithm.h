#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/Block_fwd.h>
#include <Core/ShuffleCursor.h>


namespace DB
{

/// Merges several shuffled inputs into one shuffled output.
class MergingShuffledAlgorithm final : public IMergingAlgorithm
{
public:
    MergingShuffledAlgorithm(
        SharedHeader header_,
        size_t num_inputs,
        size_t max_block_size_,
        size_t max_block_size_bytes_,
        UInt64 limit_ = 0,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false,
        bool apply_virtual_row_conversions_ = true);

    void addInput();

    const char * getName() const override { return "MergingShuffledAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:
    SharedHeader header;

    MergedData merged_data;

    /// Settings
    const UInt64 limit;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    bool apply_virtual_row_conversions;

    /// Chunks currently being merged.
    Inputs current_inputs;

    ShuffleCursors cursors;

    ShufflingQueue queue;

    Status mergeImpl();

};

}
