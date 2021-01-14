#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>


namespace DB
{

/// Merges several sorted inputs into one sorted output.
class MergingSortedAlgorithm final : public IMergingAlgorithm
{
public:
    MergingSortedAlgorithm(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        size_t max_block_size,
        UInt64 limit_ = 0,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    void addInput();

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    const MergedData & getMergedData() const { return merged_data; }

private:
    MergedData merged_data;

    /// Settings
    SortDescription description;
    UInt64 limit;
    bool has_collation = false;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// Chunks currently being merged.
    Inputs current_inputs;

    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SortCursorWithCollation> queue_with_collation;

    Status insertFromChunk(size_t source_num);

    template <typename TSortingHeap>
    Status mergeImpl(TSortingHeap & queue);
};

}
