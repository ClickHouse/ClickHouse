#pragma once
#include <Processors/Merges/IMergingAlgorithm.h>
#include <Processors/Merges/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

namespace DB
{

class MergingSortedAlgorithm final : public IMergingAlgorithm
{
public:
    MergingSortedAlgorithm(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        size_t max_block_size,
        UInt64 limit_,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes);

    MergingSortedAlgorithm(MergingSortedAlgorithm && other) = default;

    void addInput();

    void initialize(Chunks chunks) override;
    void consume(Chunk chunk, size_t source_num) override;
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
    std::vector<Chunk> source_chunks;

    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SortCursorWithCollation> queue_with_collation;

    void updateCursor(size_t source_num);
    Status insertFromChunk(size_t source_num);

    template <typename TSortingHeap>
    Status mergeImpl(TSortingHeap & queue);
};

}
