#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>


namespace DB
{

/// Merges several sorted inputs into one sorted output.
class MergingSortedTransform : public IMergingTransform
{
public:
    MergingSortedTransform(
        const Block & header,
        size_t num_inputs,
        SortDescription description,
        size_t max_block_size,
        UInt64 limit_ = 0,
        bool quiet_ = false,
        bool use_average_block_sizes = false,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingSortedTransform"; }
    void work() override;

private:

    void onNewInput() override;
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;
    void onFinish() override;

    /// Settings
    SortDescription description;
    UInt64 limit;
    bool has_collation = false;
    bool quiet = false;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// Chunks currently being merged.
    std::vector<Chunk> source_chunks;

    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SortCursorWithCollation> queue_with_collation;
    bool is_queue_initialized = false;

    template <typename TSortingHeap>
    void merge(TSortingHeap & queue);

    void insertFromChunk(size_t source_num);
    void updateCursor(Chunk chunk, size_t source_num);
};

}
