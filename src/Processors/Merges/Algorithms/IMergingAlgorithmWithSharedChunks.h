#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/SortDescription.h>

namespace DB
{

class IMergingAlgorithmWithSharedChunks : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithSharedChunks(
        Block header_, size_t num_inputs, SortDescription description_, WriteBuffer * out_row_sources_buf_, size_t max_row_refs, std::unique_ptr<MergedData> merged_data_);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;

    MergedStats getMergedStats() const override { return merged_data->getMergedStats(); }

private:
    Block header;
    SortDescription description;

    /// Allocator must be destroyed after source_chunks.
    detail::SharedChunkAllocator chunk_allocator;

    SortCursorImpls cursors;

protected:
    struct Source
    {
        detail::SharedChunkPtr chunk;
        bool skip_last_row;
    };

    /// Sources currently being merged.
    using Sources = std::vector<Source>;
    Sources sources;
    std::vector<size_t> sources_origin_merge_tree_part_level;

    SortingQueue<SortCursor> queue;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    std::unique_ptr<MergedData> merged_data;

    using RowRef = detail::RowRefWithOwnedChunk;
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, sources[cursor.impl->order].chunk); }
    bool skipLastRowFor(size_t input_number) const { return sources[input_number].skip_last_row; }
    bool rowsHaveDifferentSortColumns(const RowRef & lhs, const RowRef & rhs)
    {
        /// By the time this method is called, `sources_origin_merge_tree_part_level[lhs.source_stream_index]` must have been
        /// initialized in either `initialize` or `consume`
        if (lhs.source_stream_index == rhs.source_stream_index && sources_origin_merge_tree_part_level[lhs.source_stream_index] > 0)
            return true;
        return !lhs.hasEqualSortColumnsWith(rhs);
    }
};

}
