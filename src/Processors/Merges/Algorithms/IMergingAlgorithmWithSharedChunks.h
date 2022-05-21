#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Core/SortDescription.h>

namespace DB
{

class IMergingAlgorithmWithSharedChunks : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithSharedChunks(
        Block header_, size_t num_inputs, SortDescription description_, WriteBuffer * out_row_sources_buf_, size_t max_row_refs);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;

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

    SortingHeap<SortCursor> queue;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    using RowRef = detail::RowRefWithOwnedChunk;
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, sources[cursor.impl->order].chunk); }
    bool skipLastRowFor(size_t input_number) const { return sources[input_number].skip_last_row; }
};

}
