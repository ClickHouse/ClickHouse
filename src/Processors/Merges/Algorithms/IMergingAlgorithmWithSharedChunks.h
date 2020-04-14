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
        size_t num_inputs,
        SortDescription description_,
        WriteBuffer * out_row_sources_buf_,
        size_t max_row_refs);

    void initialize(Chunks chunks) override;
    void consume(Chunk chunk, size_t source_num) override;

private:
    SortDescription description;

    /// Allocator must be destroyed after source_chunks.
    detail::SharedChunkAllocator chunk_allocator;

    SortCursorImpls cursors;

protected:
    /// Chunks currently being merged.
    using SourceChunks = std::vector<detail::SharedChunkPtr>;
    SourceChunks source_chunks;

    SortingHeap<SortCursor> queue;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    using RowRef = detail::RowRefWithOwnedChunk;
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, source_chunks[cursor.impl->order]); }
};

}
