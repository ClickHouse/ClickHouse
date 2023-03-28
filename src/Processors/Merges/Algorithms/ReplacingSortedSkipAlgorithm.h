#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class ReplacingSortedSkipAlgorithm final : public IMergingAlgorithm
{

public:
    ReplacingSortedSkipAlgorithm(
        const Block & header,
        const Block & output_header,
        size_t num_inputs,
        SortDescription description_, const String & version_column,
        size_t max_block_size,
        bool use_average_block_sizes = false);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

private:

    SortCursorImpls cursors;

    struct Source
    {
        detail::SharedChunkPtr chunk;
        bool skip_last_row;
        bool skip_chunk;
    };

    Block header;
    SortDescription description;

    /// Allocator must be destroyed after source_chunks.
    detail::SharedChunkAllocator chunk_allocator;

    /// Sources currently being merged.
    using Sources = std::vector<Source>;
    Sources sources;

    SortingQueue<SortCursor> queue;
    MergedData merged_data;

    ssize_t version_column_number = -1;

    using RowRef = detail::RowRefWithOwnedChunk;
    static constexpr size_t max_row_refs = 2; /// last, current.
    RowRef selected_row; /// Last row with maximum version for current primary key

    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, sources[cursor.impl->order].chunk); }
    bool skipLastRowFor(size_t input_number) const { return sources[input_number].skip_last_row; }

    void emitChunk(detail::SharedChunkPtr &chunk, bool skip);
};

}
