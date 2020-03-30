#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/RowRef.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <DataStreams/ColumnGathererStream.h>

#include <common/logger_useful.h>


namespace DB
{

class ReplacingSortedTransform : public IMergingTransform
{
public:
    ReplacingSortedTransform(
        size_t num_inputs, const Block & header,
        SortDescription description_, const String & version_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    String getName() const override { return "ReplacingSorted"; }
    void work() override;

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:
    Logger * log = &Logger::get("ReplacingSortedTransform");

    SortDescription description;
    ssize_t version_column_number = -1;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// Chunks currently being merged.
    using SourceChunks = std::vector<detail::SharedChunkPtr>;
    SourceChunks source_chunks;
    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue;
    bool is_queue_initialized = false;

    using RowRef = detail::RowRef;
    static constexpr size_t max_row_refs = 3; /// last, current, selected.
    RowRef last_row;
    /// RowRef next_key; /// Primary key of next row.
    RowRef selected_row; /// Last row with maximum version for current primary key.
    size_t max_pos = 0; /// The position (into current_row_sources) of the row with the highest version.

    detail::SharedChunkAllocator chunk_allocator;

    /// Sources of rows with the current primary key.
    PODArray<RowSourcePart> current_row_sources;

    void insertRow();
    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, source_chunks[cursor.impl->order]); }
};

}
