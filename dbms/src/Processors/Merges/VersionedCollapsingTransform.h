#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/RowRef.h>
#include <Processors/Merges/FixedSizeDequeWithGaps.h>
#include <Processors/Merges/MergedData.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <DataStreams/ColumnGathererStream.h>

#include <common/logger_useful.h>
#include <queue>


namespace DB
{

class VersionedCollapsingTransform final : public IMergingTransform
{
public:
    /// Don't need version column. It's in primary key.
    VersionedCollapsingTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & sign_column_,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    String getName() const override { return "VersionedCollapsingTransform"; }
    void work() override;

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:
    Logger * log = &Logger::get("VersionedCollapsingTransform");

    MergedData merged_data;

    SortDescription description;
    size_t sign_column_number = 0;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// Chunks currently being merged.
    using SourceChunks = std::vector<detail::SharedChunkPtr>;
    SourceChunks source_chunks;
    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue;
    bool is_queue_initialized = false;

    using RowRef = detail::RowRefWithOwnedChunk;
    const size_t max_rows_in_queue;
    /// Rows with the same primary key and sign.
    FixedSizeDequeWithGaps<RowRef> current_keys;
    Int8 sign_in_queue = 0;

    detail::SharedChunkAllocator chunk_allocator;

    std::queue<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    void insertGap(size_t gap_size);
    void insertRow(size_t skip_rows, const RowRef & row);
    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, source_chunks[cursor.impl->order]); }
};

}
