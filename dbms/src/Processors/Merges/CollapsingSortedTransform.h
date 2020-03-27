#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/SharedChunk.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <DataStreams/ColumnGathererStream.h>

#include <common/logger_useful.h>

namespace DB
{

/** Merges several sorted inputs to one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps no more than one row with the value of the column `sign_column = -1` ("negative row")
  *  and no more than a row with the value of the column `sign_column = 1` ("positive row").
  * That is, it collapses the records from the change log.
  *
  * If the number of positive and negative rows is the same, and the last row is positive, then the first negative and last positive rows are written.
  * If the number of positive and negative rows is the same, and the last line is negative, it writes nothing.
  * If the positive by 1 is greater than the negative rows, then only the last positive row is written.
  * If negative by 1 is greater than positive rows, then only the first negative row is written.
  * Otherwise, a logical error.
  */
class CollapsingSortedTransform final : public IMergingTransform
{
public:
    CollapsingSortedTransform(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        const String & sign_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    String getName() const override { return "CollapsingSortedTransform"; }
    void work() override;

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:
    Logger * log = &Logger::get("CollapsingSortedTransform");

    /// Settings
    SortDescription description;
    bool has_collation = false;

    const size_t sign_column_number;

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
    RowRef first_negative_row;
    RowRef last_positive_row;
    RowRef last_row;

    size_t count_positive = 0;    /// The number of positive rows for the current primary key.
    size_t count_negative = 0;    /// The number of negative rows for the current primary key.
    bool last_is_positive = false;  /// true if the last row for the current primary key is positive.

    /// Fields specific for VERTICAL merge algorithm.
    /// Row numbers are relative to the start of current primary key.
    size_t current_pos = 0;                        /// Current row number
    size_t first_negative_pos = 0;                 /// Row number of first_negative
    size_t last_positive_pos = 0;                  /// Row number of last_positive
    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    size_t count_incorrect_data = 0;    /// To prevent too many error messages from writing to the log.

    void reportIncorrectData();
    void insertRow(RowRef & row);
    void insertRows();
    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, source_chunks[cursor.impl->order]); }
};

}
