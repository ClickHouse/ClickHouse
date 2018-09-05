#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Merges several sorted streams to one.
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
class CollapsingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    CollapsingSortedBlockInputStream(
            BlockInputStreams inputs_, const SortDescription & description_,
            const String & sign_column, size_t max_block_size_, WriteBuffer * out_row_sources_buf_ = nullptr)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_)
    {
        sign_column_number = header.getPositionByName(sign_column);
    }

    String getName() const override { return "CollapsingSorted"; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    size_t sign_column_number;

    Logger * log = &Logger::get("CollapsingSortedBlockInputStream");

    /// Read is finished.
    bool finished = false;

    RowRef current_key;         /// The current primary key.
    RowRef next_key;            /// The primary key of the next row.

    RowRef first_negative;        /// The first negative row for the current primary key.
    RowRef last_positive;         /// The last positive row for the current primary key.
    RowRef last_negative;         /// Last negative row. It is only stored if there is not one row is written to output.

    size_t count_positive = 0;    /// The number of positive rows for the current primary key.
    size_t count_negative = 0;    /// The number of negative rows for the current primary key.
    bool last_is_positive = false;  /// true if the last row for the current primary key is positive.

    size_t count_incorrect_data = 0;    /// To prevent too many error messages from writing to the log.

    size_t blocks_written = 0;

    /// Fields specific for VERTICAL merge algorithm.
    /// Row numbers are relative to the start of current primary key.
    size_t current_pos = 0;                        /// Current row number
    size_t first_negative_pos = 0;                 /// Row number of first_negative
    size_t last_positive_pos = 0;                  /// Row number of last_positive
    size_t last_negative_pos = 0;                  /// Row number of last_negative
    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    /** We support two different cursors - with Collation and without.
     *  Templates are used instead of polymorphic SortCursors and calls to virtual functions.
     */
    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output to result rows for the current primary key.
    void insertRows(MutableColumns & merged_columns, size_t & merged_rows);

    void reportIncorrectData();
};

}
