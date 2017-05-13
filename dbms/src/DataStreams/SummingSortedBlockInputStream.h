#pragma once

#include <Core/Row.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Merges several sorted streams into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  collapses them into one row, summing all the numeric columns except the primary key.
  * If in all numeric columns, except for the primary key, the result is zero, it deletes the row.
  */
class SummingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    SummingSortedBlockInputStream(BlockInputStreams inputs_,
        const SortDescription & description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum_,
        size_t max_block_size_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_), column_names_to_sum(column_names_to_sum_)
    {
    }

    String getName() const override { return "SummingSorted"; }

    String getID() const override;

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    Logger * log = &Logger::get("SummingSortedBlockInputStream");

    /// Read up to the end.
    bool finished = false;

    /// Columns with which numbers should be summed.
    Names column_names_to_sum;    /// If set, it is converted to column_numbers_to_sum when initialized.
    ColumnNumbers column_numbers_to_sum;

    /** A table can have nested tables that are treated in a special way.
     *    If the name of the nested table ends in `Map` and it contains at least two columns,
     *    satisfying the following criteria:
     *        - the first column, as well as all columns whose names end with `ID`, `Key` or `Type` - numeric ((U)IntN, Date, DateTime);
     *        (a tuple of such columns will be called `keys`)
     *        - the remaining columns are arithmetic ((U)IntN, Float32/64), called (`values`...).
     *    This nested table is treated as a mapping (keys...) => (values...) and when merge
     *    its rows, the merge of the elements of two sets by (keys...) with summing of corresponding (values...).
     *
     *    Example:
     *    [(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
     *    [(1, 100)] + [(1, 150)] -> [(1, 250)]
     *    [(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
     *    [(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
     *
     *  This very unusual functionality is made exclusively for the banner system,
     *   is not supposed for use by anyone else,
     *   and can be deleted at any time.
     */

    /// Stores numbers of key-columns and value-columns.
    struct MapDescription
    {
        std::vector<size_t> key_col_nums;
        std::vector<size_t> val_col_nums;
    };

    /// Found nested Map-tables.
    std::vector<MapDescription> maps_to_sum;

    RowRef current_key;        /// The current primary key.
    RowRef next_key;           /// The primary key of the next row.

    Row current_row;
    bool current_row_is_zero = true;    /// The current row is summed to zero, and it should be deleted.

    bool output_is_non_empty = false;    /// Have we given out at least one row as a result.

    /** We support two different cursors - with Collation and without.
     *  Templates are used instead of polymorphic SortCursor and calls to virtual functions.
     */
    template <class TSortCursor>
    void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

    /// Insert the summed row for the current group into the result.
    void insertCurrentRow(ColumnPlainPtrs & merged_columns);

    /** For nested Map, a merge by key is performed with the ejection of rows of nested arrays, in which
      * all items are zero.
      */
    template <class TSortCursor>
    bool mergeMaps(Row & row, TSortCursor & cursor);

    template <class TSortCursor>
    bool mergeMap(const MapDescription & map, Row & row, TSortCursor & cursor);

    /** Add the row under the cursor to the `row`.
      * Returns false if the result is zero.
      */
    template <class TSortCursor>
    bool addRow(Row & row, TSortCursor & cursor);
};

}
