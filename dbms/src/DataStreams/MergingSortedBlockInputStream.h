#pragma once

#include <queue>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <common/logger_useful.h>
#include <Common/SharedBlockRowRef.h>

#include <Core/Row.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


/** Merges several sorted streams into one sorted stream.
  */
class MergingSortedBlockInputStream : public IBlockInputStream
{
public:
    /** limit - if isn't 0, then we can produce only first limit rows in sorted order.
      * out_row_sources - if isn't nullptr, then at the end of execution it should contain part numbers of each readed row (and needed flag)
      * quiet - don't log profiling info
      */
    MergingSortedBlockInputStream(
        const BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_,
        UInt64 limit_ = 0, WriteBuffer * out_row_sources_buf_ = nullptr, bool quiet_ = false, bool average_block_sizes_ = false);

    String getName() const override { return "MergingSorted"; }

    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return header; }

protected:
    /// Simple class, which allows to check stop condition during merge process
    /// in simple case it just compare amount of merged rows with max_block_size
    /// in `count_average` case it compares amount of merged rows with linear combination
    /// of block sizes from which these rows were taken.
    struct MergeStopCondition
    {
        size_t sum_blocks_granularity = 0;
        size_t sum_rows_count = 0;
        bool count_average;
        size_t max_block_size;

        MergeStopCondition(bool count_average_, size_t max_block_size_)
            : count_average(count_average_)
            , max_block_size(max_block_size_)
        {}

        /// add single row from block size `granularity`
        void addRowWithGranularity(size_t granularity)
        {
            sum_blocks_granularity += granularity;
            sum_rows_count++;
        }

        /// check that sum_rows_count is enough
        bool checkStop() const;

        bool empty() const
        {
            return sum_blocks_granularity == 0;
        }
    };

    Block readImpl() override;

    void readSuffixImpl() override;

    /// Initializes the queue and the columns of next result block.
    void init(MutableColumns & merged_columns);

    /// Gets the next block from the source corresponding to the `current`.
    template <typename TSortCursor>
    void fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue);


    Block header;

    const SortDescription description;
    const size_t max_block_size;
    UInt64 limit;
    UInt64 total_merged_rows = 0;

    bool first = true;
    bool has_collation = false;
    bool quiet = false;
    bool average_block_sizes = false;

    /// May be smaller or equal to max_block_size. To do 'reserve' for columns.
    size_t expected_block_size = 0;

    /// Blocks currently being merged.
    size_t num_columns = 0;
    std::vector<SharedBlockPtr> source_blocks;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    using Queue = std::priority_queue<SortCursor>;
    Queue queue_without_collation;

    using QueueWithCollation = std::priority_queue<SortCursorWithCollation>;
    QueueWithCollation queue_with_collation;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf;


    /// These methods are used in Collapsing/Summing/Aggregating... SortedBlockInputStream-s.

    /// Save the row pointed to by cursor in `row`.
    template <typename TSortCursor>
    void setRow(Row & row, TSortCursor & cursor)
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            try
            {
                cursor->all_columns[i]->get(cursor->pos, row[i]);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);

                /// Find out the name of the column and throw more informative exception.

                String column_name;
                for (const auto & block : source_blocks)
                {
                    if (i < block->columns())
                    {
                        column_name = block->safeGetByPosition(i).name;
                        break;
                    }
                }

                throw Exception("MergingSortedBlockInputStream failed to read row " + toString(cursor->pos)
                    + " of column " + toString(i) + (column_name.empty() ? "" : " (" + column_name + ")"),
                    ErrorCodes::CORRUPTED_DATA);
            }
        }
    }

    template <typename TSortCursor>
    void setRowRef(SharedBlockRowRef & row_ref, TSortCursor & cursor)
    {
        row_ref.row_num = cursor.impl->pos;
        row_ref.shared_block = source_blocks[cursor.impl->order];
        row_ref.columns = &row_ref.shared_block->all_columns;
    }

    template <typename TSortCursor>
    void setPrimaryKeyRef(SharedBlockRowRef & row_ref, TSortCursor & cursor)
    {
        row_ref.row_num = cursor.impl->pos;
        row_ref.shared_block = source_blocks[cursor.impl->order];
        row_ref.columns = &row_ref.shared_block->sort_columns;
    }

private:

    /** We support two different cursors - with Collation and without.
     * Templates are used instead of polymorphic SortCursor and calls to virtual functions.
     */
    template <typename TSortCursor>
    void initQueue(std::priority_queue<TSortCursor> & queue);

    template <typename TSortCursor>
    void merge(MutableColumns & merged_columns, std::priority_queue<TSortCursor> & queue);

    Logger * log = &Logger::get("MergingSortedBlockInputStream");

    /// Read is finished.
    bool finished = false;
};

}
