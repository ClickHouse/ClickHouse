#pragma once

#include <queue>
#include <boost/intrusive_ptr.hpp>

#include <common/logger_useful.h>

#include <Core/Row.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


/// Allows you refer to the row in the block and hold the block ownership,
///  and thus avoid creating a temporary row object.
/// Do not use std::shared_ptr, since there is no need for a place for `weak_count` and `deleter`;
///  does not use Poco::SharedPtr, since you need to allocate a block and `refcount` in one piece;
///  does not use Poco::AutoPtr, since it does not have a `move` constructor and there are extra checks for nullptr;
/// The reference counter is not atomic, since it is used from one thread.
namespace detail
{
struct SharedBlock : Block
{
    int refcount = 0;

    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;

    SharedBlock(Block && block) : Block(std::move(block)) {}
};
}

using SharedBlockPtr = boost::intrusive_ptr<detail::SharedBlock>;

inline void intrusive_ptr_add_ref(detail::SharedBlock * ptr)
{
    ++ptr->refcount;
}

inline void intrusive_ptr_release(detail::SharedBlock * ptr)
{
    if (0 == --ptr->refcount)
        delete ptr;
}


/** Merges several sorted streams into one sorted stream.
  */
class MergingSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** limit - if isn't 0, then we can produce only first limit rows in sorted order.
      * out_row_sources - if isn't nullptr, then at the end of execution it should contain part numbers of each readed row (and needed flag)
      * quiet - don't log profiling info
      */
    MergingSortedBlockInputStream(
            BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_,
            size_t limit_ = 0, WriteBuffer * out_row_sources_buf_ = nullptr, bool quiet_ = false);

    String getName() const override { return "MergingSorted"; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    struct RowRef
    {
        ColumnRawPtrs * columns = nullptr;
        size_t row_num;
        SharedBlockPtr shared_block;

        void swap(RowRef & other)
        {
            std::swap(columns, other.columns);
            std::swap(row_num, other.row_num);
            std::swap(shared_block, other.shared_block);
        }

        /// The number and types of columns must match.
        bool operator==(const RowRef & other) const
        {
            size_t size = columns->size();
            for (size_t i = 0; i < size; ++i)
                if (0 != (*columns)[i]->compareAt(row_num, other.row_num, *(*other.columns)[i], 1))
                    return false;
            return true;
        }

        bool operator!=(const RowRef & other) const
        {
            return !(*this == other);
        }

        bool empty() const { return columns == nullptr; }
        size_t size() const { return empty() ? 0 : columns->size(); }
    };


    Block readImpl() override;

    void readSuffixImpl() override;

    /// Initializes the queue and the next result block.
    void init(Block & header, MutableColumns & merged_columns);

    /// Gets the next block from the source corresponding to the `current`.
    template <typename TSortCursor>
    void fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue);


    const SortDescription description;
    const size_t max_block_size;
    size_t limit;
    size_t total_merged_rows = 0;

    bool first = true;
    bool has_collation = false;
    bool quiet = false;

    /// May be smaller or equal to max_block_size. To do 'reserve' for columns.
    size_t expected_block_size = 0;

    /// Blocks currently being merged.
    size_t num_columns = 0;
    std::vector<SharedBlockPtr> source_blocks;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    using Queue = std::priority_queue<SortCursor>;
    Queue queue;

    using QueueWithCollation = std::priority_queue<SortCursorWithCollation>;
    QueueWithCollation queue_with_collation;

    /// Used in Vertical merge algorithm to gather non-PK columns (on next step)
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
    void setRowRef(RowRef & row_ref, TSortCursor & cursor)
    {
        row_ref.row_num = cursor.impl->pos;
        row_ref.shared_block = source_blocks[cursor.impl->order];
        row_ref.columns = &row_ref.shared_block->all_columns;
    }

    template <typename TSortCursor>
    void setPrimaryKeyRef(RowRef & row_ref, TSortCursor & cursor)
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
