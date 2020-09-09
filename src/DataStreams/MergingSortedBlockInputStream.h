#pragma once

#include <Core/Row.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockInputStream.h>


namespace Poco { class Logger; }


namespace DB
{

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
        const BlockInputStreams & inputs_, SortDescription description_, size_t max_block_size_,
        UInt64 limit_ = 0, WriteBuffer * out_row_sources_buf_ = nullptr, bool quiet_ = false);

    String getName() const override { return "MergingSorted"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

    void readSuffixImpl() override;

    /// Initializes the queue and the columns of next result block.
    void init(MutableColumns & merged_columns);

    /// Gets the next block from the source corresponding to the `current`.
    template <typename TSortCursor>
    void fetchNextBlock(const TSortCursor & current, SortingHeap<TSortCursor> & queue);

    Block header;

    const SortDescription description;
    const size_t max_block_size;
    UInt64 limit;
    UInt64 total_merged_rows = 0;

    bool first = true;
    bool has_collation = false;
    bool quiet = false;

    /// May be smaller or equal to max_block_size. To do 'reserve' for columns.
    size_t expected_block_size = 0;

    /// Blocks currently being merged.
    size_t num_columns = 0;
    Blocks source_blocks;

    SortCursorImpls cursors;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SortCursorWithCollation> queue_with_collation;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf;

private:

    /** We support two different cursors - with Collation and without.
      * Templates are used instead of polymorphic SortCursor and calls to virtual functions.
      */
    template <typename TSortingHeap>
    void merge(MutableColumns & merged_columns, TSortingHeap & queue);

    Poco::Logger * log;

    /// Read is finished.
    bool finished = false;
};

}
