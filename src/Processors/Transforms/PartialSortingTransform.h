#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Core/SortDescription.h>

namespace DB
{

/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class PartialSortingTransform : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingTransform(
        const Block & header_,
        SortDescription & description_,
        UInt64 limit_ = 0);

    String getName() const override { return "PartialSortingTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) { read_rows.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    SortDescription description;
    UInt64 limit;
    RowsBeforeLimitCounterPtr read_rows;
    Block threshold_block;
    ColumnRawPtrs threshold_block_columns;
};

}
