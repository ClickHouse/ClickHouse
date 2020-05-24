#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Common/SharedBlockRowRef.h>
#include <Core/SortDescription.h>


namespace DB
{
class OptimizedPartialSortingTransform : public ISimpleTransform
{
public:
    OptimizedPartialSortingTransform(
        const Block & header_,
        SortDescription & description_,
        UInt64 limit_ = 0);

    String getName() const override { return "OptimizedPartialSortingTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) { read_rows.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    SortDescription description;
    UInt64 limit;
    RowsBeforeLimitCounterPtr read_rows;
    SharedBlockRowWithSortDescriptionRef threshold_row;
    SharedBlockPtr threshold_shared_block;
    ColumnRawPtrs threshold_block_columns;
};

}
