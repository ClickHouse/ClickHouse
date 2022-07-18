#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Core/SortDescription.h>
#include <Common/PODArray.h>

namespace DB
{

/** Sorts each block individually by the values of the specified columns.
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
    SortDescriptionWithPositions description_with_positions;
    UInt64 limit;
    RowsBeforeLimitCounterPtr read_rows;

    Columns sort_description_threshold_columns;

    /// This are just buffers which reserve memory to reduce the number of allocations.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    IColumn::Filter filter;

    /// If limit < min_limit_for_partial_sort_optimization, skip optimization with threshold_block.
    /// Because for small LIMIT partial sorting may be very faster then additional work
    /// which is made if optimization is enabled (comparison with threshold, filtering).
    static constexpr size_t min_limit_for_partial_sort_optimization = 1500;
};

}
