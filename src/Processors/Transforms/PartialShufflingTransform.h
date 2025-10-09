#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/PODArray.h>

#include <Common/logger_useful.h>

namespace DB
{

/** Shuffles each block individually by the values of the specified columns.
  */
class PartialShufflingTransform : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can shuffle each block not completely, but only `limit` first rows by order.
    PartialShufflingTransform(
        SharedHeader header_,
        UInt64 limit_ = 0);

    String getName() const override { return "PartialShufflingTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { read_rows.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    const UInt64 limit;
    RowsBeforeStepCounterPtr read_rows;

    IColumn::Permutation getIdentityPermutation(size_t size);
    void shufflePermutation(IColumn::Permutation & permutation);
    void shuffleBlock(Block & block);

    /// This are just buffers which reserve memory to reduce the number of allocations.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    IColumn::Filter filter;

    /// If limit < min_limit_for_partial_sort_optimization, skip optimization with threshold_block.
    /// Because for small LIMIT partial Shuffling may be very faster then additional work
    /// which is made if optimization is enabled (comparison with threshold, filtering).
    static constexpr size_t min_limit_for_partial_sort_optimization = 1500;
};

}
