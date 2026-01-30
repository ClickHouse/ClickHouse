#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/PODArray.h>
#include <Columns/IColumn.h>

#include <Common/logger_useful.h>

namespace DB
{

/** Shuffles each block individually
  */
class PartialShufflingTransform : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can shuffle each block not completely, but get only `limit` first rows
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
    // PaddedPODArray<UInt64> rows_to_compare;
    // PaddedPODArray<Int8> compare_results;
    // IColumn::Filter filter;
};

}
