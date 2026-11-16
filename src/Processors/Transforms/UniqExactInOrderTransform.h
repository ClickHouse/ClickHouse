#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/IColumn.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Processors/IAccumulatingTransform.h>


namespace DB
{

/// Counts the distinct non-NULL values of a column whose equal values are adjacent. Used for a single `uniqExact`
/// without `GROUP BY` when the input is read in order of its argument. Sorting groups 0.0 with -0.0 and all NaNs,
/// which `uniqExact` tells apart by their bits, so the rows of these runs are passed to the `uniqExact` itself.
class UniqExactInOrderTransform final : public IAccumulatingTransform
{
public:
    UniqExactInOrderTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        size_t key_position_,
        int direction_,
        int nulls_direction_,
        AggregateFunctionPtr function_,
        bool empty_result_for_empty_set_);

    ~UniqExactInOrderTransform() override;

    [[nodiscard]] String getName() const override { return "UniqExactInOrderTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    const size_t key_position;
    const int direction;
    const int nulls_direction;
    const AggregateFunctionPtr function;
    const bool is_float;
    const bool empty_result_for_empty_set;
    Arena arena;
    AlignedBuffer state;
    ColumnPtr last;
    UInt64 count = 0;
    bool has_rows = false;
    bool generated = false;
};

/// Adds up the counts of `UniqExactInOrderTransform` over streams that have no value in common.
class UniqExactInOrderSumTransform final : public IAccumulatingTransform
{
public:
    UniqExactInOrderSumTransform(SharedHeader header, bool empty_result_for_empty_set_);

    [[nodiscard]] String getName() const override { return "UniqExactInOrderSumTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    const bool empty_result_for_empty_set;
    UInt64 total = 0;
    bool has_rows = false;
    bool generated = false;
};

}
