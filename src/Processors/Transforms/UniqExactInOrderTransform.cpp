#include <Processors/Transforms/UniqExactInOrderTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <cmath>

namespace DB
{

UniqExactInOrderTransform::UniqExactInOrderTransform(
    SharedHeader input_header,
    SharedHeader output_header,
    size_t key_position_,
    int direction_,
    int nulls_direction_,
    AggregateFunctionPtr function_,
    bool empty_result_for_empty_set_)
    : IAccumulatingTransform(std::move(input_header), std::move(output_header))
    , key_position(key_position_)
    , direction(direction_)
    , nulls_direction(nulls_direction_)
    , function(std::move(function_))
    , is_float(isFloat(removeNullable(function->getArgumentTypes()[0])))
    , empty_result_for_empty_set(empty_result_for_empty_set_)
    , state(function->sizeOfData(), function->alignOfData())
{
    function->create(state.data());
}

UniqExactInOrderTransform::~UniqExactInOrderTransform()
{
    function->destroy(state.data());
}

void UniqExactInOrderTransform::consume(Chunk chunk)
{
    const size_t rows = chunk.getNumRows();
    if (rows == 0)
        return;
    has_rows = true;

    const ColumnPtr column = chunk.getColumns()[key_position]->convertToFullIfWrapped();
    /// The aggregate function takes the argument without `LowCardinality`, as in the ordinary aggregation.
    const ColumnPtr argument = is_float ? recursiveRemoveLowCardinality(column) : column;
    const IColumn * values = argument.get();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(values))
        values = &nullable->getNestedColumn();
    const IColumn * arguments[] = {argument.get()};

    const bool continues_last = last && column->compareAt(0, 0, *last, nulls_direction) == 0;
    size_t prev_begin = 0;
    for (size_t begin = 0; begin < rows;)
    {
        if (begin != 0)
            chassert(column->compareAt(begin, prev_begin, *column, nulls_direction) * direction > 0);

        const size_t end = column->getEqualRangeEndAssumeSorted(begin, rows, nulls_direction);
        if (!column->isNullAt(begin))
        {
            const Float64 value = is_float ? values->getFloat64(begin) : 1;
            if (value == 0 || std::isnan(value))
                function->addBatchSinglePlace(begin, end, state.data(), arguments, &arena);
            else if (begin != 0 || !continues_last)
                ++count;
        }
        prev_begin = begin;
        begin = end;
    }
    last = column->cut(rows - 1, 1);
}

Chunk UniqExactInOrderTransform::generate()
{
    if (generated)
        return {};
    generated = true;
    if (empty_result_for_empty_set && !has_rows)
        return {};

    auto special_floats = function->getResultType()->createColumn();
    function->insertResultInto(state.data(), *special_floats, &arena);
    auto result = ColumnUInt64::create();
    result->insertValue(count + special_floats->getUInt(0));
    return Chunk(Columns{std::move(result)}, 1);
}

UniqExactInOrderSumTransform::UniqExactInOrderSumTransform(SharedHeader header, bool empty_result_for_empty_set_)
    : IAccumulatingTransform(header, header)
    , empty_result_for_empty_set(empty_result_for_empty_set_)
{
}

void UniqExactInOrderSumTransform::consume(Chunk chunk)
{
    for (UInt64 value : assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front()).getData())
    {
        total += value;
        has_rows = true;
    }
}

Chunk UniqExactInOrderSumTransform::generate()
{
    if (generated)
        return {};
    generated = true;
    if (empty_result_for_empty_set && !has_rows)
        return {};
    auto result = ColumnUInt64::create();
    result->insertValue(total);
    return Chunk(Columns{std::move(result)}, 1);
}

}
