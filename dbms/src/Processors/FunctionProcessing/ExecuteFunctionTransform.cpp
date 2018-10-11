#include <Processors/FunctionProcessing/ExecuteFunctionTransform.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>

namespace DB
{

ExecuteFunctionTransform::ExecuteFunctionTransform(
    const PreparedFunctionPtr & function,
    Block input_header,
    const ColumnNumbers & column_numbers,
    size_t result)
    : ISimpleTransform(input_header, input_header)
    , prepared_function(function)
    , column_numbers(column_numbers)
    , result(result)
{
}

void ExecuteFunctionTransform::transform(Block & block)
{
    prepared_function->execute(block, column_numbers, result, block.getNumRows());
}

}
