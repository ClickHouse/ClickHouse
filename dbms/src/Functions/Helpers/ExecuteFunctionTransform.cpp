#include <Functions/Helpers/ExecuteFunctionTransform.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>

namespace DB
{

static Block executeFunction(
    const PreparedFunctionPtr & function,
    Block && block,
    const ColumnNumbers & column_numbers,
    size_t result)
{
    function->execute(block, column_numbers, result, block.getNumRows());
    return block;
}

ExecuteFunctionTransform::ExecuteFunctionTransform(
    const PreparedFunctionPtr & function,
    Block input_header,
    const ColumnNumbers & column_numbers,
    size_t result)
    : ISimpleTransform(input_header, executeFunction(function, Block(input_header), column_numbers, result))
    , prepared_function(function)
    , column_numbers(column_numbers)
    , result(result)
{
}

void ExecuteFunctionTransform::transform(Block & block)
{
    block = executeFunction(prepared_function, std::move(block), column_numbers, result);
}

}
