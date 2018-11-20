#include <Processors/FunctionProcessing/ExecuteFunctionTransform.h>
#include <Processors/FunctionProcessing/RemoveConstantsTransform.h>
#include <Processors/FunctionProcessing/WrapConstantsTransform.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ExecuteFunctionTransform::ExecuteFunctionTransform(
    const PreparedFunctionPtr & function,
    Block input_header,
    const ColumnNumbers & column_numbers,
    size_t result,
    bool use_default_implementation_for_constants,
    const ColumnNumbers & remain_constants)
    : ISimpleTransform(input_header, input_header)
    , prepared_function(function)
    , column_numbers(column_numbers)
    , result(result)
    , use_default_implementation_for_constants(use_default_implementation_for_constants)
    , remain_constants(remain_constants)
{
}

static bool allArgumentsAreConstants(const Block & block, const ColumnNumbers & args)
{
    for (auto arg : args)
    {
        auto & column = block.getByPosition(arg).column;
        if (!column || !column->isColumnConst())
            return false;
    }
    return true;
}

static void checkArgumentsToRemainConstantsAreConstants(
        const Block & header,
        const ColumnNumbers & arguments,
        const ColumnNumbers & arguments_to_remain_constants,
        const String & function_name)
{
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < arguments.size() && !header.getByPosition(arguments[arg_num]).column->isColumnConst())
            throw Exception("Argument at index " + toString(arg_num) + " for function " + function_name
                            + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
}

void ExecuteFunctionTransform::transform(Block & block)
{
    checkArgumentsToRemainConstantsAreConstants(block, column_numbers, remain_constants, prepared_function->getName());

    bool remove_constants = !column_numbers.empty()
                            && use_default_implementation_for_constants
                            && allArgumentsAreConstants(block, column_numbers);

    size_t num_rows = block.getNumRows();

    if (remove_constants)
    {
        block = RemoveConstantsTransform::removeConstants(std::move(block), remain_constants, column_numbers, result);
        num_rows = 1;
    }

    prepared_function->execute(block, column_numbers, result, num_rows);

    if (remove_constants)
        block = WrapConstantsTransform::wrapConstants(std::move(block), column_numbers, result);
}

}
