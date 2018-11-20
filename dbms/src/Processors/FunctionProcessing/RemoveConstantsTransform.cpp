#include <Processors/FunctionProcessing/RemoveConstantsTransform.h>

#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>

namespace DB
{

static Block RemoveConstantsTransform::removeConstants(
    Block && block,
    const ColumnNumbers & remain_constants,
    const ColumnNumbers & column_numbers,
    size_t /*result*/)
{
    for (auto number : column_numbers)
    {
        ColumnWithTypeAndName & col = block.getByPosition(number);

        if (!col.column)
            continue;

        bool remain_constant = remain_constants.end()
                               != std::find(remain_constants.begin(), remain_constants.end(), number);

        if (!remain_constant)
        {
            if (auto * col_const = checkAndGetColumn<ColumnConst>(col.column.get()))
                col.column = col_const->getDataColumnPtr();
            else
                throw Exception("ColumnConst expected for RemoveConstantsTransform, got " + col.column->getName(),
                                ErrorCodes::LOGICAL_ERROR);
        }
    }

    return std::move(block);
}

RemoveConstantsTransform::RemoveConstantsTransform(
    Block input_header,
    const ColumnNumbers & arguments_to_remain_constants,
    const ColumnNumbers & column_numbers,
    size_t result)
    : ISimpleTransform(input_header, removeConstants(Block(input_header), arguments_to_remain_constants, column_numbers, result))
    , arguments_to_remain_constants(arguments_to_remain_constants)
    , column_numbers(column_numbers)
    , result(result)
{
}

void RemoveConstantsTransform::transform(Block & block)
{
    block = removeConstants(std::move(block), arguments_to_remain_constants, column_numbers, result);
}

}
