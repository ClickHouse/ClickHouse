#include <Processors/FunctionProcessing/WrapConstantsTransform.h>

#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>

namespace DB
{

static Block wrapConstants(
    Block && block,
    const ColumnNumbers & column_numbers,
    size_t result)
{
    size_t num_rows = block.getNumRows();

    auto wrapByPosition = [&](size_t position)
    {
        ColumnWithTypeAndName & col = block.getByPosition(position);

        if (col.column && !col.column->isColumnConst())
        {
            col.column = ColumnConst::create(col.column, num_rows);
        }
    };

    size_t num_args = column_numbers.size();
    for (size_t i = 0; i < num_args; ++i)
        wrapByPosition(i);

    wrapByPosition(result);

    return block;
}

WrapConstantsTransform::WrapConstantsTransform(
    Block input_header,
    const ColumnNumbers & column_numbers,
    size_t result)
    : ISimpleTransform(input_header, wrapConstants(Block(input_header), column_numbers, result))
    , column_numbers(column_numbers)
    , result(result)
{
}

void WrapConstantsTransform::transform(Block & block)
{
    block = wrapConstants(std::move(block), column_numbers, result);
}

}
