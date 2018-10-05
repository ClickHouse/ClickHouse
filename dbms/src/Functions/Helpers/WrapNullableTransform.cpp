#include <Functions/Helpers/WrapNullableTransform.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

Block wrapNullable(Blocks && blocks, const ColumnNumbers & column_numbers, size_t result)
{
    auto & block = blocks.at(0);
    auto & null_maps = blocks.at(1);

    auto wrapByPosition = [&](size_t block_position, size_t null_map_position)
    {
        auto & col = block.getByPosition(block_position);
        auto & null_map = null_maps.getByPosition(null_map_position);

        if (null_map.column)
        {
            col.type = makeNullable(col.type);

            if (col.column)
            {
                auto * column = col.column.get();
                size_t col_size = column->size();
                bool is_const = false;

                if (auto * col_const = checkAndGetColumn<ColumnConst>(column))
                {
                    is_const = true;
                    column = &col_const->getDataColumn();
                }

                col.column = ColumnNullable::create(column->getPtr(), null_map.column);

                if (is_const)
                    col.column = ColumnConst::create(col.column, col_size);
            }
        }
    };

    size_t num_args = column_numbers.size();
    for (size_t i = 0; i < num_args; ++i)
        wrapByPosition(column_numbers[i], i);

    wrapByPosition(result, num_args);

    return block;
}

WrapNullableTransform::WrapNullableTransform(Blocks input_headers, const ColumnNumbers & column_numbers, size_t result)
    : ITransform(input_headers, {wrapNullable(Blocks(input_headers), column_numbers, result)})
    , column_numbers(column_numbers)
    , result(result)
{

}

Blocks WrapNullableTransform::transform(Blocks && blocks)
{
    return {wrapNullable(std::move(blocks), column_numbers, result)};
}

}
