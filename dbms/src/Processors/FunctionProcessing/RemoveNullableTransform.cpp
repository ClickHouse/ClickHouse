#include <Processors/FunctionProcessing/RemoveNullableTransform.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

static Blocks removeNullable(Block && block, const ColumnNumbers & column_numbers, size_t result)
{
    Block null_maps;
    auto map_type = std::make_shared<DataTypeUInt8>();
    ColumnPtr map_col = ColumnUInt8::create();

    auto removeByPositions = [&](size_t positions)
    {
        auto & col = block.getByPosition(positions);

        if (!col.column)
        {
            col.type = removeNullable(col.type);
            null_maps.insert({map_col, map_type, col.name});
        }
        else if (auto * nullable_type = checkAndGetDataType<DataTypeNullable>(col.type.get()))
        {
            auto * column = col.column.get();
            size_t col_size = column->size();
            bool is_const = false;

            if (auto * col_const = checkAndGetColumn<ColumnConst>(column))
            {
                column = &col_const->getDataColumn();
                is_const = true;
            }

            if (auto * col_nullable = checkAndGetColumn<ColumnNullable>(column))
            {
                null_maps.insert({col_nullable->getNestedColumnPtr(), map_type, col.name});

                col.column = col_nullable->getNestedColumnPtr();
                col.type = nullable_type->getNestedType();

                if (is_const)
                    col.column = ColumnConst::create(col.column, col_size);
            }
            else
                throw Exception("Incompatible column " + col.column->getName() +
                                " for nullable type " + col.type->getName(), ErrorCodes::LOGICAL_ERROR);
        }
        else
            null_maps.insert({nullptr, map_type, col.name});
    };

    for (auto number : column_numbers)
        removeByPositions(number);
    removeByPositions(result);

    return {std::move(block), std::move(null_maps)};
}

RemoveNullableTransform::RemoveNullableTransform(Block input_header, const ColumnNumbers & column_numbers, size_t result)
    : ITransform({input_header}, removeNullable(Block(input_header.getColumnsWithTypeAndName()), column_numbers, result))
    , column_numbers(column_numbers)
    , result(result)
{
}

Blocks RemoveNullableTransform::transform(Blocks && blocks)
{
    return removeNullable(std::move(blocks.at(0)), column_numbers, result);
}

}
