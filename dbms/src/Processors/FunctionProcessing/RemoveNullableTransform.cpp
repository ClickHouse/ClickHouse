#include <Processors/FunctionProcessing/RemoveNullableTransform.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

void RemoveNullableTransform::mergeNullMaps(ColumnPtr & merge_to, const ColumnNullable & merge_from)
{
    if (!merge_to)
        merge_to = merge_from.getNullMapColumnPtr();
    else
    {
        auto mutable_merge_to_null_map_column = (*std::move(merge_to)).mutate();

        NullMap & result_null_map = static_cast<ColumnUInt8 &>(*mutable_merge_to_null_map_column).getData();
        const NullMap & src_null_map = merge_from.getNullMapData();

        for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
            if (src_null_map[i])
                result_null_map[i] = 1;

        merge_to = std::move(mutable_merge_to_null_map_column);
    }
}

static Blocks removeNullable(Block && block, const ColumnNumbers & column_numbers, size_t result)
{
    Block null_maps;

    ColumnPtr result_null_map_column;
    bool has_const_null_arg = false;

    auto removeByPosition = [&](size_t position)
    {
        auto & col = block.getByPosition(position);

        if (!col.column)
        {
            col.type = removeNullable(col.type);
            null_maps.insert({nullptr, nullptr, col.name});
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
                has_const_null_arg = has_const_null_arg || col_const->onlyNull();
            }

            if (auto * col_nullable = checkAndGetColumn<ColumnNullable>(column))
            {
                null_maps.insert({col_nullable->getNullMapColumnPtr(), nullptr, col.name});

                col.column = col_nullable->getNestedColumnPtr();
                col.type = nullable_type->getNestedType();

                if (is_const)
                    col.column = ColumnConst::create(col.column, col_size);
                else if (!has_const_null_arg)
                    RemoveNullableTransform::mergeNullMaps(result_null_map_column, *col_nullable);
            }
            else
                throw Exception("Incompatible column " + col.column->getName() +
                                " for nullable type " + col.type->getName(), ErrorCodes::LOGICAL_ERROR);
        }
        else
            null_maps.insert({nullptr, nullptr, col.name});
    };

    for (auto number : column_numbers)
        removeByPosition(number);
    removeByPosition(result);

    auto & result_null_map = null_maps.getByPosition(column_numbers.size());
    if (has_const_null_arg)
        result_null_map.column = DataTypeUInt8().createColumnConst(block.getNumRows(), 1);
    else
        result_null_map.column = result_null_map_column;

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
