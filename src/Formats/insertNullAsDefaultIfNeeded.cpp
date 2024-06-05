#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

bool insertNullAsDefaultIfNeeded(ColumnWithTypeAndName & input_column, const ColumnWithTypeAndName & header_column, size_t column_i, BlockMissingValues * block_missing_values)
{
    if (isArray(input_column.type) && isArray(header_column.type))
    {
        ColumnWithTypeAndName nested_input_column;
        const auto & array_input_column = checkAndGetColumn<ColumnArray>(*input_column.column);
        nested_input_column.column = array_input_column.getDataPtr();
        nested_input_column.type = checkAndGetDataType<DataTypeArray>(input_column.type.get())->getNestedType();

        ColumnWithTypeAndName nested_header_column;
        nested_header_column.column = checkAndGetColumn<ColumnArray>(*header_column.column).getDataPtr();
        nested_header_column.type = checkAndGetDataType<DataTypeArray>(header_column.type.get())->getNestedType();

        if (!insertNullAsDefaultIfNeeded(nested_input_column, nested_header_column, 0, nullptr))
            return false;

        input_column.column = ColumnArray::create(nested_input_column.column, array_input_column.getOffsetsPtr());
        input_column.type = std::make_shared<DataTypeArray>(std::move(nested_input_column.type));
        return true;
    }

    if (isTuple(input_column.type) && isTuple(header_column.type))
    {
        const auto & tuple_input_column = checkAndGetColumn<ColumnTuple>(*input_column.column);
        const auto & tuple_input_type = checkAndGetDataType<DataTypeTuple>(*input_column.type);
        const auto & tuple_header_column = checkAndGetColumn<ColumnTuple>(*header_column.column);
        const auto & tuple_header_type = checkAndGetDataType<DataTypeTuple>(*header_column.type);

        if (tuple_input_type.getElements().size() != tuple_header_type.getElements().size())
            return false;

        Columns nested_input_columns;
        nested_input_columns.reserve(tuple_input_type.getElements().size());
        DataTypes nested_input_types;
        nested_input_types.reserve(tuple_input_type.getElements().size());
        bool changed = false;
        for (size_t i = 0; i != tuple_input_type.getElements().size(); ++i)
        {
            ColumnWithTypeAndName nested_input_column;
            nested_input_column.column = tuple_input_column.getColumnPtr(i);
            nested_input_column.type = tuple_input_type.getElement(i);
            ColumnWithTypeAndName nested_header_column;
            nested_header_column.column = tuple_header_column.getColumnPtr(i);
            nested_header_column.type = tuple_header_type.getElement(i);
            changed |= insertNullAsDefaultIfNeeded(nested_input_column, nested_header_column, 0, nullptr);
            nested_input_columns.push_back(std::move(nested_input_column.column));
            nested_input_types.push_back(std::move(nested_input_column.type));
        }

        if (!changed)
            return false;

        input_column.column = ColumnTuple::create(std::move(nested_input_columns));
        input_column.type = std::make_shared<DataTypeTuple>(std::move(nested_input_types));
        return true;
    }

    if (isMap(input_column.type) && isMap(header_column.type))
    {
        ColumnWithTypeAndName nested_input_column;
        nested_input_column.column = checkAndGetColumn<ColumnMap>(*input_column.column).getNestedColumnPtr();
        nested_input_column.type = checkAndGetDataType<DataTypeMap>(*input_column.type).getNestedType();

        ColumnWithTypeAndName nested_header_column;
        nested_header_column.column = checkAndGetColumn<ColumnMap>(*header_column.column).getNestedColumnPtr();
        nested_header_column.type = checkAndGetDataType<DataTypeMap>(*header_column.type).getNestedType();

        if (!insertNullAsDefaultIfNeeded(nested_input_column, nested_header_column, 0, nullptr))
            return false;

        input_column.column = ColumnMap::create(std::move(nested_input_column.column));
        input_column.type = std::make_shared<DataTypeMap>(std::move(nested_input_column.type));
        return true;
    }

    if (!isNullableOrLowCardinalityNullable(input_column.type) || isNullableOrLowCardinalityNullable(header_column.type))
        return false;

    if (block_missing_values)
    {
        for (size_t i = 0; i < input_column.column->size(); ++i)
        {
            if (input_column.column->isNullAt(i))
                block_missing_values->setBit(column_i, i);
        }
    }

    if (input_column.type->isNullable())
    {
        input_column.column = assert_cast<const ColumnNullable *>(input_column.column.get())->getNestedColumnWithDefaultOnNull();
        input_column.type = removeNullable(input_column.type);
    }
    else
    {
        input_column.column = assert_cast<const ColumnLowCardinality *>(input_column.column.get())->cloneWithDefaultOnNull();
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(input_column.type.get());
        input_column.type = std::make_shared<DataTypeLowCardinality>(removeNullable(lc_type->getDictionaryType()));
    }

    return true;
}

}
