#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>

#include <base/FnTraits.h>


namespace DB
{

template <typename FromType>
DataTypePtr recursiveConvertType(const DataTypePtr & type, Fn<DataTypePtr(const FromType &)> auto && func)
{
    if (!type)
        return type;

    if (const auto * from_type = typeid_cast<const FromType *>(type.get()))
        return func(*from_type);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(recursiveConvertType<FromType>(array_type->getNestedType(), func));

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        return std::make_shared<DataTypeMap>(recursiveConvertType<FromType>(map_type->getKeyType(), func), recursiveConvertType<FromType>(map_type->getValueType(), func));

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return std::make_shared<DataTypeLowCardinality>(recursiveConvertType<FromType>(low_cardinality_type->getDictionaryType(), func));

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return std::make_shared<DataTypeNullable>(recursiveConvertType<FromType>(nullable_type->getNestedType(), func));

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        for (auto & element : elements)
            element = recursiveConvertType<FromType>(element, func);

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames());
        else
            return std::make_shared<DataTypeTuple>(elements);
    }

    return type;
}

template <typename FromColumn>
ColumnPtr recursiveConvertColumn(const ColumnPtr & column, Fn<ColumnPtr(const FromColumn &)> auto && func)
{
    if (!column)
        return column;

    if (const auto * from_column = typeid_cast<const FromColumn *>(column.get()))
        return func(*from_column);

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        const auto & data = column_array->getDataPtr();
        auto converted = recursiveConvertColumn<FromColumn>(data, func);
        if (data.get() == converted.get())
            return column;

        return ColumnArray::create(converted, column_array->getOffsetsPtr());
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto converted = recursiveConvertColumn<FromColumn>(nested, func);
        if (nested.get() == converted.get())
            return column;

        return ColumnConst::create(converted, column_const->size());
    }

    if (const auto * column_map = typeid_cast<const ColumnMap *>(column.get()))
    {
        const auto & nested = column_map->getNestedColumnPtr();
        auto converted = recursiveConvertColumn<FromColumn>(nested, func);
        if (nested.get() == converted.get())
            return column;

        return ColumnMap::create(converted);
    }

    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
    {
        const auto & nested = column_nullable->getNestedColumnPtr();
        auto converted = recursiveConvertColumn<FromColumn>(nested, func);
        if (nested.get() == converted.get())
            return column;

        return ColumnNullable::create(converted, column_nullable->getNullMapColumnPtr());
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveConvertColumn<FromColumn>(element, func);
        return ColumnTuple::create(columns);
    }

    return column;
}

}
