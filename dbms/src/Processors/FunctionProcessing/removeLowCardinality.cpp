#include <Processors/FunctionProcessing/removeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

DataTypePtr recursiveRemoveLowCardinality(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(recursiveRemoveLowCardinality(array_type->getNestedType()));

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        for (auto & element : elements)
            element = recursiveRemoveLowCardinality(element);

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames());
        else
            return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();

    return type;
}

ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
        return ColumnArray::create(recursiveRemoveLowCardinality(column_array->getDataPtr()), column_array->getOffsetsPtr());

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(recursiveRemoveLowCardinality(column_const->getDataColumnPtr()), column_const->size());

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        Columns columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveRemoveLowCardinality(element);
        return ColumnTuple::create(columns);
    }

    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return column_low_cardinality->convertToFullColumn();

    return column;
}

}
