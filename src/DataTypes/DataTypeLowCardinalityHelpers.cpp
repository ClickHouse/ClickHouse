#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnFunction.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>

#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
}

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
        return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        return std::make_shared<DataTypeMap>(recursiveRemoveLowCardinality(map_type->getKeyType()), recursiveRemoveLowCardinality(map_type->getValueType()));
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();

    return type;
}

ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    ColumnPtr res = column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        const auto & data = column_array->getDataPtr();
        auto data_no_lc = recursiveRemoveLowCardinality(data);
        if (data.get() != data_no_lc.get())
            res = ColumnArray::create(data_no_lc, column_array->getOffsetsPtr());
    }
    else if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveRemoveLowCardinality(nested);
        if (nested.get() != nested_no_lc.get())
            res = ColumnConst::create(nested_no_lc, column_const->size());
    }
    else if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        if (columns.empty())
            return column;

        for (auto & element : columns)
            element = recursiveRemoveLowCardinality(element);
        res = ColumnTuple::create(columns);
    }
    else if (const auto * column_map = typeid_cast<const ColumnMap *>(column.get()))
    {
        const auto & nested = column_map->getNestedColumnPtr();
        auto nested_no_lc = recursiveRemoveLowCardinality(nested);
        if (nested.get() != nested_no_lc.get())
            res = ColumnMap::create(nested_no_lc);
    }
    /// Special case when column is a lazy argument of short circuit function.
    /// We should call recursiveRemoveLowCardinality on the result column
    /// when function will be executed.
    else if (const auto * column_function = typeid_cast<const ColumnFunction *>(column.get()))
    {
        if (column_function->isShortCircuitArgument())
            res = column_function->recursivelyConvertResultToFullColumnIfLowCardinality();
    }
    else if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        res = column_low_cardinality->convertToFullColumn();
    }

    if (res != column)
    {
        /// recursiveRemoveLowCardinality() must not change the size of a passed column!
        if (res->size() != column->size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "recursiveRemoveLowCardinality() somehow changed the size of column {}. Old size={}, new size={}. It's a bug",
                            column->getName(), column->size(), res->size());
        }
    }

    return res;
}

ColumnPtr recursiveLowCardinalityTypeConversion(const ColumnPtr & column, const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    if (!column)
        return column;

    if (from_type->equals(*to_type))
        return column;

    /// We can allow insert enum column if it's numeric type is the same as the column's type in table.
    if (WhichDataType(to_type).isEnum() && from_type->getTypeId() == to_type->getTypeId())
        return column;

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveLowCardinalityTypeConversion(nested, from_type, to_type);
        if (nested.get() == nested_no_lc.get())
            return column;

        return ColumnConst::create(nested_no_lc, column_const->size());
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(from_type.get()))
    {
        if (to_type->equals(*low_cardinality_type->getDictionaryType()))
            return column->convertToFullColumnIfLowCardinality();
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(to_type.get()))
    {
        if (from_type->equals(*low_cardinality_type->getDictionaryType()))
        {
            auto col = low_cardinality_type->createColumn();
            assert_cast<ColumnLowCardinality &>(*col).insertRangeFromFullColumn(*column, 0, column->size());
            return col;
        }
    }

    if (const auto * from_array_type = typeid_cast<const DataTypeArray *>(from_type.get()))
    {
        if (const auto * to_array_type = typeid_cast<const DataTypeArray *>(to_type.get()))
        {
            const auto * column_array = typeid_cast<const ColumnArray *>(column.get());
            if (!column_array)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column {} for type {}",
                                column->getName(), from_type->getName());

            const auto & nested_from = from_array_type->getNestedType();
            const auto & nested_to = to_array_type->getNestedType();

            return ColumnArray::create(
                    recursiveLowCardinalityTypeConversion(column_array->getDataPtr(), nested_from, nested_to),
                    column_array->getOffsetsPtr());
        }
    }

    if (const auto * from_tuple_type = typeid_cast<const DataTypeTuple *>(from_type.get()))
    {
        if (const auto * to_tuple_type = typeid_cast<const DataTypeTuple *>(to_type.get()))
        {
            const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get());
            if (!column_tuple)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column {} for type {}",
                                column->getName(), from_type->getName());

            auto columns = column_tuple->getColumns();
            const auto & from_elements = from_tuple_type->getElements();
            const auto & to_elements = to_tuple_type->getElements();

            bool has_converted = false;

            for (size_t i = 0; i < columns.size(); ++i)
            {
                auto & element = columns[i];
                auto element_no_lc = recursiveLowCardinalityTypeConversion(element, from_elements.at(i), to_elements.at(i));
                if (element.get() != element_no_lc.get())
                {
                    element = element_no_lc;
                    has_converted = true;
                }
            }

            if (!has_converted)
                return column;

            return ColumnTuple::create(columns);
        }
    }

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert: {} to {}", from_type->getName(), to_type->getName());
}

}
