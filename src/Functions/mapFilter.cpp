#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include "FunctionMapMapped.h"
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** MapFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  */
struct MapFilterImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneMap() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypes & elems)
    {
        return std::make_shared<DataTypeMap>(elems);
    }

    /// If there are several arrays, the first one is passed here.
    static ColumnPtr execute(const ColumnMap & map_column, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
                return map_column.clone();
            else
                return ColumnMap::create(
                    map_column.getNestedColumnPtr()->cloneEmpty());
        }

        const IColumn::Filter & filter = column_filter->getData();
        ColumnPtr filtered = map_column.getNestedColumn().getData().filter(filter, -1);

        const IColumn::Offsets & in_offsets = map_column.getNestedColumn().getOffsets();
        auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
        IColumn::Offsets & out_offsets = column_offsets->getData();

        size_t in_pos = 0;
        size_t out_pos = 0;
        for (size_t i = 0; i < in_offsets.size(); ++i)
        {
            for (; in_pos < in_offsets[i]; ++in_pos)
            {
                if (filter[in_pos])
                    ++out_pos;
            }
            out_offsets[i] = out_pos;
        }

        return ColumnMap::create(ColumnArray::create(filtered, std::move(column_offsets)));
    }
};

struct NameMapFilter { static constexpr auto name = "mapFilter"; };
using FunctionMapFilter = FunctionMapMapped<MapFilterImpl, NameMapFilter>;

void registerFunctionMapFilter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapFilter>();
}


/** mapMap((k,v) -> expression, map) - apply the expression to the map.
  */
struct MapMapImpl
{
    /// true if the expression (for an overload of f(expression, maps)) or a map (for f(map)) should be boolean.
    static bool needBoolean() { return false; }
    /// true if the f(map) overload is unavailable.
    static bool needExpression() { return true; }
    /// true if the map must be exactly one.
    static bool needOneMap() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & , const DataTypes & elems)
    {
        return std::make_shared<DataTypeMap>(elems);
    }

    static ColumnPtr execute(const ColumnMap & map, ColumnPtr mapped)
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(mapped.get());
        if (!column_tuple)
        {
            const ColumnConst * column_const_tuple = checkAndGetColumnConst<ColumnTuple>(mapped.get());
            if (!column_const_tuple)
                throw Exception("Expected tuple column, found " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
            ColumnPtr column_tuple_ptr = recursiveRemoveLowCardinality(column_const_tuple->convertToFullColumn());
            column_tuple = checkAndGetColumn<ColumnTuple>(column_tuple_ptr.get());
        }

        return ColumnMap::create(column_tuple->getColumnPtr(0), column_tuple->getColumnPtr(1),
            map.getNestedColumn().getOffsetsPtr());
    }
};

struct NameMapMap { static constexpr auto name = "mapMap"; };
using FunctionMapMap = FunctionMapMapped<MapMapImpl, NameMapMap>;

void registerFunctionMapMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapMap>();
}

}


