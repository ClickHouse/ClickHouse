#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Higher-order functions for map.
  * These functions optionally apply a map by lambda function,
  *  and return some result based on that transformation.
  */


/** mapFilter((k, v) -> predicate, map) - leave in the map only the kv elements for which the expression is true.
  */
struct MapFilterImpl
{
    using data_type = DataTypeMap;
    using column_type = ColumnMap;

    static constexpr auto name = "mapFilter";

    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }

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
            {
                const auto * column_array = typeid_cast<const ColumnArray *>(map_column.getNestedColumnPtr().get());
                const auto * column_tuple = typeid_cast<const ColumnTuple *>(column_array->getDataPtr().get());
                ColumnPtr keys = column_tuple->getColumnPtr(0)->cloneEmpty();
                ColumnPtr values = column_tuple->getColumnPtr(1)->cloneEmpty();
                return ColumnMap::create(keys, values, ColumnArray::ColumnOffsets::create(map_column.size(), 0));
            }
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


/** mapApply((k,v) -> expression, map) - apply the expression to the map.
  */
struct MapApplyImpl
{
    using data_type = DataTypeMap;
    using column_type = ColumnMap;

    static constexpr auto name = "mapApply";

    /// true if the expression (for an overload of f(expression, maps)) or a map (for f(map)) should be boolean.
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypes & /*elems*/)
    {
        const auto * tuple_types = typeid_cast<const DataTypeTuple *>(expression_return.get());
        if (!tuple_types)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expected return type is tuple, got {}", expression_return->getName());
        if (tuple_types->getElements().size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Expected 2 columns as map's key and value, but found {}", tuple_types->getElements().size());

        return std::make_shared<DataTypeMap>(tuple_types->getElements());
    }

    static ColumnPtr execute(const ColumnMap & map, ColumnPtr mapped)
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(mapped.get());
        if (!column_tuple)
        {
            const ColumnConst * column_const_tuple = checkAndGetColumnConst<ColumnTuple>(mapped.get());
            if (!column_const_tuple)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected tuple column, found {}", mapped->getName());
            auto cols = convertConstTupleToConstantElements(*column_const_tuple);
            return ColumnMap::create(cols[0]->convertToFullColumnIfConst(), cols[1]->convertToFullColumnIfConst(), map.getNestedColumn().getOffsetsPtr());
        }

        return ColumnMap::create(column_tuple->getColumnPtr(0), column_tuple->getColumnPtr(1),
            map.getNestedColumn().getOffsetsPtr());
    }
};

REGISTER_FUNCTION(MapApply)
{
    factory.registerFunction<FunctionArrayMapped<MapFilterImpl, MapFilterImpl>>();
    factory.registerFunction<FunctionArrayMapped<MapApplyImpl, MapApplyImpl>>();
}

}


