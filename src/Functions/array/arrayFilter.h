#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{

/** arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  */
struct ArrayFilterImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    /// If there are several arrays, the first one is passed here.
    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped);
};

struct NameArrayFilter { static constexpr auto name = "arrayFilter"; };
using FunctionArrayFilter = FunctionArrayMapped<ArrayFilterImpl, NameArrayFilter>;

}
