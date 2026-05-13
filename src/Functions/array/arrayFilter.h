#pragma once

#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{

/** arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  */
struct ArrayFilterImpl
{
    /// Documentation-only — keeps the first array's elements where the lambda
    /// returns truthy; element type of the result is the element type of the
    /// first input array.
    static constexpr auto signature = "(Function((Any, ...), UInt8), Array(T : Any), ...) -> Array(T)";

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
