#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{

/** arrayAll(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for all elements of the array.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayAllImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped);
};

struct NameArrayAll { static constexpr auto name = "arrayAll"; };
using FunctionArrayAll = FunctionArrayMapped<ArrayAllImpl, NameArrayAll>;

}
