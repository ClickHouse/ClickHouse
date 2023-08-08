#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include "FunctionArrayMapped.h"


namespace DB
{/** arrayRemove(x -> predicate,array) - Returns a pointer to the modified array after removing the elements that passed the predicate condition.
  */
struct ArrayRemoveImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }
    // static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped);
    template <typename T>
    static ColumnPtr execute(const ColumnArray &array, T element);
};

struct NameArrayFilter { static constexpr auto name = "arrayRemove"; };
using FunctionArrayRemove = FunctionArrayMapped<ArrayRemoveImpl, NameArrayFilter>;
}
