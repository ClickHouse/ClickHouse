#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include "FunctionArrayMapped.h"


namespace DB
{
 /** arrayRemove(x -> predicate, array) - remove from the array only the elements for which the expression is true.
  */
struct ArrayRemoveImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    /// If there are several arrays, the first one is passed here.
    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped);
};

struct NameArrayFilter { static constexpr auto name = "arrayRemove"; };
using FunctionArrayRemove = FunctionArrayMapped<ArrayRemoveImpl, NameArrayFilter>;

}
