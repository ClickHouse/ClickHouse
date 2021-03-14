#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, init_accum) - apply the expression to each element of the array (or set of parallel arrays).
  */
struct ArrayFoldImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }
    static bool isFolding() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & accum_type)
    {
        return accum_type;
    }

    static ColumnPtr execute(const ColumnArray & /*array*/, ColumnPtr mapped)
    {
        return (mapped->size() == 0) ? mapped : mapped->cut(0, 1);
    }
};

struct NameArrayFold { static constexpr auto name = "arrayFold"; };
using FunctionArrayFold = FunctionArrayMapped<ArrayFoldImpl, NameArrayFold>;

void registerFunctionArrayFold(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFold>();
}

}


