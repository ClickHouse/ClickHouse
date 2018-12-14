#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  */
struct ArrayMapImpl
{
    /// true if the expression (for an overload of f(expression, arrays)) or an array (for f(array)) should be boolean.
    static bool needBoolean() { return false; }
    /// true if the f(array) overload is unavailable.
    static bool needExpression() { return true; }
    /// true if the array must be exactly one.
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeArray>(expression_return);
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        return mapped->isColumnConst()
            ? ColumnArray::create(mapped->convertToFullColumnIfConst(), array.getOffsetsPtr())
            : ColumnArray::create(mapped, array.getOffsetsPtr());
    }
};

struct NameArrayMap { static constexpr auto name = "arrayMap"; };
using FunctionArrayMap = FunctionArrayMapped<ArrayMapImpl, NameArrayMap>;

void registerFunctionArrayMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayMap>();
}

}


