#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

void registerFunctionEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEquals>();
}

template <>
ColumnPtr FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("equals", context),
        FunctionFactory::instance().get("and", context),
        x, y, tuple_size, input_rows_count);
}

}
