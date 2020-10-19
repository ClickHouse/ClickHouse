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
void FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(ColumnsWithTypeAndName & columns, size_t result, const ColumnsWithTypeAndName & x,
                                                                const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                size_t input_rows_count) const
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("equals", context),
        FunctionFactory::instance().get("and", context),
        columns, result, x, y, tuple_size, input_rows_count);
}

}
