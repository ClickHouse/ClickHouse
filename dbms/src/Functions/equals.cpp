#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

void registerFunctionEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEquals>();
}

template <>
void FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                size_t input_rows_count)
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("equals", context),
        FunctionFactory::instance().get("and", context),
        block, result, x, y, tuple_size, input_rows_count);
}

}
