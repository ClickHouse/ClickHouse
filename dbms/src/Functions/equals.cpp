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
void FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                size_t input_rows_count)
{
    return executeTupleEqualityImpl<FunctionEquals, FunctionAnd>(block, result, x, y, tuple_size, input_rows_count);
}

}
