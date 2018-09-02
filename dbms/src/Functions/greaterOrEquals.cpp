#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;

void registerFunctionGreaterOrEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreaterOrEquals>();
}

template <>
void FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                                  const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                                  size_t input_rows_count)
{
    return executeTupleLessGreaterImpl<
            FunctionComparison<GreaterOp, NameGreater>,
            FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>>(block, result, x, y, tuple_size, input_rows_count);
}

}
