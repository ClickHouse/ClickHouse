#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;

void registerFunctionGreater(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreater>();
}

template <>
void FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                  const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                  size_t input_rows_count)
{
    return executeTupleLessGreaterImpl<FunctionGreater, FunctionGreater>(block, result, x, y, tuple_size, input_rows_count);
}

}
