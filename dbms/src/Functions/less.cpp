#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionLess = FunctionComparison<LessOp, NameLess>;

void registerFunctionLess(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLess>();
}

template <>
void FunctionComparison<LessOp, NameLess>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                            const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                            size_t input_rows_count)
{
    return executeTupleLessGreaterImpl<FunctionLess, FunctionLess>(block, result, x, y, tuple_size, input_rows_count);
}

}
