#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;

void registerFunctionLessOrEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLessOrEquals>();
}

template <>
void FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                            const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                            size_t input_rows_count)
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<LessOp, NameLess>,
        FunctionLessOrEquals>(block, result, x, y, tuple_size, input_rows_count);
}

}
