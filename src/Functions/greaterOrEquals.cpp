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
                                                                                  size_t input_rows_count) const
{
    return executeTupleLessGreaterImpl(
        FunctionFactory::instance().get("greater", context),
        FunctionFactory::instance().get("greaterOrEquals", context),
        FunctionFactory::instance().get("and", context),
        FunctionFactory::instance().get("or", context),
        FunctionFactory::instance().get("equals", context),
        block, result, x, y, tuple_size, input_rows_count);
}

}
