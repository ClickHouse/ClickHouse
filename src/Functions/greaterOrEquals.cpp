#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;

REGISTER_FUNCTION(GreaterOrEquals)
{
    factory.registerFunction<FunctionGreaterOrEquals>();
}

template <>
ColumnPtr FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    return executeTupleLessGreaterImpl(
        FunctionFactory::instance().get("greater", getContext()),
        FunctionFactory::instance().get("greaterOrEquals", getContext()),
        FunctionFactory::instance().get("and", getContext()),
        FunctionFactory::instance().get("or", getContext()),
        FunctionFactory::instance().get("equals", getContext()),
        x, y, tuple_size, input_rows_count);
}

}
