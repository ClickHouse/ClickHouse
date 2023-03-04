#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;

REGISTER_FUNCTION(LessOrEquals)
{
    factory.registerFunction<FunctionLessOrEquals>();
}

template <>
ColumnPtr FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    return executeTupleLessGreaterImpl(
        FunctionFactory::instance().get("less", getContext()),
        FunctionFactory::instance().get("lessOrEquals", getContext()),
        FunctionFactory::instance().get("and", getContext()),
        FunctionFactory::instance().get("or", getContext()),
        FunctionFactory::instance().get("equals", getContext()),
        x, y, tuple_size, input_rows_count);
}

}
