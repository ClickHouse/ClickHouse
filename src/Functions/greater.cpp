#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;

REGISTER_FUNCTION(Greater)
{
    factory.registerFunction<FunctionGreater>();
}

template <>
ColumnPtr FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    auto greater = FunctionFactory::instance().get("greater", getContext());

    return executeTupleLessGreaterImpl(
        greater,
        greater,
        FunctionFactory::instance().get("and", getContext()),
        FunctionFactory::instance().get("or", getContext()),
        FunctionFactory::instance().get("equals", getContext()),
        x, y, tuple_size, input_rows_count);
}

}
