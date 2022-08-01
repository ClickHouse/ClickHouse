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
    auto greater = FunctionFactory::instance().get("greater", context);

    return executeTupleLessGreaterImpl(
        greater,
        greater,
        FunctionFactory::instance().get("and", context),
        FunctionFactory::instance().get("or", context),
        FunctionFactory::instance().get("equals", context),
        x, y, tuple_size, input_rows_count);
}

}
