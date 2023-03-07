#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionLess = FunctionComparison<LessOp, NameLess>;

REGISTER_FUNCTION(Less)
{
    factory.registerFunction<FunctionLess>();
}

template <>
ColumnPtr FunctionComparison<LessOp, NameLess>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    auto less = FunctionFactory::instance().get("less", getContext());

    return executeTupleLessGreaterImpl(
        less,
        less,
        FunctionFactory::instance().get("and", getContext()),
        FunctionFactory::instance().get("or", getContext()),
        FunctionFactory::instance().get("equals", getContext()),
        x, y, tuple_size, input_rows_count);
}

}
