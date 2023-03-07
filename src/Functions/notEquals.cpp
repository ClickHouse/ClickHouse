#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

REGISTER_FUNCTION(NotEquals)
{
    factory.registerFunction<FunctionNotEquals>();
}

template <>
ColumnPtr FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("notEquals", getContext()),
        FunctionFactory::instance().get("or", getContext()),
        x, y, tuple_size, input_rows_count);
}

}
