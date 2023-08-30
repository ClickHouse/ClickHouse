#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>



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
    FunctionOverloadResolverPtr func_builder_not_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionNotEquals>(check_decimal_overflow));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    return executeTupleEqualityImpl(
        func_builder_not_equals,
        func_builder_or,
        x, y, tuple_size, input_rows_count);
}

}
