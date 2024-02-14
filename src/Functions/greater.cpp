#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(Greater)
{
    factory.registerFunction<FunctionGreater>();
}

template <>
ColumnPtr FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr greater
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGreater>(check_decimal_overflow));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(check_decimal_overflow));

    return executeTupleLessGreaterImpl(
        greater,
        greater,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
