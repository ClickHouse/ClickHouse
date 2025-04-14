#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(Equals)
{
    factory.registerFunction<FunctionEquals>();
}

template <>
ColumnPtr FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(check_decimal_overflow));


    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    return executeTupleEqualityImpl(
        func_builder_equals,
        func_builder_and,
        x, y, tuple_size, input_rows_count);
}

}
