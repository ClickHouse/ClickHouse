#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

void registerFunctionNotEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotEquals>();
}

template <>
void FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(ColumnsWithTypeAndName & columns, size_t result, const ColumnsWithTypeAndName & x,
                                                                      const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                      size_t input_rows_count) const
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("notEquals", context),
        FunctionFactory::instance().get("or", context),
        columns, result, x, y, tuple_size, input_rows_count);
}

}
