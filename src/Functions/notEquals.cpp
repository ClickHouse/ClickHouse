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
void FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                                      const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                      size_t input_rows_count)
{
    return executeTupleEqualityImpl(
        FunctionFactory::instance().get("notEquals", context),
        FunctionFactory::instance().get("or", context),
        block, result, x, y, tuple_size, input_rows_count);
}

}
