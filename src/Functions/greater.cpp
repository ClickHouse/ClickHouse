#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;

void registerFunctionGreater(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreater>();
}

template <>
void FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(ColumnsWithTypeAndName & columns, size_t result, const ColumnsWithTypeAndName & x,
                                                                  const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                                  size_t input_rows_count) const
{
    auto greater = FunctionFactory::instance().get("greater", context);

    return executeTupleLessGreaterImpl(
        greater,
        greater,
        FunctionFactory::instance().get("and", context),
        FunctionFactory::instance().get("or", context),
        FunctionFactory::instance().get("equals", context),
        columns, result, x, y, tuple_size, input_rows_count);
}

}
