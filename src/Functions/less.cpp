#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>


namespace DB
{

using FunctionLess = FunctionComparison<LessOp, NameLess>;

void registerFunctionLess(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLess>();
}

template <>
void FunctionComparison<LessOp, NameLess>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                                            const ColumnsWithTypeAndName & y, size_t tuple_size,
                                                            size_t input_rows_count) const
{
    auto less = FunctionFactory::instance().get("less", context);

    return executeTupleLessGreaterImpl(
        less,
        less,
        FunctionFactory::instance().get("and", context),
        FunctionFactory::instance().get("or", context),
        FunctionFactory::instance().get("equals", context),
        block, result, x, y, tuple_size, input_rows_count);
}

}
