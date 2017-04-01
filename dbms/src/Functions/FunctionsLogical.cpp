#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

void registerFunctionsLogical(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAnd>();
    factory.registerFunction<FunctionOr>();
    factory.registerFunction<FunctionXor>();
    factory.registerFunction<FunctionNot>();
}

}
