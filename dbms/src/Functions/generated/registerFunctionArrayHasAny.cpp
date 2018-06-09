#include <Functions/registerFunctionArrayHasAny.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
