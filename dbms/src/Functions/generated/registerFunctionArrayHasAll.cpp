#include <Functions/registerFunctionArrayHasAll.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayHasAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
