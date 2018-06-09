#include <Functions/registerFunctionArray.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArray>();
}

}
