#include <Functions/registerFunctionArrayEnumerate.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayEnumerate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerate>();
}

}
