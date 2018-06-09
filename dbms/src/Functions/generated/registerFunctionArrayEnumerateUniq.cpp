#include <Functions/registerFunctionArrayEnumerateUniq.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayEnumerateUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateUniq>();
}

}
