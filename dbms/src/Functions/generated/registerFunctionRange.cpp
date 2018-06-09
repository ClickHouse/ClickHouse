#include <Functions/registerFunctionRange.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}
