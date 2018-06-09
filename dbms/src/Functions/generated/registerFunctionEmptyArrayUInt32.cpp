#include <Functions/registerFunctionEmptyArrayUInt32.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayUInt32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt32>();
}

}
