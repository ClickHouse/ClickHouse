#include <Functions/registerFunctionEmptyArrayUInt8.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayUInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt8>();
}

}
