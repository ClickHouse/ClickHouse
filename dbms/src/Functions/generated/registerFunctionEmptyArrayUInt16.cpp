#include <Functions/registerFunctionEmptyArrayUInt16.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayUInt16(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt16>();
}

}
