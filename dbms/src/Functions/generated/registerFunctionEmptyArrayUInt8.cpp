#include "registerFunctionEmptyArrayUInt8.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayUInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt8>();
}

}
