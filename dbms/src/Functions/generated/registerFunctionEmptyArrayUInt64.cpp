#include "registerFunctionEmptyArrayUInt64.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayUInt64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt64>();
}

}
