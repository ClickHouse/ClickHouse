#include "registerFunctionEmptyArrayUInt16.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayUInt16(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt16>();
}

}
