#include "registerFunctionEmptyArrayUInt32.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayUInt32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt32>();
}

}
