#include "registerFunctionArrayEnumerate.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayEnumerate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerate>();
}

}
