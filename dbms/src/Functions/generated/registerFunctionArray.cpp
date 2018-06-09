#include "registerFunctionArray.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArray>();
}

}
