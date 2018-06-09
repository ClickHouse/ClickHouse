#include "registerFunctionArrayHasAll.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayHasAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
