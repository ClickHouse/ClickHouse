#include "registerFunctionArrayHasAny.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
