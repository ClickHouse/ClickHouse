#include "registerFunctionHas.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionHas(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHas>();
}

}
