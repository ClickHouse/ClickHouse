#include "registerFunctionOneOrZero.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsProjection.h"

namespace DB
{

void registerFunctionOneOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionOneOrZero>();
}

}
