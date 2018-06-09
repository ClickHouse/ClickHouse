#include "registerFunctionRestoreProjection.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsProjection.h>

namespace DB
{

void registerFunctionRestoreProjection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRestoreProjection>();
}

}
