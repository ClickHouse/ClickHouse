#include "registerFunctionRestoreProjection.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsProjection.h"

namespace DB
{

void registerFunctionRestoreProjection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRestoreProjection>();
}

}
