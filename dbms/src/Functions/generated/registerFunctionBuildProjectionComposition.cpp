#include "registerFunctionBuildProjectionComposition.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsProjection.h"

namespace DB
{

void registerFunctionBuildProjectionComposition(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuildProjectionComposition>();
}

}
