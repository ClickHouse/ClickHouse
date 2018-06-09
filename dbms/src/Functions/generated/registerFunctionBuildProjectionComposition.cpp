#include <Functions/registerFunctionBuildProjectionComposition.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsProjection.h>

namespace DB
{

void registerFunctionBuildProjectionComposition(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuildProjectionComposition>();
}

}
