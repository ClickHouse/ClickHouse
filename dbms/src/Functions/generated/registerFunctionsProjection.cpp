#include <Functions/FunctionFactory.h>
#include "FunctionsProjection.h"

#include "registerFunctionOneOrZero.h"
#include "registerFunctionProject.h"
#include "registerFunctionBuildProjectionComposition.h"
#include "registerFunctionRestoreProjection.h"


namespace DB
{

void registerFunctionsProjection(FunctionFactory & factory)
{
    registerFunctionOneOrZero(factory);
    registerFunctionProject(factory);
    registerFunctionBuildProjectionComposition(factory);
    registerFunctionRestoreProjection(factory);

}

}
