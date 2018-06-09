#include "registerFunctionProject.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsProjection.h>

namespace DB
{

void registerFunctionProject(FunctionFactory & factory)
{
    factory.registerFunction<FunctionProject>();
}

}
