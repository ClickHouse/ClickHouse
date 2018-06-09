#include "registerFunctionDivideFloating.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideFloating>();
}

}
