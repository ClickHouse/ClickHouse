#include "registerFunctionDivideIntegral.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionDivideIntegral(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegral>();
}

}
