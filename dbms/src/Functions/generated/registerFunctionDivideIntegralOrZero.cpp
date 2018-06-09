#include "registerFunctionDivideIntegralOrZero.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionDivideIntegralOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegralOrZero>();
}

}
