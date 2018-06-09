#include <Functions/registerFunctionDivideIntegralOrZero.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionDivideIntegralOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegralOrZero>();
}

}
