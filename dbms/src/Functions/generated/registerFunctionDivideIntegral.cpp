#include <Functions/registerFunctionDivideIntegral.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionDivideIntegral(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegral>();
}

}
