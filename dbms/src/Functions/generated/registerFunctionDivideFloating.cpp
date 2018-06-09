#include <Functions/registerFunctionDivideFloating.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideFloating>();
}

}
