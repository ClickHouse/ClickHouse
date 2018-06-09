#include <Functions/registerFunctionMinus.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionMinus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMinus>();
}

}
