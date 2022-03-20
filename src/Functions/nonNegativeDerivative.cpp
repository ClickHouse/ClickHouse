#include <Functions/nonNegativeDerivative.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionNonNegativeDerivative(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNonNegativeDerivativeImpl>();
}

}
