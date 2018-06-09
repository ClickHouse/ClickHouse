#include "registerFunctionMinus.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionMinus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMinus>();
}

}
