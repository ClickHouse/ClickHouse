#include "registerFunctionMultiply.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionMultiply(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiply>();
}

}
