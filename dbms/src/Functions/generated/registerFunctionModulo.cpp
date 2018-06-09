#include "registerFunctionModulo.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionModulo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModulo>();
}

}
