#include <Functions/registerFunctionModulo.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionModulo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModulo>();
}

}
