#include <Functions/registerFunctionMultiply.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionMultiply(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiply>();
}

}
