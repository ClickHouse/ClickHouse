#include <Functions/registerFunctionBitAnd.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitAnd>();
}

}
