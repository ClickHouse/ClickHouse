#include <Functions/registerFunctionBitOr.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitOr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitOr>();
}

}
