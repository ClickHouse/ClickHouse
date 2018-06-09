#include "registerFunctionBitOr.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitOr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitOr>();
}

}
