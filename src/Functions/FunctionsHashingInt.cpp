#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(HashingInt)
{
    factory.registerFunction<FunctionIntHash32>();
    factory.registerFunction<FunctionIntHash64>();
}
}
