#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(HashingMurmur)
{
    factory.registerFunction<FunctionMurmurHash2_32>();
    factory.registerFunction<FunctionMurmurHash2_64>();
    factory.registerFunction<FunctionMurmurHash3_32>();
    factory.registerFunction<FunctionMurmurHash3_64>();
    factory.registerFunction<FunctionMurmurHash3_128>();
    factory.registerFunction<FunctionGccMurmurHash>();
}
}
