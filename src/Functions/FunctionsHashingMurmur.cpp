#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

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
    factory.registerFunction<FunctionKafkaMurmurHash>();
}
}
