#include "config.h"

#if USE_SSL

#include "FunctionsHashing.h"
#include <Functions/FunctionFactory.h>

/// SSL functions are located in the separate FunctionsHashingSSL.cpp file
///  to lower the compilation time of FunctionsHashing.cpp

namespace DB
{

REGISTER_FUNCTION(HashingSSL)
{
    factory.registerFunction<FunctionMD4>();
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
    factory.registerFunction<FunctionSHA384>();
    factory.registerFunction<FunctionSHA512>();
}
}

#endif
