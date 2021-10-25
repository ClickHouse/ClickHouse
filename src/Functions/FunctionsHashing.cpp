#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionsHashing(FunctionFactory & factory)
{
#if USE_SSL
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
#endif
    factory.registerFunction<FunctionSipHash64>();
    factory.registerFunction<FunctionSipHash128>();
    factory.registerFunction<FunctionCityHash64>();
    factory.registerFunction<FunctionFarmFingerprint64>();
    factory.registerFunction<FunctionFarmHash64>();
    factory.registerFunction<FunctionMetroHash64>();
    factory.registerFunction<FunctionIntHash32>();
    factory.registerFunction<FunctionIntHash64>();
    factory.registerFunction<FunctionURLHash>();
    factory.registerFunction<FunctionJavaHash>();
    factory.registerFunction<FunctionJavaHashUTF16LE>();
    factory.registerFunction<FunctionHiveHash>();
#if !defined(ARCADIA_BUILD)
    factory.registerFunction<FunctionMurmurHash2_32>();
    factory.registerFunction<FunctionMurmurHash2_64>();
    factory.registerFunction<FunctionMurmurHash3_32>();
    factory.registerFunction<FunctionMurmurHash3_64>();
    factory.registerFunction<FunctionMurmurHash3_128>();
    factory.registerFunction<FunctionGccMurmurHash>();
#endif

#if USE_XXHASH
    factory.registerFunction<FunctionXxHash32>();
    factory.registerFunction<FunctionXxHash64>();
#endif
}
}
