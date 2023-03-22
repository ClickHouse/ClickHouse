#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(Hashing)
{
#if USE_SSL
    factory.registerFunction<FunctionMD4>();
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
    factory.registerFunction<FunctionSHA384>();
    factory.registerFunction<FunctionSHA512>();
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
    factory.registerFunction<FunctionMurmurHash2_32>();
    factory.registerFunction<FunctionMurmurHash2_64>();
    factory.registerFunction<FunctionMurmurHash3_32>();
    factory.registerFunction<FunctionMurmurHash3_64>();
    factory.registerFunction<FunctionMurmurHash3_128>();
    factory.registerFunction<FunctionGccMurmurHash>();

    factory.registerFunction<FunctionXxHash32>();
    factory.registerFunction<FunctionXxHash64>();
    factory.registerFunction<FunctionXXH3>(
        {
            "Calculates value of XXH3 64-bit hash function. Refer to https://github.com/Cyan4973/xxHash for detailed documentation.",
            Documentation::Examples{{"hash", "SELECT xxh3('ClickHouse')"}},
            Documentation::Categories{"Hash"}
        },
        FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionWyHash64>();


    factory.registerFunction<FunctionBLAKE3>(
    {
        R"(
Calculates BLAKE3 hash string and returns the resulting set of bytes as FixedString.
This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library.
The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.
It returns a BLAKE3 hash as a byte array with type FixedString(32).
)",
        Documentation::Examples{
            {"hash", "SELECT hex(BLAKE3('ABC'))"}},
        Documentation::Categories{"Hash"}
    },
    FunctionFactory::CaseSensitive);
}
}
