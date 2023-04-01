#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

REGISTER_FUNCTION(Hashing)
{
    factory.registerFunction<FunctionSipHash64>();
    factory.registerFunction<FunctionSipHash64Keyed>();
    factory.registerFunction<FunctionSipHash128>();
    factory.registerFunction<FunctionSipHash128Keyed>();
    factory.registerFunction<FunctionSipHash128Reference>({
        "Like [sipHash128](#hash_functions-siphash128) but implements the 128-bit algorithm from the original authors of SipHash.",
        Documentation::Examples{{"hash", "SELECT hex(sipHash128Reference('foo', '\\x01', 3))"}},
        Documentation::Categories{"Hash"}
    });
    factory.registerFunction<FunctionSipHash128ReferenceKeyed>({
        "Same as [sipHash128Reference](#hash_functions-siphash128reference) but additionally takes an explicit key argument instead of using a fixed key.",
        Documentation::Examples{{"hash", "SELECT hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912),'foo', '\\x01', 3));"}},
        Documentation::Categories{"Hash"}
    });
    factory.registerFunction<FunctionCityHash64>();
    factory.registerFunction<FunctionFarmFingerprint64>();
    factory.registerFunction<FunctionFarmHash64>();
    factory.registerFunction<FunctionMetroHash64>();
    factory.registerFunction<FunctionURLHash>();
    factory.registerFunction<FunctionJavaHash>();
    factory.registerFunction<FunctionJavaHashUTF16LE>();
    factory.registerFunction<FunctionHiveHash>();

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
