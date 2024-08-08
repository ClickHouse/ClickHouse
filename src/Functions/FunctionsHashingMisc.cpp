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
    factory.registerFunction<FunctionSipHash128Reference>(FunctionDocumentation{
        .description="Like [sipHash128](#hash_functions-siphash128) but implements the 128-bit algorithm from the original authors of SipHash.",
        .examples{{"hash", "SELECT hex(sipHash128Reference('foo', '\\x01', 3))", ""}},
        .categories{"Hash"}
    });
    factory.registerFunction<FunctionSipHash128ReferenceKeyed>(FunctionDocumentation{
        .description = "Same as [sipHash128Reference](#hash_functions-siphash128reference) but additionally takes an explicit key argument "
                       "instead of using a fixed key.",
        .examples{{"hash", "SELECT hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912),'foo', '\\x01', 3));", ""}},
        .categories{"Hash"}});
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
        FunctionDocumentation{
            .description="Calculates value of XXH3 64-bit hash function. Refer to https://github.com/Cyan4973/xxHash for detailed documentation.",
            .examples{{"hash", "SELECT xxh3('ClickHouse')", ""}},
            .categories{"Hash"}
        });

    factory.registerFunction<FunctionWyHash64>();

#if USE_SSL
    factory.registerFunction<FunctionHalfMD5>(FunctionDocumentation{
        .description = R"(
[Interprets](../..//sql-reference/functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString) all the input
parameters as strings and calculates the MD5 hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the
resulting string, and interprets them as [UInt64](../../../sql-reference/data-types/int-uint.md) in big-endian byte order. The function is
relatively slow (5 million short strings per second per processor core).

Consider using the [sipHash64](../../sql-reference/functions/hash-functions.md/#hash_functions-siphash64) function instead.
                       )",
        .syntax = "SELECT halfMD5(par1,par2,...,parN);",
        .arguments
        = {{"par1,par2,...,parN",
            R"(
The function takes a variable number of input parameters. Arguments can be any of the supported data types. For some data types calculated
value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed
Tuple with the same data, Map and the corresponding Array(Tuple(key, value)) type with the same data).
                       )"}},
        .returned_value = "The computed half MD5 hash of the given input params returned as a "
                          "[UInt64](../../../sql-reference/data-types/int-uint.md) in big-endian byte order.",
        .examples
        = {{"",
            "SELECT HEX(halfMD5('abc', 'cde', 'fgh'));",
            R"(
┌─hex(halfMD5('abc', 'cde', 'fgh'))─┐
│ 2C9506B7374CFAF4                  │
└───────────────────────────────────┘
            )"}}});
#endif
}
}
