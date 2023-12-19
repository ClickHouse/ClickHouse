#include "config.h"

#if USE_SSL

#include "FunctionsHashing.h"
#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

REGISTER_FUNCTION(HashingSSL)
{
    factory.registerFunction<FunctionMD4>(FunctionDocumentation{
        .description = R"(Calculates the MD4 hash of the given string.)",
        .syntax = "SELECT MD4(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The MD4 hash of the given input string returned as a [FixedString(16)](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(MD4('abc'));",
            R"(
┌─hex(MD4('abc'))──────────────────┐
│ A448017AAF21D8525FC10AE87AA6729D │
└──────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionHalfMD5>(FunctionDocumentation{
        .description = R"(
[Interprets](../..//sql-reference/functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString) all the input
parameters as strings and calculates the MD5 hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the
resulting string, and interprets them as [UInt64](../../../sql-reference/data-types/int-uint.md) in big-endian byte order. The function is
relatively slow (5 million short strings per second per processor core).

Consider using the [sipHash64](../../sql-reference/functions/hash-functions.md/#hash_functions-siphash64) function instead.
                       )",
        .syntax = "SELECT halfMD5(par1,par2,...,parN);",
        .arguments = {{"par1,par2,...,parN",
                       R"(
The function takes a variable number of input parameters. Arguments can be any of the supported data types. For some data types calculated
value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed
Tuple with the same data, Map and the corresponding Array(Tuple(key, value)) type with the same data).
                       )"
                     }},
        .returned_value
        = "The computed half MD5 hash of the given input params returned as a [UInt64](../../../sql-reference/data-types/int-uint.md) in big-endian byte order.",
        .examples
        = {{"",
            "SELECT HEX(halfMD5('abc', 'cde', 'fgh'));",
            R"(
┌─hex(halfMD5('abc', 'cde', 'fgh'))─┐
│ 2C9506B7374CFAF4                  │
└───────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionMD5>(FunctionDocumentation{
        .description = R"(Calculates the MD5 hash of the given string.)",
        .syntax = "SELECT MD5(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The MD5 hash of the given input string returned as a [FixedString(16)](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(MD5('abc'));",
            R"(
┌─hex(MD5('abc'))──────────────────┐
│ 900150983CD24FB0D6963F7D28E17F72 │
└──────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA1>(FunctionDocumentation{
        .description = R"(Calculates the SHA1 hash of the given string.)",
        .syntax = "SELECT SHA1(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA1 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA1('abc'));",
            R"(
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA224>(FunctionDocumentation{
        .description = R"(Calculates the SHA224 hash of the given string.)",
        .syntax = "SELECT SHA224(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA224 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA224('abc'));",
            R"(
┌─hex(SHA224('abc'))───────────────────────────────────────┐
│ 23097D223405D8228642A477BDA255B32AADBCE4BDA0B3F7E36C9DA7 │
└──────────────────────────────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA256>(FunctionDocumentation{
        .description = R"(Calculates the SHA256 hash of the given string.)",
        .syntax = "SELECT SHA256(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA256 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA256('abc'));",
            R"(
┌─hex(SHA256('abc'))───────────────────────────────────────────────┐
│ BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD │
└──────────────────────────────────────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA384>(FunctionDocumentation{
        .description = R"(Calculates the SHA384 hash of the given string.)",
        .syntax = "SELECT SHA384(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA384 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA384('abc'));",
            R"(
┌─hex(SHA384('abc'))───────────────────────────────────────────────────────────────────────────────┐
│ CB00753F45A35E8BB5A03D699AC65007272C32AB0EDED1631A8B605A43FF5BED8086072BA1E7CC2358BAECA134C825A7 │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA512>(FunctionDocumentation{
        .description = R"(Calculates the SHA512 hash of the given string.)",
        .syntax = "SELECT SHA512(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA512 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA512('abc'));",
            R"(
┌─hex(SHA512('abc'))───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ DDAF35A193617ABACC417349AE20413112E6FA4E89A97EA20A9EEEE64B55D39A2192992A274FC1A836BA3C23A3FEEBBD454D4423643CE80E2A9AC94FA54CA49F │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
          }}
    });
    factory.registerFunction<FunctionSHA512_256>(FunctionDocumentation{
        .description = R"(Calculates the SHA512_256 hash of the given string.)",
        .syntax = "SELECT SHA512_256(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA512_256 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA512_256('abc'));",
            R"(
┌─hex(SHA512_256('abc'))───────────────────────────────────────────┐
│ 53048E2681941EF99B2E29B76B4C7DABE4C2D0C634FC6D46E0E2F13107E7AF23 │
└──────────────────────────────────────────────────────────────────┘
            )"
          }}
    });
}
}

#endif
