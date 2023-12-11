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
    factory.registerFunction<FunctionMD4>();
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
    factory.registerFunction<FunctionSHA384>();
    factory.registerFunction<FunctionSHA512>();
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
