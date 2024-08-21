#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.
namespace DB
{
#if USE_SSL
REGISTER_FUNCTION(HashingRipe)
{
    factory.registerFunction<FunctionRipeMD160Hash>(FunctionDocumentation{
        .description = "RIPEMD-160 hash function, primarily used in Bitcoin address generation.",
        .examples{{"", "SELECT hex(ripeMD160('The quick brown fox jumps over the lazy dog'));", R"(
            ┌─hex(ripeMD160('The quick brown fox jumps over the lazy dog'))─┐
            │ 37F332F68DB77BD9D7EDD4969571AD671CF9DD3B                      │
            └───────────────────────────────────────────────────────────────┘
        )"}},
        .categories{"Hash"}});
}
#endif
}
