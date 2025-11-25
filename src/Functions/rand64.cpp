#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace
{

struct NameRand64 { static constexpr auto name = "rand64"; };
using FunctionRand64 = FunctionRandom<UInt64, NameRand64>;

}

REGISTER_FUNCTION(Rand64)
{
    FunctionDocumentation::Description description = R"(
Returns a random distributed `UInt64` number with uniform distribution.

Uses a linear congruential generator with an initial state obtained from the system, which means that while it appears random, it's not truly random and can be predictable if the initial state is known.
For scenarios where true randomness is crucial, consider using alternative methods like system-level calls or integrating with external libraries.
    )";
    FunctionDocumentation::Syntax syntax = "rand64([x])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a random UInt64 number with uniform distribution.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT rand64();", "15030268859237645412"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRand64>(documentation);
}

}


