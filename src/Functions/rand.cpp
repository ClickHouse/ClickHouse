#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace
{

struct NameRand { static constexpr auto name = "rand"; };
using FunctionRand = FunctionRandom<UInt32, NameRand>;

}

REGISTER_FUNCTION(Rand)
{
    FunctionDocumentation::Description description = R"(
Returns a random `UInt32` number with uniform distribution.

Uses a linear congruential generator with an initial state obtained from the system, which means that while it appears random, it's not truly random and can be predictable if the initial state is known.
For scenarios where true randomness is crucial, consider using alternative methods like system-level calls or integrating with external libraries.
    )";
    FunctionDocumentation::Syntax syntax = "rand([x])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a random number of type `UInt32`.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT rand();", "1569354847"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRand>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("rand32", NameRand::name);
}

}
