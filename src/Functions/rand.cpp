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
    FunctionDocumentation::Syntax syntax = "rand()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a number of type `UInt32`.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT rand();", "1569354847 -- Note: The actual output will be a random number, not the specific number shown in the example"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRand>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("rand32", NameRand::name);
}

}
