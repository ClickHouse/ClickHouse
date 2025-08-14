#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{

template <typename A>
struct RoundAgeImpl
{
    using ResultType = UInt8;
    static constexpr const bool allow_string_or_fixed_string = false;

    static ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 18 ? 17
            : (x < 25 ? 18
            : (x < 35 ? 25
            : (x < 45 ? 35
            : (x < 55 ? 45
            : 55)))));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameRoundAge { static constexpr auto name = "roundAge"; };
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundAge> : PositiveMonotonicity {};

REGISTER_FUNCTION(RoundAge)
{
    FunctionDocumentation::Description description = R"(
Accepts a number within various commonly used ranges of human age and returns either a maximum or a minimum within that range.
)";
    FunctionDocumentation::Syntax syntax = "roundAge(num)";
    FunctionDocumentation::Arguments arguments = {
        {"age", "A number representing an age in years.", {"`UInt`/`Float`"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0`, for age < 1. Returns `17`, for 1 ≤ age ≤ 17. Returns `18`, for 18 ≤ age ≤ 24. Returns `25`, for 25 ≤ age ≤ 34. Returns `35`, for 35 ≤ age ≤ 44. Returns `45`, for 45 ≤ age ≤ 54. Returns `55`, for age ≥ 55.", {"`UInt8`"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT *, roundAge(*) FROM system.numbers WHERE number IN (0, 5, 20, 31, 37, 54, 72);", "┌─number─┬─roundAge(number)─┐\n│      0 │                0 │\n│      5 │               17 │\n│     20 │               18 │\n│     31 │               25 │\n│     37 │               35 │\n│     54 │               45 │\n│     72 │               55 │\n└────────┴──────────────────┘"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionRoundAge>(documentation);
}

}
