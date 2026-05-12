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
Takes a number representing a human age, compares it to standard age ranges, and returns either the highest or lowest value of the range the number falls within.

- Returns `0`, for `age < 1`.
- Returns `17`, for `1 ≤ age ≤ 17`.
- Returns `18`, for `18 ≤ age ≤ 24`.
- Returns `25`, for `25 ≤ age ≤ 34`.
- Returns `35`, for `35 ≤ age ≤ 44`.
- Returns `45`, for `45 ≤ age ≤ 54`.
- Returns `55`, for `age ≥ 55`.
)";
    FunctionDocumentation::Syntax syntax = "roundAge(num)";
    FunctionDocumentation::Arguments arguments = {
        {"age", "A number representing an age in years.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns either the highest or lowest age of the range `age` falls within.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT *, roundAge(*) FROM system.numbers WHERE number IN (0, 5, 20, 31, 37, 54, 72);",
        R"(
┌─number─┬─roundAge(number)─┐
│      0 │                0 │
│      5 │               17 │
│     20 │               18 │
│     31 │               25 │
│     37 │               35 │
│     54 │               45 │
│     72 │               55 │
└────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionRoundAge>(documentation);
}

}
