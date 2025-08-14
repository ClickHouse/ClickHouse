#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{

template <typename A>
struct RoundDurationImpl
{
    using ResultType = UInt16;
    static constexpr bool allow_string_or_fixed_string = false;

    static ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 10 ? 1
            : (x < 30 ? 10
            : (x < 60 ? 30
            : (x < 120 ? 60
            : (x < 180 ? 120
            : (x < 240 ? 180
            : (x < 300 ? 240
            : (x < 600 ? 300
            : (x < 1200 ? 600
            : (x < 1800 ? 1200
            : (x < 3600 ? 1800
            : (x < 7200 ? 3600
            : (x < 18000 ? 7200
            : (x < 36000 ? 18000
            : 36000))))))))))))));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameRoundDuration { static constexpr auto name = "roundDuration"; };
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundDuration> : PositiveMonotonicity {};

REGISTER_FUNCTION(RoundDuration)
{
    FunctionDocumentation::Description description = R"(
Accepts a number. If the number is less than one, it returns `0`. Otherwise, it rounds the number down to numbers from the set of commonly used durations: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000`.
)";
    FunctionDocumentation::Syntax syntax = "roundDuration(num)";
    FunctionDocumentation::Arguments arguments = {
        {"num", "A number to round to one of the numbers in the set of common durations.", {"`UInt`/`Float`"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0`, for `num` < 1. Otherwise, one of: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000`.", {"`UInt16`"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT *, roundDuration(*) FROM system.numbers WHERE number IN (0, 9, 19, 47, 101, 149, 205, 271, 421, 789, 1423, 2345, 4567, 9876, 24680, 42573)", "┌─number─┬─roundDuration(number)─┐\n│      0 │                     0 │\n│      9 │                     1 │\n│     19 │                    10 │\n│     47 │                    30 │\n│    101 │                    60 │\n│    149 │                   120 │\n│    205 │                   180 │\n│    271 │                   240 │\n│    421 │                   300 │\n│    789 │                   600 │\n│   1423 │                  1200 │\n│   2345 │                  1800 │\n│   4567 │                  3600 │\n│   9876 │                  7200 │\n│  24680 │                 18000 │\n│  42573 │                 36000 │\n└────────┴───────────────────────┘"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionRoundDuration>(documentation);
}

}
