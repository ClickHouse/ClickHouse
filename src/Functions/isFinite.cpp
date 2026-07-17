#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>
#include <type_traits>


namespace DB
{
namespace
{

struct IsFiniteImpl
{
    /// Better implementation, because isinf, isfinite, isnan are not inlined for unknown reason.
    /// Assuming IEEE 754.
    /// NOTE gcc 7 doesn't vectorize this loop.

    static constexpr auto name = "isFinite";
    template <typename T>
    static bool execute(const T t)
    {
        if constexpr (std::is_same_v<T, float>)
            return (std::bit_cast<uint32_t>(t)
                 & 0b01111111100000000000000000000000)
                != 0b01111111100000000000000000000000;
        else if constexpr (std::is_same_v<T, double>)
            return (std::bit_cast<uint64_t>(t)
                 & 0b0111111111110000000000000000000000000000000000000000000000000000)
                != 0b0111111111110000000000000000000000000000000000000000000000000000;
        else
        {
            (void)t;
            return true;
        }
    }
};

using FunctionIsFinite = FunctionNumericPredicate<IsFiniteImpl>;

}

REGISTER_FUNCTION(IsFinite)
{
    FunctionDocumentation::Description description = R"(
Returns `1` if the Float32 or Float64 argument not infinite and not a `NaN`,
otherwise this function returns `0`.
    )";
    FunctionDocumentation::Syntax syntax = "isFinite(x)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "Number to check for finiteness.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"`1` if x is not infinite and not `NaN`, otherwise `0`."};
    FunctionDocumentation::Examples examples = {{"Test if a number is finite", "SELECT isFinite(inf)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionIsFinite>(documentation);
}

}
