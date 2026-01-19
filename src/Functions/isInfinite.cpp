#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>
#include <type_traits>


namespace DB
{
namespace
{

struct IsInfiniteImpl
{
    static constexpr auto name = "isInfinite";
    template <typename T>
    static bool execute(const T t)
    {
        if constexpr (std::is_same_v<T, float>)
            return (std::bit_cast<uint32_t>(t)
                 & 0b01111111111111111111111111111111)
                == 0b01111111100000000000000000000000;
        else if constexpr (std::is_same_v<T, double>)
            return (std::bit_cast<uint64_t>(t)
                 & 0b0111111111111111111111111111111111111111111111111111111111111111)
                == 0b0111111111110000000000000000000000000000000000000000000000000000;
        else
        {
            (void)t;
            return false;
        }
    }
};

using FunctionIsInfinite = FunctionNumericPredicate<IsInfiniteImpl>;

}

REGISTER_FUNCTION(IsInfinite)
{
    FunctionDocumentation::Description description = R"(
    Returns `1` if the Float32 or Float64 argument is infinite, otherwise this function returns `0`.
    Note that `0` is returned for a `NaN`.
    )";
    FunctionDocumentation::Syntax syntax = "isInfinite(x)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "Number to check for infiniteness.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"`1` if x is infinite, otherwise `0` (including for `NaN`)."};
    FunctionDocumentation::Examples examples = {{"Test if a number is infinite", "SELECT isInfinite(inf), isInfinite(NaN), isInfinite(10))", "1 0 0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionIsInfinite>(documentation);
}

}
