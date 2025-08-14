#include <type_traits>
#include <base/bit_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename T>
requires std::is_integral_v<T> && (sizeof(T) <= sizeof(UInt32))
inline T roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (31 - __builtin_clz(x)));
}

template <typename T>
requires std::is_integral_v<T> && (sizeof(T) == sizeof(UInt64))
inline T roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (63 - __builtin_clzll(x)));
}

template <typename T>
requires std::is_same_v<T, Float32>
inline T roundDownToPowerOfTwo(T x)
{
    return bit_cast<T>(bit_cast<UInt32>(x) & ~((1ULL << 23) - 1));
}

template <typename T>
requires std::is_same_v<T, Float64>
inline T roundDownToPowerOfTwo(T x)
{
    return bit_cast<T>(bit_cast<UInt64>(x) & ~((1ULL << 52) - 1));
}

template <typename T>
requires is_big_int_v<T>
inline T roundDownToPowerOfTwo(T)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "roundToExp2() for big integers is not implemented");
}

/** For integer data types:
  * - if number is greater than zero, round it down to nearest power of two (example: roundToExp2(100) = 64, roundToExp2(64) = 64);
  * - otherwise, return 0.
  *
  * For floating point data types: zero out mantissa, but leave exponent.
  * - if number is greater than zero, round it down to nearest power of two (example: roundToExp2(3) = 2);
  * - negative powers are also used (example: roundToExp2(0.7) = 0.5);
  * - if number is zero, return zero;
  * - if number is less than zero, the result is symmetrical: roundToExp2(x) = -roundToExp2(-x). (example: roundToExp2(-0.3) = -0.25);
  */

template <typename T>
struct RoundToExp2Impl
{
    using ResultType = T;
    static constexpr const bool allow_string_or_fixed_string = false;

    static T apply(T x)
    {
        return roundDownToPowerOfTwo<T>(x);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameRoundToExp2 { static constexpr auto name = "roundToExp2"; };
using FunctionRoundToExp2 = FunctionUnaryArithmetic<RoundToExp2Impl, NameRoundToExp2, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundToExp2> : PositiveMonotonicity {};

REGISTER_FUNCTION(RoundToExp2)
{
    FunctionDocumentation::Description description = R"(
Accepts a number. If the number is less than one, it returns `0`. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.
)";
    FunctionDocumentation::Syntax syntax = "roundToExp2(num)";
    FunctionDocumentation::Arguments arguments = {
        {"num", "A number to round.", {"`UInt`/`Float`"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0`, for `num` < 1. Otherwise, returns `num` rounded down to the nearest (whole non-negative) degree of two.", {"`UInt8` for `num` < 1, otherwise `UInt`/`Float` equivalent to the input type"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT *, roundToExp2(*) FROM system.numbers WHERE number IN (0, 2, 5, 10, 19, 50)", "┌─number─┬─roundToExp2(number)─┐\n│      0 │                   0 │\n│      2 │                   2 │\n│      5 │                   4 │\n│     10 │                   8 │\n│     19 │                  16 │\n│     50 │                  32 │\n└────────┴─────────────────────┘"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionRoundToExp2>(documentation);
}

}
