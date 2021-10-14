#include <type_traits>
#include <ext/bit_cast.h>
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
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= sizeof(UInt32)), T>
roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (31 - __builtin_clz(x)));
}

template <typename T>
inline std::enable_if_t<std::is_integral_v<T> && (sizeof(T) == sizeof(UInt64)), T>
roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (63 - __builtin_clzll(x)));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Int128>, T>
roundDownToPowerOfTwo(T x)
{
    if (x <= 0)
        return 0;

    if (Int64 x64 = Int64(x >> 64))
        return Int128(roundDownToPowerOfTwo(x64)) << 64;
    return roundDownToPowerOfTwo(Int64(x));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float32>, T>
roundDownToPowerOfTwo(T x)
{
    return ext::bit_cast<T>(ext::bit_cast<UInt32>(x) & ~((1ULL << 23) - 1));
}

template <typename T>
inline std::enable_if_t<std::is_same_v<T, Float64>, T>
roundDownToPowerOfTwo(T x)
{
    return ext::bit_cast<T>(ext::bit_cast<UInt64>(x) & ~((1ULL << 52) - 1));
}

template <typename T>
inline std::enable_if_t<is_big_int_v<T>, T>
roundDownToPowerOfTwo(T)
{
    throw Exception("roundToExp2() for big integers is not implemented", ErrorCodes::NOT_IMPLEMENTED);
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
    static constexpr const bool allow_fixed_string = false;

    static inline T apply(T x)
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

void registerFunctionRoundToExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundToExp2>();
}

}
