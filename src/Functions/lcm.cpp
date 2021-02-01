#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

#include <numeric>
#include <limits>
#include <type_traits>


namespace
{

template <typename T>
constexpr T abs(T value) noexcept
{
    if constexpr (std::is_signed_v<T>)
    {
        if (value >= 0 || value == std::numeric_limits<T>::min())
            return value;
        return -value;
    }
    else
        return value;
}

}


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename A, typename B>
struct LCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline std::enable_if_t<is_big_int_v<A> || is_big_int_v<B> || is_big_int_v<Result>, Result>
    apply([[maybe_unused]] A a, [[maybe_unused]] B b)
    {
        throw Exception("LCM is not implemented for big integers", ErrorCodes::NOT_IMPLEMENTED);
    }

    template <typename Result = ResultType>
    static inline std::enable_if_t<!is_big_int_v<A> && !is_big_int_v<B> && !is_big_int_v<Result>, Result>
    apply([[maybe_unused]] A a, [[maybe_unused]] B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));

        /** It's tempting to use std::lcm function.
          * But it has undefined behaviour on overflow.
          * And assert in debug build.
          * We need some well defined behaviour instead
          * (example: throw an exception or overflow in implementation specific way).
          */

        using Int = typename NumberTraits::ToInteger<Result>::Type;
        using Unsigned = make_unsigned_t<Int>;

        Unsigned val1 = abs<Int>(a) / std::gcd(Int(a), Int(b));
        Unsigned val2 = abs<Int>(b);

        /// Overflow in implementation specific way.
        return Result(val1 * val2);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

struct NameLCM { static constexpr auto name = "lcm"; };
using FunctionLCM = BinaryArithmeticOverloadResolver<LCMImpl, NameLCM, false>;

}

void registerFunctionLCM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLCM>();
}

}
