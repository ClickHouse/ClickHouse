#pragma once

#include <cmath>
#include <type_traits>
#include <Common/Exception.h>
#include <DataTypes/NumberTraits.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(is_signed_v<A> && is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if (unlikely(is_signed_v<A> && is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}

template <typename A, typename B>
inline auto checkedDivision(A a, B b)
{
    throwIfDivisionLeadsToFPE(a, b);
    return a / b;
}


#pragma GCC diagnostic pop

template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        if constexpr (is_integral_v<A> && is_integral_v<B> && (is_signed_v<A> || is_signed_v<B>))
            return checkedDivision(std::make_signed_t<A>(a),
                sizeof(A) > sizeof(B) ? std::make_signed_t<A>(b) : std::make_signed_t<B>(b));
        else
            return checkedDivision(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr (std::is_floating_point_v<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return ResultType(a) - trunc(ResultType(a) / ResultType(b)) * ResultType(b);
        }
        else
        {
            throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
            return typename NumberTraits::ToInteger<A>::Type(a) % typename NumberTraits::ToInteger<B>::Type(b);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

}
