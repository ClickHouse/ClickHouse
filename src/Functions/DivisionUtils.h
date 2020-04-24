#pragma once

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


#pragma GCC diagnostic pop

namespace {
template <typename Result, typename A, typename B>
inline Result applyBigInteger(A /*a*/, B /*b*/) {
    throw Exception("Division is not implemented for big integers", ErrorCodes::NOT_IMPLEMENTED);
    // if constexpr (std::is_same_v<A, UInt8>)
    //     return static_cast<Result>(static_cast<UInt16>(a) / b);
    // else if constexpr (std::is_same_v<B, UInt8>)
    //     return static_cast<Result>(a / static_cast<UInt16>(b));
    // else
    //     return static_cast<Result>(a / b);
}
}

template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(a, b);

        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            return applyBigInteger<Result>(a, b);
        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        else if constexpr (is_integral_v<A> && is_integral_v<B> && (is_signed_v<A> || is_signed_v<B>))
            return make_signed_t<A>(a) / make_signed_t<B>(b);
        else
            return a / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename Result, typename A, typename B>
inline Result applyBigIntModulo(A a, B b) {
    if constexpr (std::is_same_v<A, UInt8>)
        return static_cast<UInt16>(a) % b;
    else if constexpr (std::is_same_v<B, UInt8>)
        return a % static_cast<UInt16>(b);
    else if constexpr (sizeof(A) > sizeof(B))
        return a % static_cast<A>(b);
    else
        return static_cast<B>(a) % b;
}

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool is_special = is_big_int_v<typename NumberTraits::ToInteger<A>::Type>
                                             || is_big_int_v<typename NumberTraits::ToInteger<B>::Type>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));

        if constexpr (is_special)
            return applyBigIntModulo<Result>(typename NumberTraits::ToInteger<A>::Type(a),
                                             typename NumberTraits::ToInteger<B>::Type(b));
        else
            return typename NumberTraits::ToInteger<A>::Type(a) % typename NumberTraits::ToInteger<B>::Type(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

}
