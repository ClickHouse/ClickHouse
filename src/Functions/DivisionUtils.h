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

    if constexpr (is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(a / b);
    else if constexpr (!is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(B(a) / b);
    else
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
        if constexpr (is_big_int_v<A> && std::is_floating_point_v<B>)
            return Result(static_cast<B>(a) / b);
        else if constexpr (is_big_int_v<B> && std::is_floating_point_v<A>)
            return a / static_cast<Result>(b);
        else if constexpr (is_big_int_v<A> && std::is_same_v<B, UInt8>)
            return static_cast<Result>(checkedDivision(make_signed_t<A>(a), Int16(b)));
        else if constexpr (is_big_int_v<B> && std::is_same_v<A, UInt8>)
            return checkedDivision(Int16(a), make_signed_t<B>(b));
        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        else if constexpr (is_signed_v<A> || is_signed_v<B>)
        {
            if constexpr (is_integer_v<A> && is_integer_v<B>)
            {
                return checkedDivision(make_signed_t<A>(a),
                    sizeof(A) > sizeof(B) ? make_signed_t<A>(b) : make_signed_t<B>(b));
            }
            else
                return checkedDivision(a, b);
        }
        else
            return checkedDivision(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename Result, typename A, typename B>
inline Result applyBigIntModulo(A a, B b)
{
    if constexpr (std::is_same_v<A, UInt8>)
        return UInt16(a) % b;
    else if constexpr (std::is_same_v<B, UInt8>)
        return static_cast<UInt16>(a % UInt16(b));
    else if constexpr (sizeof(A) > sizeof(B))
        return static_cast<Result>(a % A(b));
    else
        return static_cast<Result>(B(a) % b);
}

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    using IntegerAType = typename NumberTraits::ToInteger<A>::Type;
    using IntegerBType = typename NumberTraits::ToInteger<B>::Type;

    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool is_special = is_big_int_v<IntegerAType> || is_big_int_v<IntegerBType>;

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
            throwIfDivisionLeadsToFPE(IntegerAType(a), IntegerBType(b));

            if constexpr (is_special)
                return applyBigIntModulo<Result>(IntegerAType(a), IntegerBType(b));
            else
                return IntegerAType(a) % IntegerBType(b);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

}
