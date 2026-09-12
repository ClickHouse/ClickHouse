#pragma once

#include <cmath>
#include <type_traits>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (b == 0) [[unlikely]]
        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by zero");

    /// http://avva.livejournal.com/2548306.html
    if constexpr (is_signed_v<A> && is_signed_v<B>)
    {
        if (a == std::numeric_limits<A>::min() && b == -1) [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division of minimal signed number by minus one");
    }
}

}


namespace DB
{
template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (b == 0) [[unlikely]]
        return true;

    if constexpr (is_signed_v<A> && is_signed_v<B>)
    {
        if (a == std::numeric_limits<A>::min() && b == -1) [[unlikely]]
            return true;
    }

    return false;
}

/// Whether integer division of `a` by `b` would raise an FPE, accounting for the same operand
/// casts that `DivideIntegralImpl::apply` performs before dividing. This matters for mixed
/// signed/unsigned operands: e.g. `Int8(-128) / UInt8(255)` is evaluated as `Int8(-128) / Int8(-1)`,
/// which is the `INT_MIN / -1` overflow even though the raw `divisionLeadsToFPE(a, b)` would miss it
/// (because `B` is unsigned). Must stay in sync with `DivideIntegralImpl::apply`.
template <typename A, typename B>
inline bool integerDivisionLeadsToFPE(A a, B b)
{
    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    if constexpr (is_integer<A> && is_integer<B> && (is_signed_v<A> || is_signed_v<B>))
    {
        using SignedCastA = make_signed_t<CastA>;
        using SignedCastB = std::conditional_t<sizeof(A) <= sizeof(B), make_signed_t<CastB>, SignedCastA>;

        return divisionLeadsToFPE(static_cast<SignedCastA>(a), static_cast<SignedCastB>(b));
    }
    else
        return divisionLeadsToFPE(static_cast<CastA>(a), static_cast<CastB>(b));
}

/// Whether modulo of `a` by `b` is the FPE-like case that the `*OrNull` modulo functions must turn
/// into `NULL`. Unlike integer division, floating-point modulo never raises a floating-point
/// exception: `INT_MIN % -1` is a finite remainder and `a % 0` yields `NaN`, so only division by zero
/// is treated as the null case (matching `divideOrNull`). The plain `divisionLeadsToFPE(a, b)` cannot
/// be used for floating operands because `Float32`/`Float64` are signed and
/// `std::numeric_limits<Float>::min()` is the smallest positive value, so it would wrongly flag e.g.
/// `moduloOrNull(toFloat32(1.17549435e-38), toFloat32(-1))`. Integer modulo keeps the full check
/// because the `idiv` instruction computes the quotient too, so `INT_MIN % -1` raises just like division.
template <typename A, typename B>
inline bool moduloLeadsToFPE(A a, B b)
{
    if constexpr (is_floating_point<typename NumberTraits::ResultOfModulo<A, B>::Type>)
        return b == 0;
    else
        return divisionLeadsToFPE(a, b);
}

template <typename A, typename B>
inline auto checkedDivision(A a, B b)
{
    throwIfDivisionLeadsToFPE(a, b);

    if constexpr (is_floating_point<A> && !is_floating_point<B>)
        return a / static_cast<A>(b);
    else if constexpr (!is_floating_point<A> && is_floating_point<B>)
        return static_cast<B>(a) / b;
    else if constexpr (is_floating_point<A> && is_floating_point<B>)
    {
        /// Both operands are floating-point; promote to the higher-precision type
        /// explicitly so that mixed `Float32`/`Float64` calls do not implicitly widen.
        if constexpr (sizeof(A) >= sizeof(B))
            return a / static_cast<A>(b);
        else
            return static_cast<B>(a) / b;
    }
    else if constexpr (is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(a / b);
    else if constexpr (!is_big_int_v<A> && is_big_int_v<B>)
        return static_cast<A>(B(a) / b);
    else
        return a / b;
}


template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;
    /// No mainstream ISA (x86 SSE/AVX/AVX-512, ARM NEON/SVE/SVE2) has SIMD
    /// integer division. Auto-vectorization wraps each scalar div in
    /// extract/insert making the loop ~3x larger and slower.
    /// Example: DivideIntegralOrZeroImpl<UInt32, UInt64> Vector went from
    /// 384 B (x86-64-v2) to 1088 B (x86-64-v3) before this flag was added.
    static constexpr bool no_vectorize = true;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
        using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

        /// Otherwise overflow may occur due to integer promotion. Example: int8_t(-1) / uint64_t(2).
        /// NOTE: overflow is still possible when dividing large signed number to large unsigned number or vice-versa. But it's less harmful.
        if constexpr (is_integer<A> && is_integer<B> && (is_signed_v<A> || is_signed_v<B>))
        {
            using SignedCastA = make_signed_t<CastA>;
            using SignedCastB = std::conditional_t<sizeof(A) <= sizeof(B), make_signed_t<CastB>, SignedCastA>;

            return static_cast<Result>(checkedDivision(static_cast<SignedCastA>(a), static_cast<SignedCastB>(b)));
        }
        else
        {
            /// Comparisons are not strict to avoid rounding issues when operand is implicitly cast to float.

            if constexpr (is_floating_point<A>)
                if (isNaN(a) || a >= std::numeric_limits<CastA>::max() || a <= std::numeric_limits<CastA>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            if constexpr (is_floating_point<B>)
                if (isNaN(b) || b >= std::numeric_limits<CastB>::max() || b <= std::numeric_limits<CastB>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            auto res = checkedDivision(CastA(a), CastB(b));

            if constexpr (is_floating_point<decltype(res)>)
            {
                /// `std::numeric_limits<Result>::max()` for 64-bit integer Result types does not
                /// fit precisely in `Float32`, so promote `res` to `double` for the bounds check.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
                if (isNaN(res) || res >= static_cast<double>(std::numeric_limits<Result>::max()) || res <= std::numeric_limits<Result>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division, because it will produce infinite or too large number");
#pragma clang diagnostic pop
            }

            return static_cast<Result>(res);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename A, typename B>
struct DivideIntegralOrNullImpl : DivideIntegralImpl<A, B>
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template<typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if (unlikely(integerDivisionLeadsToFPE(a, b)))
            return 0;
        else
            return DivideIntegralImpl<A, B>::apply(a, b);
    }
};

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    using IntegerAType = typename NumberTraits::ToInteger<A>::Type;
    using IntegerBType = typename NumberTraits::ToInteger<B>::Type;

    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;
    /// Integer modulo uses the same `div` instruction as integer division — no
    /// SIMD benefit, only code bloat.  But the float path (a - trunc(a/b)*b)
    /// vectorizes well (divpd/roundpd are 2x throughput of divsd/roundsd).
    static constexpr bool no_vectorize = !is_floating_point<typename NumberTraits::ResultOfModulo<A, B>::Type>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr (is_floating_point<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return static_cast<ResultType>(a) - std::trunc(static_cast<ResultType>(a) / static_cast<ResultType>(b)) * static_cast<ResultType>(b);
        }
        else
        {
            if constexpr (is_floating_point<A>)
                if (isNaN(a) || a > std::numeric_limits<IntegerAType>::max() || a < std::numeric_limits<IntegerAType>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            if constexpr (is_floating_point<B>)
                if (isNaN(b) || b > std::numeric_limits<IntegerBType>::max() || b < std::numeric_limits<IntegerBType>::lowest())
                    throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Cannot perform integer division on infinite or too large floating point numbers");

            throwIfDivisionLeadsToFPE(IntegerAType(a), IntegerBType(b));

            using CastA = std::conditional_t<std::is_same_v<IntegerAType, UInt8>, uint8_t, IntegerAType>;
            using CastB = std::conditional_t<std::is_same_v<IntegerBType, UInt8>, uint8_t, IntegerBType>;

            /// `%` is evaluated after the usual arithmetic conversions, which make it unsigned as soon
            /// as the unsigned operand is at least as wide as the signed one (`Int32 % UInt32`,
            /// `UInt64 % Int64`, `Int128 % UInt128`), so a negative operand wraps to a large positive
            /// value before the remainder is taken.
            ///
            /// Casting an operand to a wider (or same-width) SIGNED type, the way
            /// `DivideIntegralImpl::apply` above does for the quotient, is not safe for the remainder:
            /// `DivideIntegralImpl`'s own comment accepts that overflow there "is less harmful", because
            /// the quotient is still right as long as the wrapped value's magnitude stays smaller than
            /// the divisor - a remainder's own magnitude is not exempt from that, since it IS the
            /// value being produced. Measured: `modulo(toUInt16(37528), toInt32(167682982))` cast
            /// `UInt16(37528)` to `Int16` first, overflowing to -28008; and separately,
            /// `modulo(toInt64(-9e18), toUInt64(1e19))` cast the `UInt64` divisor to `Int64`,
            /// overflowing it, and returned the wrong remainder for an input the un-fixed code had
            /// answered correctly by an unrelated pair of wraps that happened to cancel out.
            ///
            /// Work in UNSIGNED magnitudes instead, which never overflows at any width: casting any
            /// value - signed or unsigned, including the minimal signed number - to an unsigned type
            /// at least as wide as its own is exact (mod 2^width, which recovers the true value for a
            /// non-negative one); negating that exact unsigned value with unsigned subtraction is
            /// exact too, since it is well-defined modulo 2^width and it recovers the exact magnitude
            /// for a negative one (`0u - (Unsigned)a == |a|`, including `|INT_MIN|`, which does not fit
            /// the signed type of that width but does fit the unsigned one). `|a| mod |b|`, both in the
            /// wider of the two operands' unsigned widths, is then an ordinary unsigned remainder with
            /// no way to overflow; the sign of the final result follows the dividend, per C semantics,
            /// and `ResultType` is sized to hold it (`NumberTraits::ResultOfModulo`).
            if constexpr (is_integer<IntegerAType> && is_integer<IntegerBType>
                && (is_signed_v<IntegerAType> || is_signed_v<IntegerBType>))
            {
                using CommonUnsigned = typename NumberTraits::Construct<false, false,
                    std::max(sizeof(CastA), sizeof(CastB))>::Type;

                const bool a_negative = is_signed_v<CastA> && (IntegerAType(a) < 0);
                const CommonUnsigned ua = static_cast<CommonUnsigned>(IntegerAType(a));
                const CommonUnsigned magnitude_a = a_negative ? (CommonUnsigned(0) - ua) : ua;

                const bool b_negative = is_signed_v<CastB> && (IntegerBType(b) < 0);
                const CommonUnsigned ub = static_cast<CommonUnsigned>(IntegerBType(b));
                const CommonUnsigned magnitude_b = b_negative ? (CommonUnsigned(0) - ub) : ub;

                const CommonUnsigned magnitude_result = magnitude_a % magnitude_b;
                if (!a_negative)
                    return static_cast<Result>(magnitude_result);

                /// Negating a `Result` value directly can itself overflow: when `magnitude_result`
                /// equals `|Result::min()|` (reachable whenever `Result` is no wider than
                /// `CommonUnsigned`, e.g. `modulo(toInt64(-9223372036854775808),
                /// toUInt64(18446744073709551615))`, where the correctly-computed magnitude is
                /// exactly 2^63), `static_cast<Result>(magnitude_result)` is already
                /// `Result::min()`, and `-Result::min()` is undefined behaviour - caught by UBSan
                /// as "negation of ... cannot be represented". Negate in `Result`'s own unsigned
                /// type instead: `magnitude_result` always fits it (`Result` is sized to hold every
                /// representable outcome, per `NumberTraits::ResultOfModulo`), and unsigned
                /// subtraction is exact and well-defined even at this exact boundary.
                using UnsignedResult = make_unsigned_t<Result>;
                return static_cast<Result>(UnsignedResult(0) - static_cast<UnsignedResult>(magnitude_result));
            }
            else if constexpr (is_big_int_v<IntegerAType> || is_big_int_v<IntegerBType>)
            {
                CastA int_a(a);
                CastB int_b(b);

                if constexpr (is_big_int_v<IntegerBType> && sizeof(IntegerAType) <= sizeof(IntegerBType))
                    return static_cast<Result>(static_cast<CastB>(int_a) % int_b);
                else
                    return static_cast<Result>(int_a % static_cast<CastA>(int_b));
            }
            else
                return static_cast<Result>(IntegerAType(a) % IntegerBType(b));
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

template <typename A, typename B>
struct ModuloLegacyImpl : ModuloImpl<A, B>
{
    using ResultType = typename NumberTraits::ResultOfModuloLegacy<A, B>::Type;

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// moduloLegacy is only used in partition key expression
#endif
};

template <typename A, typename B>
struct ModuloOrNullImpl : ModuloImpl<A, B>
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if (unlikely(moduloLeadsToFPE(a, b)))
            return 0;
        else
            return ModuloImpl<A, B>::apply(a, b);
    }
};

template <typename A, typename B>
struct PositiveModuloImpl : ModuloImpl<A, B>
{
    using OriginResultType = typename ModuloImpl<A, B>::ResultType;
    using ResultType = typename NumberTraits::ResultOfPositiveModulo<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        auto res = ModuloImpl<A, B>::template apply<OriginResultType>(a, b);
        if constexpr (is_signed_v<A>)
        {
            if (res < 0)
            {
                if constexpr (is_unsigned_v<B>)
                {
                    if constexpr (is_integer<OriginResultType>)
                    {
                        /// Perform the addition in unsigned arithmetic to avoid
                        /// undefined behavior when b does not fit in the signed OriginResultType.
                        /// This is correct because mathematically 0 <= res + b < b.
                        return static_cast<ResultType>(
                            static_cast<make_unsigned_t<OriginResultType>>(res) + static_cast<make_unsigned_t<OriginResultType>>(b));
                    }
                    else
                    {
                        return static_cast<ResultType>(res + static_cast<OriginResultType>(b));
                    }
                }
                else
                {
                    if (b == std::numeric_limits<B>::lowest())
                        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by the most negative number");
                    return static_cast<ResultType>(
                        res + (b >= 0 ? static_cast<OriginResultType>(b) : static_cast<OriginResultType>(-b)));
                }
            }
        }
        return static_cast<ResultType>(res);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

template <typename A, typename B>
struct PositiveModuloOrNullImpl : PositiveModuloImpl<A, B>
{
    using ResultType = typename NumberTraits::ResultOfPositiveModulo<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if (unlikely(moduloLeadsToFPE(a, b)))
            return 0;
        else
            return PositiveModuloImpl<A, B>::apply(a, b);
    }
};

}
