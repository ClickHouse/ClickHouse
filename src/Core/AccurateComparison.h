#pragma once

#include <cmath>
#include <limits>
#include "Defines.h"
#include "Types.h"
#include <common/extended_types.h>
#include <Common/NaNUtils.h>
#include <Common/UInt128.h>

/** Preceptually-correct number comparisons.
  * Example: Int8(-1) != UInt8(255)
*/

namespace accurate
{

using DB::UInt64;

/** Cases:
    1) Safe conversion (in case of default C++ operators)
        a) int vs any int
        b) uint vs any uint
        c) float vs any float
    2) int vs uint
        a) sizeof(int) <= sizeof(uint). Accurate comparison with MAX_INT tresholds
        b) sizeof(int)  > sizeof(uint). Casting to int
    3) integral_type vs floating_type
        a) sizeof(integral_type) <= 4. Comparison via casting arguments to Float64
        b) sizeof(integral_type) == 8. Accurate comparison. Consider 3 sets of intervals:
            1) interval between adjacent floats less or equal 1
            2) interval between adjacent floats greater then 2
            3) float is outside [MIN_INT64; MAX_INT64]
*/

// Case 1. Is pair of floats or pair of ints or pair of uints
template <typename A, typename B>
constexpr bool is_safe_conversion = (std::is_floating_point_v<A> && std::is_floating_point_v<B>)
    || (is_integer_v<A> && is_integer_v<B> && !(is_signed_v<A> ^ is_signed_v<B>));
template <typename A, typename B>
using bool_if_safe_conversion = std::enable_if_t<is_safe_conversion<A, B>, bool>;
template <typename A, typename B>
using bool_if_not_safe_conversion = std::enable_if_t<!is_safe_conversion<A, B>, bool>;


/// Case 2. Are params IntXX and UIntYY ?
template <typename TInt, typename TUInt>
constexpr bool is_any_int_vs_uint
    = is_integer_v<TInt> && is_integer_v<TUInt> && is_signed_v<TInt> && is_unsigned_v<TUInt>;

// Case 2a. Are params IntXX and UIntYY and sizeof(IntXX) >= sizeof(UIntYY) (in such case will use accurate compare)
template <typename TInt, typename TUInt>
constexpr bool is_le_int_vs_uint = is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) <= sizeof(TUInt));

static_assert(is_le_int_vs_uint<Int128, DB::UInt128>);
static_assert(is_le_int_vs_uint<Int128, DB::UInt256>);

template <typename TInt, typename TUInt>
using bool_if_le_int_vs_uint_t = std::enable_if_t<is_le_int_vs_uint<TInt, TUInt>, bool>;

template <typename TInt, typename TUInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> greaterOpTmpl(TInt a, TUInt b)
{
    return static_cast<TUInt>(a) > b && a >= 0 && b <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TUInt, typename TInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> greaterOpTmpl(TUInt a, TInt b)
{
    return a > static_cast<TUInt>(b) || b < 0 || a > static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TInt, typename TUInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> equalsOpTmpl(TInt a, TUInt b)
{
    return static_cast<TUInt>(a) == b && a >= 0 && b <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TUInt, typename TInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> equalsOpTmpl(TUInt a, TInt b)
{
    return a == static_cast<TUInt>(b) && b >= 0 && a <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}


// Case 2b. Are params IntXX and UIntYY and sizeof(IntXX) > sizeof(UIntYY) (in such case will cast UIntYY to IntXX and compare)
template <typename TInt, typename TUInt>
constexpr bool is_gt_int_vs_uint = is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) > sizeof(TUInt));

template <typename TInt, typename TUInt>
using bool_if_gt_int_vs_uint = std::enable_if_t<is_gt_int_vs_uint<TInt, TUInt>, bool>;

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> greaterOpTmpl(TInt a, TUInt b)
{
    return bigint_cast<TInt>(a) > bigint_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> greaterOpTmpl(TUInt a, TInt b)
{
    using CastA = std::conditional_t<is_big_int_v<TInt> && std::is_same_v<TUInt, DB::UInt128>, DB::UInt256, TInt>;

    return bigint_cast<CastA>(a) > b;
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TInt a, TUInt b)
{
    return bigint_cast<TInt>(a) == bigint_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TUInt a, TInt b)
{
    return bigint_cast<TInt>(a) == bigint_cast<TInt>(b);
}


// Case 3a. Comparison via conversion to double.
template <typename TAInt, typename TAFloat>
using bool_if_double_can_be_used
    = std::enable_if_t<is_integer_v<TAInt> && (sizeof(TAInt) <= 4) && std::is_floating_point_v<TAFloat>, bool>;

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> greaterOpTmpl(TAInt a, TAFloat b)
{
    return static_cast<double>(a) > static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> greaterOpTmpl(TAFloat a, TAInt b)
{
    return static_cast<double>(a) > static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> equalsOpTmpl(TAInt a, TAFloat b)
{
    return static_cast<double>(a) == static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> equalsOpTmpl(TAFloat a, TAInt b)
{
    return static_cast<double>(a) == static_cast<double>(b);
}

// Big integers vs Float (not equal in any case for now, until big floats are introduced?)
template <typename TABigInt, typename TAFloat>
constexpr bool if_big_int_vs_float = (is_big_int_v<TABigInt> && std::is_floating_point_v<TAFloat>);

template <typename TABigInt, typename TAFloat>
using bool_if_big_int_vs_float = std::enable_if_t<if_big_int_vs_float<TABigInt, TAFloat>, bool>;

template <typename TABigInt, typename TAFloat>
inline bool_if_big_int_vs_float<TABigInt, TAFloat> greaterOpTmpl(TAFloat, TABigInt)
{
    return false;
}

template <typename TABigInt, typename TAFloat>
inline bool_if_big_int_vs_float<TABigInt, TAFloat> greaterOpTmpl(TABigInt, TAFloat)
{
    return false;
}

template <typename TABigInt, typename TAFloat>
inline bool_if_big_int_vs_float<TABigInt, TAFloat> equalsOpTmpl(TAFloat, TABigInt)
{
    return false;
}

template <typename TABigInt, typename TAFloat>
inline bool_if_big_int_vs_float<TABigInt, TAFloat> equalsOpTmpl(TABigInt, TAFloat)
{
    return false;
}

/* Final realiztions */


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> greaterOp(A a, B b)
{
    return greaterOpTmpl(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> greaterOp(A a, B b)
{
    using CastA1 = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB1 = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, DB::UInt128>, B, CastA1>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, DB::UInt128>, A, CastB1>;

    if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        return bigint_cast<CastA>(a) > bigint_cast<CastB>(b);
    else
        return a > b;
}

// Case 3b. 64-bit integers vs floats comparison.
// See hint at https://github.com/JuliaLang/julia/issues/257 (but it doesn't work properly for -2**63)

constexpr DB::Int64 MAX_INT64_WITH_EXACT_FLOAT64_REPR = 9007199254740992LL; // 2^53

template <>
inline bool greaterOp<DB::Float64, DB::Int64>(DB::Float64 f, DB::Int64 i)
{
    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f > static_cast<DB::Float64>(i);

    return (f >= static_cast<DB::Float64>(std::numeric_limits<DB::Int64>::max())) // rhs is 2**63 (not 2^63 - 1)
            || (f > static_cast<DB::Float64>(std::numeric_limits<DB::Int64>::min()) && static_cast<DB::Int64>(f) > i);
}

template <>
inline bool greaterOp<DB::Int64, DB::Float64>(DB::Int64 i, DB::Float64 f)
{
    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f < static_cast<DB::Float64>(i);

    return (f < static_cast<DB::Float64>(std::numeric_limits<DB::Int64>::min()))
            || (f < static_cast<DB::Float64>(std::numeric_limits<DB::Int64>::max()) && i > static_cast<DB::Int64>(f));
}

template <>
inline bool greaterOp<DB::Float64, DB::UInt64>(DB::Float64 f, DB::UInt64 u)
{
    if (u <= static_cast<DB::UInt64>(MAX_INT64_WITH_EXACT_FLOAT64_REPR))
        return f > static_cast<DB::Float64>(u);

    return (f >= static_cast<DB::Float64>(std::numeric_limits<DB::UInt64>::max()))
            || (f >= 0 && static_cast<DB::UInt64>(f) > u);
}

template <>
inline bool greaterOp<DB::UInt64, DB::Float64>(DB::UInt64 u, DB::Float64 f)
{
    if (u <= static_cast<DB::UInt64>(MAX_INT64_WITH_EXACT_FLOAT64_REPR))
        return static_cast<DB::Float64>(u) > f;

    return (f < 0)
            || (f < static_cast<DB::Float64>(std::numeric_limits<DB::UInt64>::max()) && u > static_cast<UInt64>(f));
}

// Case 3b for float32
template <>
inline bool greaterOp<DB::Float32, DB::Int64>(DB::Float32 f, DB::Int64 i)
{
    return greaterOp(static_cast<DB::Float64>(f), i);
}

template <>
inline bool greaterOp<DB::Int64, DB::Float32>(DB::Int64 i, DB::Float32 f)
{
    return greaterOp(i, static_cast<DB::Float64>(f));
}

template <>
inline bool greaterOp<DB::Float32, DB::UInt64>(DB::Float32 f, DB::UInt64 u)
{
    return greaterOp(static_cast<DB::Float64>(f), u);
}

template <>
inline bool greaterOp<DB::UInt64, DB::Float32>(DB::UInt64 u, DB::Float32 f)
{
    return greaterOp(u, static_cast<DB::Float64>(f));
}

template <>
inline bool greaterOp<DB::Float64, DB::UInt128>(DB::Float64 f, DB::UInt128 u)
{
    return u.low == 0 && greaterOp(f, u.high);
}

template <>
inline bool greaterOp<DB::UInt128, DB::Float64>(DB::UInt128 u, DB::Float64 f)
{
    return u.low != 0 || greaterOp(u.high, f);
}

template <>
inline bool greaterOp<DB::Float32, DB::UInt128>(DB::Float32 f, DB::UInt128 u)
{
    return greaterOp(static_cast<DB::Float64>(f), u);
}

template <>
inline bool greaterOp<DB::UInt128, DB::Float32>(DB::UInt128 u, DB::Float32 f)
{
    return greaterOp(u, static_cast<DB::Float64>(f));
}

template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> equalsOp(A a, B b)
{
    return equalsOpTmpl(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> equalsOp(A a, B b)
{
    using LargestType = std::conditional_t<(sizeof(A) > sizeof(B)) || ((sizeof(A) == sizeof(B)) && !std::is_same_v<A, DB::UInt128>), A, B>;

    return bigint_cast<LargestType>(a) == bigint_cast<LargestType>(b);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Float64, DB::UInt64>(DB::Float64 f, DB::UInt64 u)
{
    return static_cast<DB::UInt64>(f) == u && f == static_cast<DB::Float64>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::UInt64, DB::Float64>(DB::UInt64 u, DB::Float64 f)
{
    return u == static_cast<DB::UInt64>(f) && static_cast<DB::Float64>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Float64, DB::Int64>(DB::Float64 f, DB::Int64 u)
{
    return static_cast<DB::Int64>(f) == u && f == static_cast<DB::Float64>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Int64, DB::Float64>(DB::Int64 u, DB::Float64 f)
{
    return u == static_cast<DB::Int64>(f) && static_cast<DB::Float64>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Float32, DB::UInt64>(DB::Float32 f, DB::UInt64 u)
{
    return static_cast<DB::UInt64>(f) == u && f == static_cast<DB::Float32>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::UInt64, DB::Float32>(DB::UInt64 u, DB::Float32 f)
{
    return u == static_cast<DB::UInt64>(f) && static_cast<DB::Float32>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Float32, DB::Int64>(DB::Float32 f, DB::Int64 u)
{
    return static_cast<DB::Int64>(f) == u && f == static_cast<DB::Float32>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::Int64, DB::Float32>(DB::Int64 u, DB::Float32 f)
{
    return u == static_cast<DB::Int64>(f) && static_cast<DB::Float32>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<DB::UInt128, DB::Float64>(DB::UInt128 u, DB::Float64 f)
{
    return u.low == 0 && equalsOp(static_cast<UInt64>(u.high), f);
}

template <>
inline bool equalsOp<DB::UInt128, DB::Float32>(DB::UInt128 u, DB::Float32 f)
{
    return equalsOp(u, static_cast<DB::Float64>(f));
}

template <>
inline bool equalsOp<DB::Float64, DB::UInt128>(DB::Float64 f, DB::UInt128 u)
{
    return equalsOp(u, f);
}

template <>
inline bool equalsOp<DB::Float32, DB::UInt128>(DB::Float32 f, DB::UInt128 u)
{
    return equalsOp(static_cast<DB::Float64>(f), u);
}

inline bool NO_SANITIZE_UNDEFINED greaterOp(DB::Int128 i, DB::Float64 f)
{
    static constexpr Int128 min_int128 = minInt128();
    static constexpr Int128 max_int128 = maxInt128();

    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return static_cast<DB::Float64>(i) > f;

    return (f < static_cast<DB::Float64>(min_int128))
        || (f < static_cast<DB::Float64>(max_int128) && i > static_cast<DB::Int128>(f));
}

inline bool NO_SANITIZE_UNDEFINED greaterOp(DB::Float64 f, DB::Int128 i)
{
    static constexpr Int128 min_int128 = minInt128();
    static constexpr Int128 max_int128 = maxInt128();

    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f > static_cast<DB::Float64>(i);

    return (f >= static_cast<DB::Float64>(max_int128))
        || (f > static_cast<DB::Float64>(min_int128) && static_cast<DB::Int128>(f) > i);
}

inline bool greaterOp(DB::Int128 i, DB::Float32 f) { return greaterOp(i, static_cast<DB::Float64>(f)); }
inline bool greaterOp(DB::Float32 f, DB::Int128 i) { return greaterOp(static_cast<DB::Float64>(f), i); }

inline bool NO_SANITIZE_UNDEFINED equalsOp(DB::Int128 i, DB::Float64 f) { return i == static_cast<DB::Int128>(f) && static_cast<DB::Float64>(i) == f; }
inline bool NO_SANITIZE_UNDEFINED equalsOp(DB::Int128 i, DB::Float32 f) { return i == static_cast<DB::Int128>(f) && static_cast<DB::Float32>(i) == f; }
inline bool equalsOp(DB::Float64 f, DB::Int128 i) { return equalsOp(i, f); }
inline bool equalsOp(DB::Float32 f, DB::Int128 i) { return equalsOp(i, f); }

template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> notEqualsOp(A a, B b)
{
    return !equalsOp(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> notEqualsOp(A a, B b)
{
    using CastA1 = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB1 = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, DB::UInt128>, B, CastA1>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, DB::UInt128>, A, CastB1>;

    if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        return bigint_cast<CastA>(a) != bigint_cast<CastB>(b);
    else
        return a != b;
}


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> lessOp(A a, B b)
{
    return greaterOp(b, a);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> lessOp(A a, B b)
{
    using CastA1 = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB1 = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, DB::UInt128>, B, CastA1>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, DB::UInt128>, A, CastB1>;

    if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        return bigint_cast<CastA>(a) < bigint_cast<CastB>(b);
    else
        return a < b;
}


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> lessOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;
    return !greaterOp(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> lessOrEqualsOp(A a, B b)
{
    using CastA1 = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB1 = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, DB::UInt128>, B, CastA1>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, DB::UInt128>, A, CastB1>;

    if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        return bigint_cast<CastA>(a) <= bigint_cast<CastB>(b);
    else
        return a <= b;
}


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> greaterOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;
    return !greaterOp(b, a);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> greaterOrEqualsOp(A a, B b)
{
    using CastA1 = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, UInt8>, uint8_t, A>;
    using CastB1 = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, UInt8>, uint8_t, B>;

    using CastA = std::conditional_t<is_big_int_v<B> && std::is_same_v<A, DB::UInt128>, B, CastA1>;
    using CastB = std::conditional_t<is_big_int_v<A> && std::is_same_v<B, DB::UInt128>, A, CastB1>;

    if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        return bigint_cast<CastA>(a) >= bigint_cast<CastB>(b);
    else
        return a >= b;
}

/// Converts numeric to an equal numeric of other type.
template <typename From, typename To>
inline bool NO_SANITIZE_UNDEFINED convertNumeric(From value, To & result)
{
    /// If the type is actually the same it's not necessary to do any checks.
    if constexpr (std::is_same_v<From, To>)
    {
        result = value;
        return true;
    }

    /// Note that NaNs doesn't compare equal to anything, but they are still in range of any Float type.
    if (isNaN(value) && std::is_floating_point_v<To>)
    {
        result = value;
        return true;
    }

    result = static_cast<To>(value);
    return equalsOp(value, result);
}

}


namespace DB
{

template <typename A, typename B> struct EqualsOp
{
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<B, A>;

    static UInt8 apply(A a, B b) { return accurate::equalsOp(a, b); }
};

template <typename A, typename B> struct NotEqualsOp
{
    using SymmetricOp = NotEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::notEqualsOp(a, b); }
};

template <typename A, typename B> struct GreaterOp;

template <typename A, typename B> struct LessOp
{
    using SymmetricOp = GreaterOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOp(a, b); }
};

template <typename A, typename B> struct GreaterOp
{
    using SymmetricOp = LessOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOp(a, b); }
};

template <typename A, typename B> struct GreaterOrEqualsOp;

template <typename A, typename B> struct LessOrEqualsOp
{
    using SymmetricOp = GreaterOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOrEqualsOp(a, b); }
};

template <typename A, typename B> struct GreaterOrEqualsOp
{
    using SymmetricOp = LessOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOrEqualsOp(a, b); }
};

}
