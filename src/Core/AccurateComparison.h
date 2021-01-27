#pragma once

#include <cmath>
#include <limits>
#include "Defines.h"
#include "Types.h"
#include <common/extended_types.h>
#include <Common/NaNUtils.h>

/** Preceptually-correct number comparisons.
  * Example: Int8(-1) != UInt8(255)
*/

namespace accurate
{

using namespace DB;

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

// Case 2a. Are params IntXX and UIntYY and sizeof(IntXX) <= sizeof(UIntYY) (in such case will use accurate compare)
template <typename TInt, typename TUInt>
constexpr bool is_le_int_vs_uint = is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) <= sizeof(TUInt));

static_assert(is_le_int_vs_uint<Int128, UInt128>);
static_assert(is_le_int_vs_uint<Int128, UInt256>);

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
    return static_cast<TInt>(a) > static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> greaterOpTmpl(TUInt a, TInt b)
{
    return static_cast<TInt>(a) > static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TInt a, TUInt b)
{
    return static_cast<TInt>(a) == static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TUInt a, TInt b)
{
    return static_cast<TInt>(a) == static_cast<TInt>(b);
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

/* Final implementations */


template <typename A, typename B>
inline bool greaterOp(A a, B b)
{
    return greaterOpTmpl(a, b);
}

// Case 3b. 64-bit integers vs floats comparison.
// See hint at https://github.com/JuliaLang/julia/issues/257 (but it doesn't work properly for -2**63)

constexpr Int64 MAX_INT64_WITH_EXACT_FLOAT64_REPR = 9007199254740992LL; // 2^53

template <>
inline bool greaterOp<Float64, Int64>(Float64 f, Int64 i)
{
    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f > static_cast<Float64>(i);

    return (f >= static_cast<Float64>(std::numeric_limits<Int64>::max())) // rhs is 2**63 (not 2^63 - 1)
            || (f > static_cast<Float64>(std::numeric_limits<Int64>::min()) && static_cast<Int64>(f) > i);
}

template <>
inline bool greaterOp<Int64, Float64>(Int64 i, Float64 f)
{
    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f < static_cast<Float64>(i);

    return (f < static_cast<Float64>(std::numeric_limits<Int64>::min()))
            || (f < static_cast<Float64>(std::numeric_limits<Int64>::max()) && i > static_cast<Int64>(f));
}

template <>
inline bool greaterOp<Float64, UInt64>(Float64 f, UInt64 u)
{
    if (u <= static_cast<UInt64>(MAX_INT64_WITH_EXACT_FLOAT64_REPR))
        return f > static_cast<Float64>(u);

    return (f >= static_cast<Float64>(std::numeric_limits<UInt64>::max()))
            || (f >= 0 && static_cast<UInt64>(f) > u);
}

template <>
inline bool greaterOp<UInt64, Float64>(UInt64 u, Float64 f)
{
    if (u <= static_cast<UInt64>(MAX_INT64_WITH_EXACT_FLOAT64_REPR))
        return static_cast<Float64>(u) > f;

    return (f < 0)
            || (f < static_cast<Float64>(std::numeric_limits<UInt64>::max()) && u > static_cast<UInt64>(f));
}

// Case 3b for float32
template <> inline bool greaterOp<Float32, Int64>(Float32 f, Int64 i) { return greaterOp(static_cast<Float64>(f), i); }
template <> inline bool greaterOp<Float32, Int128>(Float32 f, Int128 i) { return greaterOp(static_cast<Float64>(f), i); }
template <> inline bool greaterOp<Float32, Int256>(Float32 f, Int256 i) { return greaterOp(static_cast<Float64>(f), i); }
template <> inline bool greaterOp<Float32, UInt64>(Float32 f, UInt64 i) { return greaterOp(static_cast<Float64>(f), i); }
template <> inline bool greaterOp<Float32, UInt128>(Float32 f, UInt128 i) { return greaterOp(static_cast<Float64>(f), i); }
template <> inline bool greaterOp<Float32, UInt256>(Float32 f, UInt256 i) { return greaterOp(static_cast<Float64>(f), i); }

template <> inline bool greaterOp<Int64, Float32>(Int64 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
template <> inline bool greaterOp<Int128, Float32>(Int128 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
template <> inline bool greaterOp<Int256, Float32>(Int256 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
template <> inline bool greaterOp<UInt64, Float32>(UInt64 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
template <> inline bool greaterOp<UInt128, Float32>(UInt128 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
template <> inline bool greaterOp<UInt256, Float32>(UInt256 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> equalsOp(A a, B b)
{
    return equalsOpTmpl(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> equalsOp(A a, B b)
{
    using LargestType = std::conditional_t<(sizeof(A) >= sizeof(B)), A, B>;

    return static_cast<LargestType>(a) == static_cast<LargestType>(b);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Float64, UInt64>(Float64 f, UInt64 u)
{
    return static_cast<UInt64>(f) == u && f == static_cast<Float64>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<UInt64, Float64>(UInt64 u, Float64 f)
{
    return u == static_cast<UInt64>(f) && static_cast<Float64>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Float64, Int64>(Float64 f, Int64 u)
{
    return static_cast<Int64>(f) == u && f == static_cast<Float64>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Int64, Float64>(Int64 u, Float64 f)
{
    return u == static_cast<Int64>(f) && static_cast<Float64>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Float32, UInt64>(Float32 f, UInt64 u)
{
    return static_cast<UInt64>(f) == u && f == static_cast<Float32>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<UInt64, Float32>(UInt64 u, Float32 f)
{
    return u == static_cast<UInt64>(f) && static_cast<Float32>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Float32, Int64>(Float32 f, Int64 u)
{
    return static_cast<Int64>(f) == u && f == static_cast<Float32>(u);
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<Int64, Float32>(Int64 u, Float32 f)
{
    return u == static_cast<Int64>(f) && static_cast<Float32>(u) == f;
}

template <>
inline bool NO_SANITIZE_UNDEFINED equalsOp<UInt128, Float64>(UInt128 u, Float64 f)
{
    return u.items[0] == 0 && equalsOp(static_cast<UInt64>(u.items[1]), f);
}

template <>
inline bool equalsOp<UInt128, Float32>(UInt128 u, Float32 f)
{
    return equalsOp(u, static_cast<Float64>(f));
}

template <>
inline bool equalsOp<Float64, UInt128>(Float64 f, UInt128 u)
{
    return equalsOp(u, f);
}

template <>
inline bool equalsOp<Float32, UInt128>(Float32 f, UInt128 u)
{
    return equalsOp(static_cast<Float64>(f), u);
}

inline bool NO_SANITIZE_UNDEFINED greaterOp(Int128 i, Float64 f)
{
    static constexpr Int128 min_int128 = minInt128();
    static constexpr Int128 max_int128 = maxInt128();

    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return static_cast<Float64>(i) > f;

    return (f < static_cast<Float64>(min_int128))
        || (f < static_cast<Float64>(max_int128) && i > static_cast<Int128>(f));
}

inline bool NO_SANITIZE_UNDEFINED greaterOp(Float64 f, Int128 i)
{
    static constexpr Int128 min_int128 = minInt128();
    static constexpr Int128 max_int128 = maxInt128();

    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return f > static_cast<Float64>(i);

    return (f >= static_cast<Float64>(max_int128))
        || (f > static_cast<Float64>(min_int128) && static_cast<Int128>(f) > i);
}

inline bool greaterOp(Int128 i, Float32 f) { return greaterOp(i, static_cast<Float64>(f)); }
inline bool greaterOp(Float32 f, Int128 i) { return greaterOp(static_cast<Float64>(f), i); }

inline bool NO_SANITIZE_UNDEFINED equalsOp(Int128 i, Float64 f) { return i == static_cast<Int128>(f) && static_cast<Float64>(i) == f; }
inline bool NO_SANITIZE_UNDEFINED equalsOp(Int128 i, Float32 f) { return i == static_cast<Int128>(f) && static_cast<Float32>(i) == f; }
inline bool equalsOp(Float64 f, Int128 i) { return equalsOp(i, f); }
inline bool equalsOp(Float32 f, Int128 i) { return equalsOp(i, f); }

template <typename A, typename B>
inline bool notEqualsOp(A a, B b)
{
    return !equalsOp(a, b);
}


template <typename A, typename B>
inline bool lessOp(A a, B b)
{
    return greaterOp(b, a);
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

    if constexpr (std::is_floating_point_v<From> && std::is_floating_point_v<To>)
    {
        /// Note that NaNs doesn't compare equal to anything, but they are still in range of any Float type.
        if (isNaN(value))
        {
            result = value;
            return true;
        }

        if (value == std::numeric_limits<From>::infinity())
        {
            result = std::numeric_limits<To>::infinity();
            return true;
        }

        if (value == -std::numeric_limits<From>::infinity())
        {
            result = -std::numeric_limits<To>::infinity();
            return true;
        }
    }

    if (accurate::greaterOp(value, std::numeric_limits<To>::max())
        || accurate::greaterOp(std::numeric_limits<To>::lowest(), value))
    {
        return false;
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
