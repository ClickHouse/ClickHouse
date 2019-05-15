#pragma once

#include <cmath>
#include <limits>
#include "Defines.h"
#include "Types.h"
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
    || (std::is_integral_v<A> && std::is_integral_v<B> && !(std::is_signed_v<A> ^ std::is_signed_v<B>))
    || (std::is_same_v<A, DB::Int128> && std::is_same_v<B, DB::Int128>)
    || (std::is_integral_v<A> && std::is_same_v<B, DB::Int128>)
    || (std::is_same_v<A, DB::Int128> && std::is_integral_v<B>);
template <typename A, typename B>
using bool_if_safe_conversion = std::enable_if_t<is_safe_conversion<A, B>, bool>;
template <typename A, typename B>
using bool_if_not_safe_conversion = std::enable_if_t<!is_safe_conversion<A, B>, bool>;


/// Case 2. Are params IntXX and UIntYY ?
template <typename TInt, typename TUInt>
constexpr bool is_any_int_vs_uint = std::is_integral_v<TInt> && std::is_integral_v<TUInt> &&
                               std::is_signed_v<TInt> && std::is_unsigned_v<TUInt>;


// Case 2a. Are params IntXX and UIntYY and sizeof(IntXX) >= sizeof(UIntYY) (in such case will use accurate compare)
template <typename TInt, typename TUInt>
constexpr bool is_le_int_vs_uint = is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) <= sizeof(TUInt));

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
using bool_if_double_can_be_used = std::enable_if_t<
                                        std::is_integral_v<TAInt> && (sizeof(TAInt) <= 4) && std::is_floating_point_v<TAFloat>,
                                        bool>;

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

/* Final realiztions */


template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> greaterOp(A a, B b)
{
    return greaterOpTmpl(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> greaterOp(A a, B b)
{
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
    using LargestType = std::conditional_t<sizeof(A) >= sizeof(B), A, B>;
    return static_cast<LargestType>(a) == static_cast<LargestType>(b);
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
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;

    if (-MAX_INT64_WITH_EXACT_FLOAT64_REPR <= i && i <= MAX_INT64_WITH_EXACT_FLOAT64_REPR)
        return static_cast<DB::Float64>(i) > f;

    return (f < static_cast<DB::Float64>(min_int128))
        || (f < static_cast<DB::Float64>(max_int128) && i > static_cast<DB::Int128>(f));
}

inline bool NO_SANITIZE_UNDEFINED greaterOp(DB::Float64 f, DB::Int128 i)
{
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;

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
