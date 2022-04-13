#pragma once

#include <cmath>
#include <limits>
#include <base/DecomposedFloat.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <base/extended_types.h>
#include <Common/NaNUtils.h>

/** Preceptually-correct number comparisons.
  * Example: Int8(-1) != UInt8(255)
*/

namespace accurate
{

using namespace DB;


template <typename A, typename B>
bool lessOp(A a, B b)
{
    if constexpr (std::is_same_v<A, B>)
        return a < b;

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>)
        return a < b;

    /// anything vs NaN
    if (isNaN(a) || isNaN(b))
        return false;

    /// int vs int
    if constexpr (is_integer<A> && is_integer<B>)
    {
        /// same signedness
        if constexpr (is_signed_v<A> == is_signed_v<B>)
            return a < b;

        /// different signedness

        if constexpr (is_signed_v<A> && !is_signed_v<B>)
            return a < 0 || static_cast<make_unsigned_t<A>>(a) < b;

        if constexpr (!is_signed_v<A> && is_signed_v<B>)
            return b >= 0 && a < static_cast<make_unsigned_t<B>>(b);
    }

    /// int vs float
    if constexpr (is_integer<A> && std::is_floating_point_v<B>)
    {
        if constexpr (sizeof(A) <= 4)
            return static_cast<double>(a) < static_cast<double>(b);

        return DecomposedFloat<B>(b).greater(a);
    }

    if constexpr (std::is_floating_point_v<A> && is_integer<B>)
    {
        if constexpr (sizeof(B) <= 4)
            return static_cast<double>(a) < static_cast<double>(b);

        return DecomposedFloat<A>(a).less(b);
    }

    static_assert(is_integer<A> || std::is_floating_point_v<A>);
    static_assert(is_integer<B> || std::is_floating_point_v<B>);
    __builtin_unreachable();
}

template <typename A, typename B>
bool greaterOp(A a, B b)
{
    return lessOp(b, a);
}

template <typename A, typename B>
bool greaterOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;

    return !lessOp(a, b);
}

template <typename A, typename B>
bool lessOrEqualsOp(A a, B b)
{
    if (isNaN(a) || isNaN(b))
        return false;

    return !lessOp(b, a);
}

template <typename A, typename B>
bool equalsOp(A a, B b)
{
    if constexpr (std::is_same_v<A, B>)
        return a == b;

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>)
        return a == b;

    /// anything vs NaN
    if (isNaN(a) || isNaN(b))
        return false;

    /// int vs int
    if constexpr (is_integer<A> && is_integer<B>)
    {
        /// same signedness
        if constexpr (is_signed_v<A> == is_signed_v<B>)
            return a == b;

        /// different signedness

        if constexpr (is_signed_v<A> && !is_signed_v<B>)
            return a >= 0 && static_cast<make_unsigned_t<A>>(a) == b;

        if constexpr (!is_signed_v<A> && is_signed_v<B>)
            return b >= 0 && a == static_cast<make_unsigned_t<B>>(b);
    }

    /// int vs float
    if constexpr (is_integer<A> && std::is_floating_point_v<B>)
    {
        if constexpr (sizeof(A) <= 4)
            return static_cast<double>(a) == static_cast<double>(b);

        return DecomposedFloat<B>(b).equals(a);
    }

    if constexpr (std::is_floating_point_v<A> && is_integer<B>)
    {
        if constexpr (sizeof(B) <= 4)
            return static_cast<double>(a) == static_cast<double>(b);

        return DecomposedFloat<A>(a).equals(b);
    }

    /// e.g comparing UUID with integer.
    return false;
}

template <typename A, typename B>
bool notEqualsOp(A a, B b)
{
    return !equalsOp(a, b);
}

/// Converts numeric to an equal numeric of other type.
/// When `strict` is `true` check that result exactly same as input, otherwise just check overflow
template <typename From, typename To, bool strict = true>
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

    if (greaterOp(value, std::numeric_limits<To>::max())
        || lessOp(value, std::numeric_limits<To>::lowest()))
    {
        return false;
    }

    result = static_cast<To>(value);
    if constexpr (strict)
        return equalsOp(value, result);
    return true;
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
