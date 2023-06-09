#pragma once

#include <base/extended_types.h>
#include <base/defines.h>


namespace common
{
    /// Multiply and ignore overflow.
    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED mulIgnoreOverflow(T1 x, T2 y)
    {
        return x * y;
    }

    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED addIgnoreOverflow(T1 x, T2 y)
    {
        return x + y;
    }

    template <typename T1, typename T2>
    inline auto NO_SANITIZE_UNDEFINED subIgnoreOverflow(T1 x, T2 y)
    {
        return x - y;
    }

    template <typename T>
    inline auto NO_SANITIZE_UNDEFINED negateIgnoreOverflow(T x)
    {
        return -x;
    }

    template <typename T>
    inline bool addOverflow(T x, T y, T & res)
    {
        return __builtin_add_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(int x, int y, int & res)
    {
        return __builtin_sadd_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long x, long y, long & res)
    {
        return __builtin_saddl_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long long x, long long y, long long & res)
    {
        return __builtin_saddll_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(Int128 x, Int128 y, Int128 & res)
    {
        res = addIgnoreOverflow(x, y);
        return (y > 0 && x > std::numeric_limits<Int128>::max() - y) ||
            (y < 0 && x < std::numeric_limits<Int128>::min() - y);
    }

    template <>
    inline bool addOverflow(UInt128 x, UInt128 y, UInt128 & res)
    {
        res = addIgnoreOverflow(x, y);
        return x > std::numeric_limits<UInt128>::max() - y;
    }

    template <>
    inline bool addOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = addIgnoreOverflow(x, y);
        return (y > 0 && x > std::numeric_limits<Int256>::max() - y) ||
            (y < 0 && x < std::numeric_limits<Int256>::min() - y);
    }

    template <>
    inline bool addOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = addIgnoreOverflow(x, y);
        return x > std::numeric_limits<UInt256>::max() - y;
    }

    template <typename T>
    inline bool subOverflow(T x, T y, T & res)
    {
        return __builtin_sub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(int x, int y, int & res)
    {
        return __builtin_ssub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long x, long y, long & res)
    {
        return __builtin_ssubl_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long long x, long long y, long long & res)
    {
        return __builtin_ssubll_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(Int128 x, Int128 y, Int128 & res)
    {
        res = subIgnoreOverflow(x, y);
        return (y < 0 && x > std::numeric_limits<Int128>::max() + y) ||
            (y > 0 && x < std::numeric_limits<Int128>::min() + y);
    }

    template <>
    inline bool subOverflow(UInt128 x, UInt128 y, UInt128 & res)
    {
        res = subIgnoreOverflow(x, y);
        return x < y;
    }

    template <>
    inline bool subOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = subIgnoreOverflow(x, y);
        return (y < 0 && x > std::numeric_limits<Int256>::max() + y) ||
            (y > 0 && x < std::numeric_limits<Int256>::min() + y);
    }

    template <>
    inline bool subOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = subIgnoreOverflow(x, y);
        return x < y;
    }

    template <typename T>
    inline bool mulOverflow(T x, T y, T & res)
    {
        return __builtin_mul_overflow(x, y, &res);
    }

    template <typename T, typename U, typename R>
    inline bool mulOverflow(T x, U y, R & res)
    {
        // not built in type, wide integer
        if constexpr (is_big_int_v<T>  || is_big_int_v<R> || is_big_int_v<U>)
        {
            res = mulIgnoreOverflow<R>(x, y);
            return false;
        }
        else
            return __builtin_mul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(int x, int y, int & res)
    {
        return __builtin_smul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long x, long y, long & res)
    {
        return __builtin_smull_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long long x, long long y, long long & res)
    {
        return __builtin_smulll_overflow(x, y, &res);
    }

    /// Overflow check is not implemented for big integers.

    template <>
    inline bool mulOverflow(Int128 x, Int128 y, Int128 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(UInt128 x, UInt128 y, UInt128 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }

    template <>
    inline bool mulOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = mulIgnoreOverflow(x, y);
        return false;
    }
}
