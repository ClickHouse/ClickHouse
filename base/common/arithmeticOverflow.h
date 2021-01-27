#pragma once

#include <common/extended_types.h>

namespace common
{
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
    inline bool addOverflow(__int128 x, __int128 y, __int128 & res)
    {
        static constexpr __int128 min_int128 = minInt128();
        static constexpr __int128 max_int128 = maxInt128();
        res = x + y;
        return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
    }

    template <>
    inline bool addOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = x + y;
        return (y > 0 && x > std::numeric_limits<Int256>::max() - y) ||
            (y < 0 && x < std::numeric_limits<Int256>::min() - y);
    }

    template <>
    inline bool addOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = x + y;
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
    inline bool subOverflow(__int128 x, __int128 y, __int128 & res)
    {
        static constexpr __int128 min_int128 = minInt128();
        static constexpr __int128 max_int128 = maxInt128();
        res = x - y;
        return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
    }

    template <>
    inline bool subOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = x - y;
        return (y < 0 && x > std::numeric_limits<Int256>::max() + y) ||
            (y > 0 && x < std::numeric_limits<Int256>::min() + y);
    }

    template <>
    inline bool subOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = x - y;
        return x < y;
    }

    template <typename T>
    inline bool mulOverflow(T x, T y, T & res)
    {
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

    template <>
    inline bool mulOverflow(Int128 x, Int128 y, Int128 & res)
    {
        res = static_cast<UInt128>(x) * static_cast<UInt128>(y);    /// Avoid signed integer overflow.
        if (!x || !y)
            return false;

        UInt128 a = (x > 0) ? x : -x;
        UInt128 b = (y > 0) ? y : -y;
        return (a * b) / b != a;
    }

    template <>
    inline bool mulOverflow(Int256 x, Int256 y, Int256 & res)
    {
        res = x * y;
        if (!x || !y)
            return false;

        Int256 a = (x > 0) ? x : -x;
        Int256 b = (y > 0) ? y : -y;
        return (a * b) / b != a;
    }

    template <>
    inline bool mulOverflow(UInt128 x, UInt128 y, UInt128 & res)
    {
        res = x * y;
        if (!x || !y)
            return false;
        return (x * y) / y != x;
    }

    template <>
    inline bool mulOverflow(UInt256 x, UInt256 y, UInt256 & res)
    {
        res = x * y;
        if (!x || !y)
            return false;
        return (x * y) / y != x;
    }
}
