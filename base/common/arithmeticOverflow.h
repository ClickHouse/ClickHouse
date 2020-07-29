#pragma once

#include <boost/multiprecision/cpp_int.hpp>

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
        static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
        static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
        res = x + y;
        return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
    }

    template <>
    inline bool addOverflow(boost::multiprecision::int128_t x,
                            boost::multiprecision::int128_t y,
                            boost::multiprecision::int128_t & res)
    {
        using boost::multiprecision::int128_t;
        static const int128_t max_int128 = (int128_t(1) << 127) + ((int128_t(1) << 127) - 1);
        static const int128_t min_int128 = -max_int128;

        res = x + y;
        return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
    }

    template <>
    inline bool addOverflow(boost::multiprecision::uint128_t x,
                            boost::multiprecision::uint128_t y,
                            boost::multiprecision::uint128_t & res)
    {
        using boost::multiprecision::uint128_t;
        static const uint128_t max_uint128 = (uint128_t(1) << 127) + ((uint128_t(1) << 127) - 1);

        res = x + y;
        return x > max_uint128 - y;
    }

    template <>
    inline bool addOverflow(boost::multiprecision::int256_t x,
                            boost::multiprecision::int256_t y,
                            boost::multiprecision::int256_t & res)
    {
        using boost::multiprecision::int256_t;
        static const int256_t max_int256 = (int256_t(1) << 255) + ((int256_t(1) << 255) - 1);
        static const int256_t min_int256 = -max_int256;

        res = x + y;
        return (y > 0 && x > max_int256 - y) || (y < 0 && x < min_int256 - y);
    }

    template <>
    inline bool addOverflow(boost::multiprecision::uint256_t x,
                            boost::multiprecision::uint256_t y,
                            boost::multiprecision::uint256_t & res)
    {
        using boost::multiprecision::uint256_t;
        static const uint256_t max_uint256 = (uint256_t(1) << 255) + ((uint256_t(1) << 255) - 1);

        res = x + y;
        return x > max_uint256 - y;
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
        static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
        static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
        res = x - y;
        return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
    }

    template <>
    inline bool subOverflow(boost::multiprecision::int128_t x,
                            boost::multiprecision::int128_t y,
                            boost::multiprecision::int128_t & res)
    {
        using boost::multiprecision::int128_t;
        static const int128_t max_int128 = (int128_t(1) << 127) + ((int128_t(1) << 127) - 1);
        static const int128_t min_int128 = -max_int128;

        res = x - y;
        return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
    }

    template <>
    inline bool subOverflow(boost::multiprecision::uint128_t x,
                            boost::multiprecision::uint128_t y,
                            boost::multiprecision::uint128_t & res)
    {
        res = x - y;
        return x < y;
    }

    template <>
    inline bool subOverflow(boost::multiprecision::int256_t x,
                            boost::multiprecision::int256_t y,
                            boost::multiprecision::int256_t & res)
    {
        using boost::multiprecision::int256_t;
        static const int256_t max_int256 = (int256_t(1) << 255) + ((int256_t(1) << 255) - 1);
        static const int256_t min_int256 = -max_int256;

        res = x - y;
        return (y < 0 && x > max_int256 + y) || (y > 0 && x < min_int256 + y);
    }

    template <>
    inline bool subOverflow(boost::multiprecision::uint256_t x,
                            boost::multiprecision::uint256_t y,
                            boost::multiprecision::uint256_t & res)
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
    inline bool mulOverflow(__int128 x, __int128 y, __int128 & res)
    {
        res = static_cast<unsigned __int128>(x) * static_cast<unsigned __int128>(y);    /// Avoid signed integer overflow.
        if (!x || !y)
            return false;

        unsigned __int128 a = (x > 0) ? x : -x;
        unsigned __int128 b = (y > 0) ? y : -y;
        return (a * b) / b != a;
    }

    template <>
    inline bool mulOverflow(boost::multiprecision::int128_t x,
                            boost::multiprecision::int128_t y,
                            boost::multiprecision::int128_t & res)
    {
        res = x * y;
        if (!x || !y)
            return false;

        boost::multiprecision::int128_t a = (x > 0) ? x : -x;
        boost::multiprecision::int128_t b = (y > 0) ? y : -y;
        return (a * b) / b != a;
    }

    template <>
    inline bool mulOverflow(boost::multiprecision::uint128_t x,
                            boost::multiprecision::uint128_t y,
                            boost::multiprecision::uint128_t & res)
    {
        res = x * y;
        if (!x || !y)
            return false;
        return (x * y) / y != x;
    }

    template <>
    inline bool mulOverflow(boost::multiprecision::int256_t x,
                            boost::multiprecision::int256_t y,
                            boost::multiprecision::int256_t & res)
    {
        res = x * y;
        if (!x || !y)
            return false;

        boost::multiprecision::int256_t a = (x > 0) ? x : -x;
        boost::multiprecision::int256_t b = (y > 0) ? y : -y;
        return (a * b) / b != a;
    }

    template <>
    inline bool mulOverflow(boost::multiprecision::uint256_t x,
                            boost::multiprecision::uint256_t y,
                            boost::multiprecision::uint256_t & res)
    {
        res = x * y;
        if (!x || !y)
            return false;
        return (x * y) / y != x;
    }
}
