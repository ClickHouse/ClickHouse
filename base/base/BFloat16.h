#pragma once

#include <bit>
#include <base/types.h>


//using BFloat16 = __bf16;

class BFloat16
{
private:
    UInt16 x = 0;

public:
    constexpr BFloat16() = default;
    constexpr BFloat16(const BFloat16 & other) = default;
    constexpr BFloat16 & operator=(const BFloat16 & other) = default;

    explicit constexpr BFloat16(const Float32 & other)
    {
        x = static_cast<UInt16>(std::bit_cast<UInt32>(other) >> 16);
    }

    template <typename T>
    explicit constexpr BFloat16(const T & other)
        : BFloat16(Float32(other))
    {
    }

    template <typename T>
    constexpr BFloat16 & operator=(const T & other)
    {
        *this = BFloat16(other);
        return *this;
    }

    explicit constexpr operator Float32() const
    {
        return std::bit_cast<Float32>(static_cast<UInt32>(x) << 16);
    }

    template <typename T>
    explicit constexpr operator T() const
    {
        return T(Float32(*this));
    }

    constexpr bool isFinite() const
    {
        return (x & 0b0111111110000000) != 0b0111111110000000;
    }

    constexpr bool isNaN() const
    {
        return !isFinite() && (x & 0b0000000001111111) != 0b0000000000000000;
    }

    constexpr bool signBit() const
    {
        return x & 0b1000000000000000;
    }

    constexpr bool operator==(const BFloat16 & other) const
    {
        return x == other.x;
    }

    constexpr bool operator!=(const BFloat16 & other) const
    {
        return x != other.x;
    }

    constexpr BFloat16 operator+(const BFloat16 & other) const
    {
        return BFloat16(Float32(*this) + Float32(other));
    }

    constexpr BFloat16 operator-(const BFloat16 & other) const
    {
        return BFloat16(Float32(*this) - Float32(other));
    }

    constexpr BFloat16 operator*(const BFloat16 & other) const
    {
        return BFloat16(Float32(*this) * Float32(other));
    }

    constexpr BFloat16 operator/(const BFloat16 & other) const
    {
        return BFloat16(Float32(*this) / Float32(other));
    }

    constexpr BFloat16 & operator+=(const BFloat16 & other)
    {
        *this = *this + other;
        return *this;
    }

    constexpr BFloat16 & operator-=(const BFloat16 & other)
    {
        *this = *this - other;
        return *this;
    }

    constexpr BFloat16 & operator*=(const BFloat16 & other)
    {
        *this = *this * other;
        return *this;
    }

    constexpr BFloat16 & operator/=(const BFloat16 & other)
    {
        *this = *this / other;
        return *this;
    }

    constexpr BFloat16 operator-() const
    {
        BFloat16 res;
        res.x = x ^ 0b1000000000000000;
        return res;
    }
};


template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator==(const BFloat16 & a, const T & b)
{
    return Float32(a) == b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator==(const T & a, const BFloat16 & b)
{
    return a == Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator!=(const BFloat16 & a, const T & b)
{
    return Float32(a) != b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator!=(const T & a, const BFloat16 & b)
{
    return a != Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator<(const BFloat16 & a, const T & b)
{
    return Float32(a) < b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator<(const T & a, const BFloat16 & b)
{
    return a < Float32(b);
}

constexpr inline bool operator<(BFloat16 a, BFloat16 b)
{
    return Float32(a) < Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator>(const BFloat16 & a, const T & b)
{
    return Float32(a) > b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator>(const T & a, const BFloat16 & b)
{
    return a > Float32(b);
}

constexpr inline bool operator>(BFloat16 a, BFloat16 b)
{
    return Float32(a) > Float32(b);
}


template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator<=(const BFloat16 & a, const T & b)
{
    return Float32(a) <= b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator<=(const T & a, const BFloat16 & b)
{
    return a <= Float32(b);
}

constexpr inline bool operator<=(BFloat16 a, BFloat16 b)
{
    return Float32(a) <= Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator>=(const BFloat16 & a, const T & b)
{
    return Float32(a) >= b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr bool operator>=(const T & a, const BFloat16 & b)
{
    return a >= Float32(b);
}

constexpr inline bool operator>=(BFloat16 a, BFloat16 b)
{
    return Float32(a) >= Float32(b);
}


template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator+(T a, BFloat16 b)
{
    return a + Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator+(BFloat16 a, T b)
{
    return Float32(a) + b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator-(T a, BFloat16 b)
{
    return a - Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator-(BFloat16 a, T b)
{
    return Float32(a) - b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator*(T a, BFloat16 b)
{
    return a * Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator*(BFloat16 a, T b)
{
    return Float32(a) * b;
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator/(T a, BFloat16 b)
{
    return a / Float32(b);
}

template <typename T>
requires(!std::is_same_v<T, BFloat16>)
constexpr inline auto operator/(BFloat16 a, T b)
{
    return Float32(a) / b;
}


namespace std
{
    inline constexpr bool isfinite(BFloat16 x) { return x.isFinite(); }
    inline constexpr bool isnan(BFloat16 x) { return x.isNaN(); }
    inline constexpr bool signbit(BFloat16 x) { return x.signBit(); }
}
