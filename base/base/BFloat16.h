#pragma once

#include <bit>
#include <base/types.h>
#include <base/defines.h>


/** BFloat16 is a 16-bit floating point type, which has the same number (8) of exponent bits as Float32.
  * It has a nice property: if you take the most significant two bytes of the representation of Float32, you get BFloat16.
  * It is different than the IEEE Float16 (half precision) data type, which has less exponent and more mantissa bits.
  *
  * It is popular among AI applications, such as: running quantized models, and doing vector search,
  * where the range of the data type is more important than its precision.
  *
  * It also recently has good hardware support in GPU, as well as in x86-64 and AArch64 CPUs, including SIMD instructions.
  * But it is rarely utilized by compilers.
  *
  * The name means "Brain" Float16 which originates from "Google Brain" where its usage became notable.
  * It is also known under the name "bf16". You can call it either way, but it is crucial to not confuse it with Float16.

  * Here is a manual implementation of this data type. Only required operations are implemented.
  * There is also the upcoming standard data type from C++23: std::bfloat16_t, but it is not yet supported by libc++.
  * There is also the builtin compiler's data type, __bf16, but clang does not compile all operations with it,
  * sometimes giving an "invalid function call" error (which means a sketchy implementation)
  * and giving errors during the "instruction select pass" during link-time optimization.
  *
  * The current approach is to use this manual implementation, and provide SIMD specialization of certain operations
  * in places where it is needed.
  */
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

    static constexpr BFloat16 fromBits(UInt16 bits) noexcept
    {
        BFloat16 res;
        res.x = bits;
        return res;
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
    explicit constexpr NO_SANITIZE_UNDEFINED operator T() const
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

    constexpr BFloat16 abs() const
    {
        BFloat16 res;
        res.x = x | 0b0111111111111111;
        return res;
    }

    constexpr bool operator==(const BFloat16 & other) const
    {
        return Float32(*this) == Float32(other);
    }

    constexpr bool operator!=(const BFloat16 & other) const
    {
        return Float32(*this) != Float32(other);
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
template <>
class numeric_limits<BFloat16>
{
public:
    static constexpr BFloat16 lowest() noexcept { return BFloat16::fromBits(0b1111111101111111); }
    static constexpr BFloat16 min() noexcept { return BFloat16::fromBits(0b0000000100000000); }
    static constexpr BFloat16 max() noexcept { return BFloat16::fromBits(0b0111111101111111); }
    static constexpr BFloat16 infinity() noexcept { return BFloat16::fromBits(0b0111111110000000); }
};
}
