#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <base/extended_types.h>


/// Allows to check the internals of IEEE-754 floating point number.

template <typename T> struct FloatTraits;

template <>
struct FloatTraits<float>
{
    using UInt = uint32_t;
    static constexpr size_t bits = 32;
    static constexpr size_t exponent_bits = 8;
    static constexpr size_t mantissa_bits = bits - exponent_bits - 1;
};

template <>
struct FloatTraits<double>
{
    using UInt = uint64_t;
    static constexpr size_t bits = 64;
    static constexpr size_t exponent_bits = 11;
    static constexpr size_t mantissa_bits = bits - exponent_bits - 1;
};


/// x = sign * (2 ^ normalized_exponent) * (1 + mantissa * 2 ^ -mantissa_bits)
/// x = sign * (2 ^ normalized_exponent + mantissa * 2 ^ (normalized_exponent - mantissa_bits))
template <typename T>
struct DecomposedFloat
{
    using Traits = FloatTraits<T>;

    explicit DecomposedFloat(T x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    typename Traits::UInt x_uint;

    bool isNegative() const
    {
        return x_uint >> (Traits::bits - 1);
    }

    /// Returns 0 for both +0. and -0.
    int sign() const
    {
        if (exponent() == 0 && mantissa() == 0)
            return 0;
        return isNegative() ? -1 : 1;
    }

    uint16_t exponent() const
    {
        return (x_uint >> (Traits::mantissa_bits)) & (((1ull << (Traits::exponent_bits + 1)) - 1) >> 1);
    }

    int16_t normalizedExponent() const
    {
        return int16_t(exponent()) - ((1ull << (Traits::exponent_bits - 1)) - 1);
    }

    uint64_t mantissa() const
    {
        return x_uint & ((1ull << Traits::mantissa_bits) - 1);
    }

    int64_t mantissaWithSign() const
    {
        return isNegative() ? -mantissa() : mantissa();
    }

    /// NOTE Probably floating point instructions can be better.
    bool isIntegerInRepresentableRange() const
    {
        return x_uint == 0
            || (normalizedExponent() >= 0  /// The number is not less than one
                /// The number is inside the range where every integer has exact representation in float
                && normalizedExponent() <= static_cast<int16_t>(Traits::mantissa_bits)
                /// After multiplying by 2^exp, the fractional part becomes zero, means the number is integer
                && ((mantissa() & ((1ULL << (Traits::mantissa_bits - normalizedExponent())) - 1)) == 0));
    }


    /// Compare float with integer of arbitrary width (both signed and unsigned are supported). Assuming two's complement arithmetic.
    /// This function is generic, big integers (128, 256 bit) are supported as well.
    /// Infinities are compared correctly. NaNs are treat similarly to infinities, so they can be less than all numbers.
    /// (note that we need total order)
    /// Returns -1, 0 or 1.
    template <typename Int>
    int compare(Int rhs) const
    {
        if (rhs == 0)
            return sign();

        /// Different signs
        if (isNegative() && rhs > 0)
            return -1;
        if (!isNegative() && rhs < 0)
            return 1;

        /// Fractional number with magnitude less than one
        if (normalizedExponent() < 0)
        {
            if (!isNegative())
                return rhs > 0 ? -1 : 1;
            return rhs >= 0 ? -1 : 1;
        }

        /// The case of the most negative integer
        if constexpr (is_signed_v<Int>)
        {
            if (rhs == std::numeric_limits<Int>::lowest())
            {
                assert(isNegative());

                if (normalizedExponent() < static_cast<int16_t>(8 * sizeof(Int) - is_signed_v<Int>))
                    return 1;
                if (normalizedExponent() > static_cast<int16_t>(8 * sizeof(Int) - is_signed_v<Int>))
                    return -1;

                if (mantissa() == 0)
                    return 0;
                return -1;
            }
        }

        /// Too large number: abs(float) > abs(rhs). Also the case with infinities and NaN.
        if (normalizedExponent() >= static_cast<int16_t>(8 * sizeof(Int) - is_signed_v<Int>))
            return isNegative() ? -1 : 1;

        using UInt = std::conditional_t<(sizeof(Int) > sizeof(typename Traits::UInt)), make_unsigned_t<Int>, typename Traits::UInt>;
        UInt uint_rhs = rhs < 0 ? -rhs : rhs;

        /// Smaller octave: abs(rhs) < abs(float)
        /// FYI, TIL: octave is also called "binade", https://en.wikipedia.org/wiki/Binade
        if (uint_rhs < (static_cast<UInt>(1) << normalizedExponent()))
            return isNegative() ? -1 : 1;

        /// Larger octave: abs(rhs) > abs(float)
        if (normalizedExponent() + 1 < static_cast<int16_t>(8 * sizeof(Int) - is_signed_v<Int>)
            && uint_rhs >= (static_cast<UInt>(1) << (normalizedExponent() + 1)))
            return isNegative() ? 1 : -1;

        /// The same octave
        /// uint_rhs == 2 ^ normalizedExponent + mantissa * 2 ^ (normalizedExponent - mantissa_bits)

        bool large_and_always_integer = normalizedExponent() >= static_cast<int16_t>(Traits::mantissa_bits);

        UInt a = large_and_always_integer
            ? static_cast<UInt>(mantissa()) << (normalizedExponent() - Traits::mantissa_bits)
            : static_cast<UInt>(mantissa()) >> (Traits::mantissa_bits - normalizedExponent());

        UInt b = uint_rhs - (static_cast<UInt>(1) << normalizedExponent());

        if (a < b)
            return isNegative() ? 1 : -1;
        if (a > b)
            return isNegative() ? -1 : 1;

        /// Float has no fractional part means that the numbers are equal.
        if (large_and_always_integer || (mantissa() & ((1ULL << (Traits::mantissa_bits - normalizedExponent())) - 1)) == 0)
            return 0;
        /// Float has fractional part means its abs value is larger.
        return isNegative() ? -1 : 1;
    }


    template <typename Int>
    bool equals(Int rhs) const
    {
        return compare(rhs) == 0;
    }

    template <typename Int>
    bool notEquals(Int rhs) const
    {
        return compare(rhs) != 0;
    }

    template <typename Int>
    bool less(Int rhs) const
    {
        return compare(rhs) < 0;
    }

    template <typename Int>
    bool greater(Int rhs) const
    {
        return compare(rhs) > 0;
    }

    template <typename Int>
    bool lessOrEquals(Int rhs) const
    {
        return compare(rhs) <= 0;
    }

    template <typename Int>
    bool greaterOrEquals(Int rhs) const
    {
        return compare(rhs) >= 0;
    }
};


using DecomposedFloat64 = DecomposedFloat<double>;
using DecomposedFloat32 = DecomposedFloat<float>;
