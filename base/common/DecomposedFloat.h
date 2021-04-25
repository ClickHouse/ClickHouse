#pragma once

#include <cstdint>
#include <cstring>
#include <common/extended_types.h>


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

    DecomposedFloat(T x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    typename Traits::UInt x_uint;

    bool sign() const
    {
        return x_uint >> (Traits::bits - 1);
    }

    uint16_t exponent() const
    {
        return (x_uint >> (Traits::mantissa_bits)) & (((1ull << (Traits::exponent_bits + 1)) - 1) >> 1);
    }

    int16_t normalized_exponent() const
    {
        return int16_t(exponent()) - ((1ull << (Traits::exponent_bits - 1)) - 1);
    }

    uint64_t mantissa() const
    {
        return x_uint & ((1ull << Traits::mantissa_bits) - 1);
    }

    int64_t mantissa_with_sign() const
    {
        return sign() ? -mantissa() : mantissa();
    }

    /// NOTE Probably floating point instructions can be better.
    bool is_integer_in_representable_range() const
    {
        return x_uint == 0
            || (normalized_exponent() >= 0  /// The number is not less than one
                /// The number is inside the range where every integer has exact representation in float
                && normalized_exponent() <= static_cast<int16_t>(Traits::mantissa_bits)
                /// After multiplying by 2^exp, the fractional part becomes zero, means the number is integer
                && ((mantissa() & ((1ULL << (Traits::mantissa_bits - normalized_exponent())) - 1)) == 0));
    }


    template <typename Int>
    bool equals(Int rhs)
    {
        /// Different signs
        if ((rhs < 0) != sign())
            return false;

        if (rhs == 0 && x_uint == 0)
            return true;

        /// Fractional number
        if (normalized_exponent() < 0)
            return false;

        /// Too large number
        if (normalized_exponent() >= static_cast<int16_t>(8 * sizeof(Int) + is_signed_v<Int>))
            return false;

        using UInt = make_unsigned_t<Int>;
        UInt uint_rhs = rhs < 0 ? -rhs : rhs;

        /// Smaller octave
        if (uint_rhs < (static_cast<UInt>(1) << normalized_exponent()))
            return false;

        /// Larger octave
        if (normalized_exponent() + 1 < static_cast<int16_t>(8 * sizeof(Int) + is_signed_v<Int>)
            && uint_rhs >= (static_cast<UInt>(1) << (normalized_exponent() + 1)))
            return false;

        /// The same octave
        /// uint_rhs == 2 ^ normalized_exponent + mantissa * 2 ^ (normalized_exponent - mantissa_bits)

        /// After multiplying by 2^exp, the fractional part becomes zero, means the number is integer
        if ((mantissa() & ((1ULL << (Traits::mantissa_bits - normalized_exponent())) - 1)) != 0)
            return false;

        /// uint_rhs - 2 ^ normalized_exponent == mantissa * 2 ^ (normalized_exponent - mantissa_bits)
        return uint_rhs - (static_cast<UInt>(1) << normalized_exponent()) == (mantissa() >> (Traits::mantissa_bits - normalized_exponent()));
    }


    template <typename Int>
    bool less(Int rhs)
    {
        /// Different signs
        if (sign() && rhs >= 0)
            return true;
        if (!sign() && rhs < 0)
            return false;
        if (rhs == 0)
            return sign();

        /// Fractional number
        if (normalized_exponent() < 0)
        {
            if (!sign())
                return rhs > 0;
            else
                return rhs >= 0;
        }

        /// Too large number
        if (normalized_exponent() >= static_cast<int16_t>(8 * sizeof(Int) + is_signed_v<Int>))
            return sign();

        using UInt = make_unsigned_t<Int>;
        UInt uint_rhs = rhs < 0 ? -rhs : rhs;

        /// Smaller octave
        if (uint_rhs < (static_cast<UInt>(1) << normalized_exponent()))
            return !sign();

        /// Larger octave
        if (normalized_exponent() + 1 < static_cast<int16_t>(8 * sizeof(Int) + is_signed_v<Int>)
            && uint_rhs >= (static_cast<UInt>(1) << (normalized_exponent() + 1)))
            return sign();

        /// The same octave
        /// uint_rhs == 2 ^ normalized_exponent + mantissa * 2 ^ (normalized_exponent - mantissa_bits)

        typename Traits::UInt a = mantissa() >> (Traits::mantissa_bits - normalized_exponent());
        typename Traits::UInt b = uint_rhs - (static_cast<UInt>(1) << normalized_exponent());

        if (a < b)
            return !sign();
        if (a > b)
            return sign();

        if ((mantissa() & ((1ULL << (Traits::mantissa_bits - normalized_exponent())) - 1)) == 0)
            return !sign();
        else
            return sign();
    }


    template <typename Int>
    bool greaterOrEquals(Int rhs)
    {
        return !less(rhs);
    }

    template <typename Int>
    bool lessOrEquals(Int rhs)
    {
        return less(rhs) || equals(rhs);
    }

    template <typename Int>
    bool greater(Int rhs)
    {
        return !lessOrEquals(rhs);
    }
};


using DecomposedFloat64 = DecomposedFloat<double>;
using DecomposedFloat32 = DecomposedFloat<float>;
