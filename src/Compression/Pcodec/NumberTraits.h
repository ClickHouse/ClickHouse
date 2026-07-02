#pragma once

#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>

#include <bit>
#include <cstdint>
#include <type_traits>

/** Mapping from a numeric type ("Number") to its unsigned "Latent" representation, and the
  * order-preserving bijection between them.
  *
  * Ported from tmp/pcodec_ref/pco/src/data_types/{unsigned,signed,float}.rs.
  *
  *  - Unsigned ints: identity.
  *  - Signed ints: `to = x - MIN` (wrapping), mapping MIN..MAX -> 0..2^bits-1 monotonically.
  *  - Floats: a sign-magnitude-to-ordered transform so that the latent ordering matches
  *    `total_cmp` float ordering (including -0/+0, Inf, NaN).
  *
  * `int_float_to/from_latent` is the separate bijection used by FloatMult/FloatQuant that maps
  * integer-valued floats to small integers.
  */
namespace DB::Pcodec
{

template <typename T>
struct NumberTraits;

/// --- Unsigned integers: identity mapping. ---
template <typename T>
    requires(std::is_unsigned_v<T> && std::is_integral_v<T>)
struct UnsignedTraitsBase
{
    using Latent = T;
    static Latent toLatentOrdered(T x) { return x; }
    static T fromLatentOrdered(Latent l) { return l; }
};

template <> struct NumberTraits<uint8_t> : UnsignedTraitsBase<uint8_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::U8; };
template <> struct NumberTraits<uint16_t> : UnsignedTraitsBase<uint16_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::U16; };
template <> struct NumberTraits<uint32_t> : UnsignedTraitsBase<uint32_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::U32; };
template <> struct NumberTraits<uint64_t> : UnsignedTraitsBase<uint64_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::U64; };

/// --- Signed integers: shift by MID (== -MIN) so the ordering is preserved. ---
template <typename T, typename U>
struct SignedTraitsBase
{
    using Latent = U;
    /// `(x - MIN) mod 2^bits`; since MIN == -MID, this is `(unsigned)x + MID` in modular arithmetic.
    static Latent toLatentOrdered(T x) { return static_cast<U>(static_cast<U>(x) + latentMid<U>); }
    /// Inverse: add MID again (wraps back) and reinterpret the bits as signed.
    static T fromLatentOrdered(Latent l) { return std::bit_cast<T>(static_cast<U>(l + latentMid<U>)); }
};

template <> struct NumberTraits<int8_t> : SignedTraitsBase<int8_t, uint8_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::I8; };
template <> struct NumberTraits<int16_t> : SignedTraitsBase<int16_t, uint16_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::I16; };
template <> struct NumberTraits<int32_t> : SignedTraitsBase<int32_t, uint32_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::I32; };
template <> struct NumberTraits<int64_t> : SignedTraitsBase<int64_t, uint64_t>
{ static constexpr NumberTypeByte type_byte = NumberTypeByte::I64; };

/// --- Floats: sign-magnitude to ordered-unsigned transform. ---
template <typename T, typename U, int mantissa_digits, NumberTypeByte number_type_byte>
struct FloatTraitsBase
{
    using Latent = U;
    static constexpr NumberTypeByte type_byte = number_type_byte;
    /// Number of significand bits excluding the implicit leading one (23 for f32, 52 for f64).
    static constexpr Bitlen PRECISION_BITS = mantissa_digits - 1;
    static constexpr int MANTISSA_DIGITS = mantissa_digits;
    static constexpr U SIGN_MASK = latentMid<U>;

    // Branchless: negative floats (sign bit set) -> ~bits, positive -> bits ^ sign_mask.
    // Equivalent to the if/else but vectorizable (important in the float decode/join hot loops).
    static Latent toLatentOrdered(T x)
    {
        U mem = std::bit_cast<U>(x);
        U sign = static_cast<U>(static_cast<std::make_signed_t<U>>(mem) >> (latentBits<U> - 1)); // all-ones iff negative
        return static_cast<U>(mem ^ (SIGN_MASK | sign));
    }

    static T fromLatentOrdered(Latent l)
    {
        U sign = static_cast<U>(static_cast<std::make_signed_t<U>>(l) >> (latentBits<U> - 1)); // all-ones iff was negative
        U mask = static_cast<U>(~(static_cast<U>(~SIGN_MASK) & sign));
        return std::bit_cast<T>(static_cast<U>(l ^ mask));
    }

    /// Surjectively maps a latent onto the set of integer-valued floats (e.g. 3.0, Inf, NaN).
    static T intFloatFromLatent(U l)
    {
        U mid = latentMid<U>;
        bool negative = false;
        U abs_int = 0;
        if (l >= mid)
        {
            negative = false;
            abs_int = static_cast<U>(l - mid);
        }
        else
        {
            negative = true;
            abs_int = static_cast<U>(mid - 1 - l);
        }
        U gpi = U{1} << MANTISSA_DIGITS;
        T abs_float;
        if (abs_int < gpi)
            abs_float = static_cast<T>(abs_int);
        else
            abs_float = std::bit_cast<T>(static_cast<U>(std::bit_cast<U>(static_cast<T>(gpi)) + (abs_int - gpi)));
        return negative ? -abs_float : abs_float;
    }

    /// Inverse of `intFloatFromLatent`.
    static U intFloatToLatent(T x)
    {
        T abs = x < T(0) ? -x : x;
        U gpi = U{1} << MANTISSA_DIGITS;
        T gpi_float = static_cast<T>(gpi);
        U abs_int;
        if (abs < gpi_float)
            abs_int = static_cast<U>(abs);
        else
            abs_int = static_cast<U>(gpi + (std::bit_cast<U>(abs) - std::bit_cast<U>(gpi_float)));
        // is_sign_positive: false for -0.0 too, so use the sign bit directly.
        bool positive = (std::bit_cast<U>(x) & SIGN_MASK) == 0;
        if (positive)
            return static_cast<U>(latentMid<U> + abs_int);
        // -1 to distinguish -0.0 from +0.0
        return static_cast<U>(latentMid<U> - 1 - abs_int);
    }
};

template <> struct NumberTraits<float> : FloatTraitsBase<float, uint32_t, 24, NumberTypeByte::F32> { };
template <> struct NumberTraits<double> : FloatTraitsBase<double, uint64_t, 53, NumberTypeByte::F64> { };

}
