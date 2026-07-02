#pragma once

#include <Compression/Pcodec/Constants.h>

#include <bit>
#include <concepts>
#include <cstdint>

/** Bit-manipulation helpers and latent-type traits for the pcodec port.
  *
  * A "latent" in pcodec is an unsigned integer (u8/u16/u32/u64) that a `Number` is decomposed
  * into. Ported from tmp/pcodec_ref/pco/src/{bits.rs,read_write_uint.rs,data_types/latent_priv.rs}.
  */
namespace DB::Pcodec
{

template <typename U>
concept Latent = std::same_as<U, uint8_t> || std::same_as<U, uint16_t> || std::same_as<U, uint32_t> || std::same_as<U, uint64_t>;

/// Number of bits in a latent type, as a `Bitlen` (u32), matching `LatentPriv::BITS`.
template <Latent U>
inline constexpr Bitlen latentBits = static_cast<Bitlen>(sizeof(U) * 8);

/// The "center" value of a latent type: `1 << (BITS - 1)`. Used by signed/float ordering and by
/// `toggleCenter`. Matches `LatentPriv::MID`.
template <Latent U>
inline constexpr U latentMid = static_cast<U>(U{1} << (latentBits<U> - 1));

/// Returns `x` with all but its lowest `n` bits cleared. Does NOT handle `n >= BITS`.
template <Latent U>
inline U lowestBitsFast(U x, Bitlen n)
{
    return x & ((U{1} << n) - U{1});
}

/// Returns `x` with all but its lowest `n` bits cleared, handling `n >= BITS` (returns `x`).
template <Latent U>
inline U lowestBits(U x, Bitlen n)
{
    if (n >= latentBits<U>)
        return x;
    return lowestBitsFast(x, n);
}

/// Number of bits needed to encode an offset in `[0, max_offset]`.
/// `bits_to_encode_offset(0) == 0`. Matches `bits::bits_to_encode_offset`.
template <Latent U>
inline Bitlen bitsToEncodeOffset(U max_offset)
{
    return latentBits<U> - static_cast<Bitlen>(std::countl_zero(max_offset));
}

/// Number of bits used to encode a bin's `offset_bits` field in the header
/// (6 for 32-bit latents, 7 for 64-bit). Matches `bits::bits_to_encode_offset_bits`.
template <Latent U>
inline constexpr Bitlen bitsToEncodeOffsetBits()
{
    return static_cast<Bitlen>(32 - std::countl_zero(latentBits<U>));
}

/// Toggle the center: `x + MID` (wrapping). Maps signed deltas centered at 0 to unsigned latents.
template <Latent U>
inline U toggleCenter(U x)
{
    return static_cast<U>(x + latentMid<U>);
}

/// The maximum number of bytes the bit reader may touch when reading a value of the given
/// precision. See `bit_reader::read_uint_at` for the derivation of the thresholds.
constexpr size_t calcMaxBytes(Bitlen precision)
{
    return precision == 0 ? 0 : static_cast<size_t>((precision + 14) / 8);
}

/// The maximum number of `u64` words the bit writer may touch when writing a value of the given
/// precision. Slightly more conservative than reading. Matches `calc_max_u64s_for_writing`.
constexpr size_t calcMaxU64s(Bitlen precision)
{
    if (precision == 0)
        return 0;
    if (precision <= 56)
        return 1;
    if (precision <= 113)
        return 2;
    return 3;
}

template <Latent U>
inline constexpr size_t maxBytesFor = calcMaxBytes(latentBits<U>);

template <Latent U>
inline constexpr size_t maxU64sFor = calcMaxU64s(latentBits<U>);

}
