#pragma once

#include <bit>
#include <cstring>
#include <type_traits>
#include <utility>
#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>

/** Both transposes exchange the two indices of an 8x8 tile: the byte transpose moves the byte at
  * 8 * j + b to 8 * b + j across eight consecutive lanes, and the bit transpose does the same one
  * level down, within a lane. The scalar loops below carry out either exchange one byte (or one
  * bit) at a time. The byte exchange instead becomes a single whole-vector byte shuffle, and the
  * bit exchange three mask-and-shift delta swaps per lane.
  *
  * The kernels are written with generic clang vectors, so no arch-specific code or runtime
  * dispatch is needed: the compiler lowers each permutation to the target's own shuffle sequence.
  * Bytes are addressed in native order, so the fast path also requires a little-endian build to
  * match the little-endian on-disk format; others fall back to the scalar loops.
  */
#if (((defined(__x86_64__) || defined(__i386__)) && defined(__SSE2__)) || (defined(__aarch64__) && defined(__ARM_NEON))) \
    && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define T64_CODEC_SIMD_TRANSPOSE 1
#else
#define T64_CODEC_SIMD_TRANSPOSE 0
#endif

namespace DB::T64Transpose
{

/// Shared helpers below have a single implementation used by both the scalar and SIMD kernels and called directly from the codec.
/// The two transpose kernels that genuinely diverge live in the `scalar` and `simd` namespaces, and `active` picks one at compile time.

template <typename T>
T restoreCommonBits(T value, T common_negative, T common_positive, T sign_bit)
{
    if constexpr (std::is_signed_v<T>)
    {
        if (sign_bit && !(value & sign_bit))
            return static_cast<T>(value | common_positive);
    }

    return static_cast<T>(value | common_negative);
}

template <typename T>
void transposeBytes(T value, UInt64 * matrix, UInt32 col)
{
    UInt8 * matrix8 = reinterpret_cast<UInt8 *>(matrix);
    const UInt8 * value8 = reinterpret_cast<const UInt8 *>(&value);

    if constexpr (sizeof(T) > 4)
    {
        matrix8[64 * 7 + col] = value8[7];
        matrix8[64 * 6 + col] = value8[6];
        matrix8[64 * 5 + col] = value8[5];
        matrix8[64 * 4 + col] = value8[4];
    }

    if constexpr (sizeof(T) > 2)
    {
        matrix8[64 * 3 + col] = value8[3];
        matrix8[64 * 2 + col] = value8[2];
    }

    if constexpr (sizeof(T) > 1)
        matrix8[64 * 1 + col] = value8[1];

    matrix8[64 * 0 + col] = value8[0];
}

template <typename T>
T reverseTransposeBytes(const UInt64 * matrix, UInt32 col)
{
    UInt64 value = 0;
    const auto * matrix8 = reinterpret_cast<const unsigned char *>(matrix);

    /// Byte k of the value is stored at matrix row k (each row is 64 bytes) and belongs at bit 8 * k.
    for (size_t k = 0; k < sizeof(T); ++k)
        value |= static_cast<UInt64>(matrix8[64 * k + col]) << (8 * k);

    return static_cast<T>(value);
}

/// Inverse of `transpose64x8` reading only the first `num_bits` planes (the rest are 0): bit i of plane p becomes bit p of byte i.
/// Please do not touch this function unless you really know what you are doing – it's tightly vectorised.
inline ALWAYS_INLINE void reverseTransposePlanes(UInt64 * matrix, UInt32 num_bits)
{
    /// pattern[i] = 1 << (i % 8): the bit of output byte i inside its plane byte.
    static constexpr UInt8 pattern[64] = {
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, ///
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, ///
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, ///
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, ///
    };

    /// For every output i in 0..63, if bit i of the word matrix[bit] is set, set bit bit in output[i].
    /// Below is a fancy way to do `output[i] |= ((matrix[bit] >> i) & 1) << bit;`.
    UInt8 output[64] = {};
    const auto * bytes = reinterpret_cast<const unsigned char *>(matrix);
    for (UInt32 bit = 0; bit < num_bits; ++bit)
    {
        const UInt8 weight = static_cast<UInt8>(1u << bit);

        /// Splat each plane byte over a 32-bit lane so the test loop vectorises as 64 lanes.
        UInt32 expanded[16];
        for (UInt32 lane = 0; lane < 16; ++lane)
        {
            UInt32 byte_index = lane / 2;
            if constexpr (std::endian::native == std::endian::big)
                byte_index = 7 - byte_index;
            expanded[lane] = bytes[8 * bit + byte_index] * 0x01010101u;
        }

        const auto * expanded_bytes = reinterpret_cast<const unsigned char *>(expanded);
        for (UInt32 i = 0; i < 64; ++i)
            output[i] |= (expanded_bytes[i] & pattern[i]) ? weight : 0;
    }

    memcpy(matrix, output, sizeof(output));
}


namespace scalar
{

inline ALWAYS_INLINE void transpose64x8(UInt64 * src_dst)
{
    const auto * src8 = reinterpret_cast<const UInt8 *>(src_dst);
    UInt64 dst[8] = {};

    for (UInt32 i = 0; i < 64; ++i)
    {
        UInt64 value = src8[i];
        dst[0] |= (value & 0x1) << i;
        dst[1] |= ((value >> 1) & 0x1) << i;
        dst[2] |= ((value >> 2) & 0x1) << i;
        dst[3] |= ((value >> 3) & 0x1) << i;
        dst[4] |= ((value >> 4) & 0x1) << i;
        dst[5] |= ((value >> 5) & 0x1) << i;
        dst[6] |= ((value >> 6) & 0x1) << i;
        dst[7] |= ((value >> 7) & 0x1) << i;
    }

    memcpy(src_dst, dst, 8 * sizeof(UInt64));
}

inline ALWAYS_INLINE void reverseTranspose64x8(UInt64 * src_dst)
{
    UInt8 dst8[64];

    for (UInt32 i = 0; i < 64; ++i)
    {
        dst8[i] = static_cast<UInt8>(
            ((src_dst[0] >> i) & 0x1) | (((src_dst[1] >> i) & 0x1) << 1) | (((src_dst[2] >> i) & 0x1) << 2)
            | (((src_dst[3] >> i) & 0x1) << 3) | (((src_dst[4] >> i) & 0x1) << 4) | (((src_dst[5] >> i) & 0x1) << 5)
            | (((src_dst[6] >> i) & 0x1) << 6) | (((src_dst[7] >> i) & 0x1) << 7));
    }

    memcpy(src_dst, dst8, 8 * sizeof(UInt64));
}

/// `matrix8[64 * byte + col]` = byte-th byte of `src[col]`, filling the matrix column by column.
template <typename T>
ALWAYS_INLINE void transposeMatrixBytes(const T * src, UInt64 * matrix, UInt32 tail)
{
    for (UInt32 col = 0; col < tail; ++col)
        transposeBytes(src[col], matrix, col);
}

/// Inverse of `transposeMatrixBytes`, fused with `restoreCommonBits` and the stores.
template <typename T>
ALWAYS_INLINE void
reverseTransposeMatrixBytes(const UInt64 * matrix, char * dst, UInt32 tail, T common_negative, T common_positive, T sign_bit)
{
    for (UInt32 col = 0; col < tail; ++col)
    {
        T value = reverseTransposeBytes<T>(matrix, col);
        value = restoreCommonBits(value, common_negative, common_positive, sign_bit);
        unalignedStore<T>(dst + col * sizeof(T), value);
    }
}

}


#if T64_CODEC_SIMD_TRANSPOSE
namespace simd
{

using ByteVec [[gnu::vector_size(64)]] = UInt8;

/// Move the byte at position 8 * j + b to 8 * b + j, i.e. transpose the 8x8 tile of bytes formed by
/// eight consecutive 64-bit lanes. Self-inverse, so one helper serves both directions. The vector is
/// passed by pointer: a 64-byte vector argument is split across registers without AVX-512, which
/// changes the ABI.
template <size_t... i>
ALWAYS_INLINE void transposeByteLanes(UInt64 * lanes, std::index_sequence<i...>)
{
    ByteVec vec;
    memcpy(&vec, lanes, sizeof(vec));
    vec = __builtin_shufflevector(vec, vec, (8 * (i % 8) + i / 8)...);
    memcpy(lanes, &vec, sizeof(vec));
}

inline ALWAYS_INLINE void transposeByteLanes(UInt64 * lanes)
{
    transposeByteLanes(lanes, std::make_index_sequence<64>{});
}

/// The same index exchange one level down: bit 8 * j + b of a lane moves to 8 * b + j, via three
/// delta swaps (Hacker's Delight 7-3). Also self-inverse.
inline ALWAYS_INLINE UInt64 transposeBitsInLane(UInt64 lane)
{
    lane = (lane & 0xAA55AA55AA55AA55ULL) | ((lane & 0x00AA00AA00AA00AAULL) << 7) | ((lane >> 7) & 0x00AA00AA00AA00AAULL);
    lane = (lane & 0xCCCC3333CCCC3333ULL) | ((lane & 0x0000CCCC0000CCCCULL) << 14) | ((lane >> 14) & 0x0000CCCC0000CCCCULL);
    lane = (lane & 0xF0F0F0F00F0F0F0FULL) | ((lane & 0x00000000F0F0F0F0ULL) << 28) | ((lane >> 28) & 0x00000000F0F0F0F0ULL);
    return lane;
}

inline ALWAYS_INLINE void transpose64x8(UInt64 * src_dst)
{
    /// A 64x8 bit transpose is the per-lane bit transpose followed by the byte transpose across
    /// lanes; applying the two passes in the opposite order inverts it, which is what
    /// `reverseTranspose64x8` below does. The byte pass is shared with the matrix transposes.
    for (UInt32 lane = 0; lane < 8; ++lane)
        src_dst[lane] = transposeBitsInLane(src_dst[lane]);
    transposeByteLanes(src_dst);
}

inline ALWAYS_INLINE void reverseTranspose64x8(UInt64 * src_dst)
{
    transposeByteLanes(src_dst);
    UInt64 lanes[8];
    memcpy(lanes, src_dst, sizeof(lanes));
    for (auto & lane : lanes)
        lane = transposeBitsInLane(lane);
    memcpy(src_dst, lanes, sizeof(lanes));
}

/// A full matrix of 8-byte values goes through the byte shuffle eight values at a time. One
/// iteration transposes the 8 columns whose bytes occupy one 64-byte group, then spreads the
/// resulting rows across the eight matrix lines they belong to. Other sizes and tails fall back to
/// the scalar column loop.
template <typename T>
ALWAYS_INLINE void transposeMatrixBytes(const T * src, UInt64 * matrix, UInt32 tail)
{
    if constexpr (sizeof(T) == sizeof(UInt64))
    {
        if (tail == 64)
        {
            auto * matrix8 = reinterpret_cast<UInt8 *>(matrix);
            for (UInt32 group = 0; group < 8; ++group)
            {
                UInt64 rows[8];
                memcpy(rows, src + 8 * group, sizeof(rows));
                transposeByteLanes(rows);
                for (UInt32 byte = 0; byte < 8; ++byte)
                    memcpy(matrix8 + 64 * byte + 8 * group, &rows[byte], sizeof(UInt64));
            }
            return;
        }
    }
    scalar::transposeMatrixBytes(src, matrix, tail);
}

/// A full matrix of 8-byte values goes through the byte shuffle eight values at a time, about twice
/// as fast as `reverseTransposeBytes` on NEON. Other sizes and tails fall back to the scalar loop.
template <typename T>
ALWAYS_INLINE void
reverseTransposeMatrixBytes(const UInt64 * matrix, char * dst, UInt32 tail, T common_negative, T common_positive, T sign_bit)
{
    if constexpr (sizeof(T) == sizeof(UInt64))
    {
        if (tail == 64)
        {
            for (UInt32 group = 0; group < 8; ++group)
            {
                UInt64 rows[8];
                for (UInt32 byte = 0; byte < 8; ++byte)
                    rows[byte] = matrix[8 * byte + group];
                transposeByteLanes(rows);
                for (UInt32 row = 0; row < 8; ++row)
                {
                    T value = restoreCommonBits(static_cast<T>(rows[row]), common_negative, common_positive, sign_bit);
                    unalignedStore<T>(dst + row * sizeof(T), value);
                }
                dst += 8 * sizeof(T);
            }
            return;
        }
    }
    scalar::reverseTransposeMatrixBytes(matrix, dst, tail, common_negative, common_positive, sign_bit);
}

}
#endif

/// Compile-time selection of the production kernels. Callers inline the chosen implementation.
#if T64_CODEC_SIMD_TRANSPOSE
namespace active = simd;
#else
namespace active = scalar;
#endif

}
