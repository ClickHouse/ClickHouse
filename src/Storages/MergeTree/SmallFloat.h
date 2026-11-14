#pragma once

#include <base/types.h>

#include <bit>

namespace DB
{

/// `SmallFloat` encodes a non-negative integer into a single byte.
/// Encoding preserves order: a larger input never produces a smaller byte.
/// Small values (below `NUM_FREE_VALUES`) are stored exactly.
/// Larger values keep only their 4 most significant bits, like a tiny floating-point number.
/// Inputs above `MAX_ENCODABLE_VALUE` saturate to 255.
///
/// It is used to store the document length (token count) of each row of a text index for BM25 scoring.
/// Because the encoding is monotonic, the minimum doc-length byte of a block can be found by comparing raw bytes.
///
/// The algorithm comes from Lucene's `SmallFloat`, with one difference: the saturation cap is 2^24 - 1 instead of `Int32` maximum.
/// The lower cap leaves more bytes for the exact range (80 instead of 24), and no realistic row comes close to 16M tokens anyway.
namespace SmallFloat
{

/// Float-like encoding for a non-negative value that preserves ordering and keeps the 4 most significant bits.
inline constexpr UInt32 toInt4(UInt64 i)
{
    const UInt32 num_bits = 64 - std::countl_zero(i);

    /// Subnormal value.
    if (num_bits < 4)
        return static_cast<UInt32>(i);

    const UInt32 shift = num_bits - 4;
    UInt32 encoded = static_cast<UInt32>(i >> shift);

    /// Clear the most significant bit, which is implicit.
    encoded &= 0x07;
    /// Encode the shift, +1 because 0 is reserved for subnormal.
    encoded |= (shift + 1) << 3;
    return encoded;
}

/// Decode a value encoded with `toInt4`.
inline constexpr UInt64 fromInt4(UInt32 i)
{
    const UInt64 bits = i & 0x07;
    const UInt32 shift = i >> 3;

    /// Subnormal value.
    if (shift == 0)
        return bits;

    return (bits | 0x08) << (shift - 1);
}

/// Inputs above this value saturate to byte 255. The encoding is order-preserving up
/// to the cap, which maps to byte 255, so clamping larger inputs to 255 keeps the byte
/// monotonic non-decreasing across the whole `UInt32` range.
inline constexpr UInt32 MAX_ENCODABLE_VALUE = (1u << 24) - 1;
inline constexpr UInt32 MAX_INT4 = toInt4(MAX_ENCODABLE_VALUE);
inline constexpr UInt32 NUM_FREE_VALUES = 255 - MAX_INT4;
static_assert(NUM_FREE_VALUES < 128, "NUM_FREE_VALUES must be less than 128");

/// Encode a non-negative integer to a byte.
/// Values below `NUM_FREE_VALUES` are exact,
/// Larger values use the order-preserving `toInt4` encoding.
inline constexpr UInt8 toInt4Byte(UInt32 i)
{
    if (i > MAX_ENCODABLE_VALUE)
        return 255;

    if (i < NUM_FREE_VALUES)
        return static_cast<UInt8>(i);

    return static_cast<UInt8>(NUM_FREE_VALUES + toInt4(i - NUM_FREE_VALUES));
}

/// Decode a value that was encoded with `toInt4Byte`.
inline constexpr UInt32 fromInt4Byte(UInt8 b)
{
    const UInt32 i = b;
    if (i < NUM_FREE_VALUES)
        return i;

    return static_cast<UInt32>(NUM_FREE_VALUES + fromInt4(i - NUM_FREE_VALUES));
}

}

}
