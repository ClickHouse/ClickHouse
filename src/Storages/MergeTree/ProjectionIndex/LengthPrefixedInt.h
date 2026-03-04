#pragma once

#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

/// Prefix-Based Variable-Length Integer Encoding (Prefix VarInt).
///
/// This encoding is a variation of the "Length-Descriptor" VarInt,
/// originating from the SQLite 4 design and widely utilized in systems
/// like ClickHouse for high-performance serialization.
///
/// ### Key Advantages:
/// 1. **Instruction-Level Parallelism (ILP)**: Unlike LEB128 (Protobuf) which
///    has a serial data dependency on the continuation bit, this format
///    determines the total length solely from the first byte. This allows
///    the CPU to load and process payload bytes in parallel.
/// 2. **Branch-Prediction Friendly**: The implementation is loop-less and
///    unrolled, reducing CPU pipeline stalls during decoding.
/// 3. **Lexicographical Comparison**: The big-endian-like prefix structure
///    is designed to be more compatible with memcmp-based sorting and indexing
///    compared to little-endian VarInts.
///
/// ### Encoding Thresholds:
/// - [0 - 176]        : 1 byte  (Value = B0)
/// - [177 - 16560]    : 2 bytes (Value = (B0-177) * 256 + B1 + 177)
/// - [16561 - 540848] : 3 bytes (Value = (B0-241) * 65536 + (B1<<8) + B2 + 16561)
/// - [540849 - 2^24-1]: 4 bytes (Marker 249 + 3 bytes raw payload)
/// - [Up to 2^32-1]   : 5 bytes (Marker 250 + 4 bytes raw payload)
namespace LengthPrefixedInt
{

/// Encoding thresholds
static constexpr UInt32 ONE_BYTE_MAX = 176;
static constexpr UInt32 TWO_BYTE_MAX = 16560;
static constexpr UInt32 THREE_BYTE_MAX = 540848;
static constexpr UInt32 FOUR_BYTE_MAX = 16777215;  /// 2^24 - 1

/// Marker byte boundaries
static constexpr UInt8 TWO_BYTE_MARKER_START = 177;
static constexpr UInt8 TWO_BYTE_MARKER_END = 240;
static constexpr UInt8 THREE_BYTE_MARKER_START = 241;
static constexpr UInt8 THREE_BYTE_MARKER_END = 248;
static constexpr UInt8 FOUR_BYTE_MARKER = 249;
static constexpr UInt8 FIVE_BYTE_MARKER = 250;

/// Offsets for compact encodings (1-3 bytes only)
static constexpr UInt32 TWO_BYTE_OFFSET = 177;
static constexpr UInt32 THREE_BYTE_OFFSET = 16561;

inline void throwReadAfterEOF()
{
    throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after eof");
}

template <bool check_eof>
inline void readUInt32Impl(UInt32 & x, ReadBuffer & istr)
{
    if constexpr (check_eof)
        if (istr.eof()) [[unlikely]]
            throwReadAfterEOF();

    const UInt8 first_byte = *istr.position()++;

    if (first_byte <= ONE_BYTE_MAX)
    {
        x = first_byte;
        return;
    }

    if constexpr (check_eof)
        if (istr.eof()) [[unlikely]]
            throwReadAfterEOF();

    const UInt8 second_byte = *istr.position()++;

    if (first_byte <= TWO_BYTE_MARKER_END)
    {
        x = ((first_byte - TWO_BYTE_MARKER_START) << 8) + second_byte + TWO_BYTE_OFFSET;
        return;
    }

    if constexpr (check_eof)
        if (istr.eof()) [[unlikely]]
            throwReadAfterEOF();

    const UInt8 third_byte = *istr.position()++;

    if (first_byte <= THREE_BYTE_MARKER_END)
    {
        x = ((first_byte - THREE_BYTE_MARKER_START) << 16) + (second_byte << 8) + third_byte + THREE_BYTE_OFFSET;
        return;
    }

    if constexpr (check_eof)
        if (istr.eof()) [[unlikely]]
            throwReadAfterEOF();

    const UInt8 fourth_byte = *istr.position()++;

    if (first_byte == FOUR_BYTE_MARKER)
    {
        x = (second_byte << 16) | (third_byte << 8) | fourth_byte;
        return;
    }

    if constexpr (check_eof)
        if (istr.eof()) [[unlikely]]
            throwReadAfterEOF();

    const UInt8 fifth_byte = *istr.position()++;
    x = (UInt32(second_byte) << 24) | (UInt32(third_byte) << 16) | (UInt32(fourth_byte) << 8) | fifth_byte;
}

template <bool check_eof>
inline void writeUInt32Impl(UInt32 x, WriteBuffer & ostr)
{
    if (x <= ONE_BYTE_MAX)
    {
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(x);
        return;
    }

    if (x <= TWO_BYTE_MAX)
    {
        const UInt32 adjusted = x - TWO_BYTE_OFFSET;
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(TWO_BYTE_MARKER_START + (adjusted >> 8));
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(adjusted & 0xFF);
        return;
    }

    if (x <= THREE_BYTE_MAX)
    {
        const UInt32 adjusted = x - THREE_BYTE_OFFSET;
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(THREE_BYTE_MARKER_START + (adjusted >> 16));
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>((adjusted >> 8) & 0xFF);
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(adjusted & 0xFF);
        return;
    }

    if (x <= FOUR_BYTE_MAX)
    {
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = FOUR_BYTE_MARKER;
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>((x >> 16) & 0xFF);
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>((x >> 8) & 0xFF);
        if constexpr (check_eof)
            ostr.nextIfAtEnd();
        *ostr.position()++ = static_cast<UInt8>(x & 0xFF);
        return;
    }

    if constexpr (check_eof)
        ostr.nextIfAtEnd();
    *ostr.position()++ = FIVE_BYTE_MARKER;
    if constexpr (check_eof)
        ostr.nextIfAtEnd();
    *ostr.position()++ = static_cast<UInt8>((x >> 24) & 0xFF);
    if constexpr (check_eof)
        ostr.nextIfAtEnd();
    *ostr.position()++ = static_cast<UInt8>((x >> 16) & 0xFF);
    if constexpr (check_eof)
        ostr.nextIfAtEnd();
    *ostr.position()++ = static_cast<UInt8>((x >> 8) & 0xFF);
    if constexpr (check_eof)
        ostr.nextIfAtEnd();
    *ostr.position()++ = static_cast<UInt8>(x & 0xFF);
}

inline void readUInt32(UInt32 & x, ReadBuffer & istr)
{
    if (istr.available() >= 5)
        readUInt32Impl<false>(x, istr);
    else
        readUInt32Impl<true>(x, istr);
}

inline void writeUInt32(UInt32 x, WriteBuffer & ostr)
{
    if (ostr.available() >= 5)
        writeUInt32Impl<false>(x, ostr);
    else
        writeUInt32Impl<true>(x, ostr);
}

inline size_t getLengthOfUInt32(UInt32 x)
{
    if (x <= ONE_BYTE_MAX)
        return 1;
    if (x <= TWO_BYTE_MAX)
        return 2;
    if (x <= THREE_BYTE_MAX)
        return 3;
    if (x <= FOUR_BYTE_MAX)
        return 4;
    return 5;
}

}

}
