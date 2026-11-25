#pragma once

#include <base/types.h>

#include <bit>
#include <cstring>

namespace DB
{

class WriteBuffer;

/// Skips whole 16-byte blocks of ASCII: returns the first such block with a byte of `0x80` or more, or the
/// position where fewer than 16 bytes are left. Checks 64-byte blocks first, so long ASCII runs are skipped
/// with the widest vectors of the target.
inline const char * skipASCIIBlocks(const char * p, const char * end)
{
    using Bytes = Int8 __attribute__((ext_vector_type(16)));
    using Mask = bool __attribute__((ext_vector_type(16)));

    /// `bytes >> 7` is nonzero exactly where the sign bit (`bytes < 0`) is set: a comparison would depend
    /// on `-faltivec-src-compat` on PowerPC. Only the sign bits are tested: `vpmovmskb` on x86, `cmlt`
    /// plus `umaxv` on NEON.
    auto has_non_ascii = [](Bytes bytes) { return __builtin_reduce_or(__builtin_convertvector(bytes >> 7, Mask)); };

    /// In non-ASCII text the next such byte is usually close, and then a 64-byte block would be checked
    /// in vain before the 16-byte one. The first block is checked again by the loops, so that they cover
    /// the same blocks for every length.
    if (end - p >= 16)
    {
        Bytes bytes;
        memcpy(&bytes, p, sizeof(bytes));
        if (has_non_ascii(bytes))
            return p;
    }

    for (Bytes bytes[4]; end - p >= 64; p += 64)
    {
        memcpy(bytes, p, sizeof(bytes));
        if (has_non_ascii(bytes[0] | bytes[1] | bytes[2] | bytes[3]))
            break;
    }

    for (Bytes bytes; end - p >= 16; p += 16)
    {
        memcpy(&bytes, p, sizeof(bytes));
        if (has_non_ascii(bytes))
            break;
    }

    return p;
}

/// Skips ASCII from `p`, which is ASCII, in `[begin, end)`: returns the first byte of `0x80` or more, or `end`.
inline const char * skipASCII(const char * begin, const char * p, const char * end)
{
    /// The high bit of each byte of the 8 at `at`, in memory order from the lowest bit. Runs between non-ASCII
    /// characters are short, like a space or `, `: a general register finds their end without the round trip from
    /// a vector register.
    auto non_ascii_bits = [](const char * at)
    {
        UInt64 word = 0;
        memcpy(&word, at, sizeof(word));
        if constexpr (std::endian::native == std::endian::big)
            word = std::byteswap(word);
        return word & 0x8080808080808080ULL;
    };

    if (end - p >= 8)
    {
        if (const UInt64 bits = non_ascii_bits(p))
            return p + std::countr_zero(bits) / 8;
        p = skipASCIIBlocks(p, end);

        if (end - p >= 16)
        {
            /// The block at `p` has a byte of `0x80` or more.
            using Bytes = Int8 __attribute__((ext_vector_type(16)));
            using Mask = bool __attribute__((ext_vector_type(16)));
            Bytes bytes;
            memcpy(&bytes, p, sizeof(bytes));
            return p + std::countr_zero(__builtin_bit_cast(UInt16, __builtin_convertvector(bytes >> 7, Mask)));
        }
    }

    /// Fewer than 16 bytes are left.
    if (end - p >= 8)
    {
        if (const UInt64 bits = non_ascii_bits(p))
            return p + std::countr_zero(bits) / 8;
        p += 8;
    }

    /// Fewer than 8: check them with the last 8 bytes of the input, which overlap the bytes before `p`.
    if (p < end && end - begin >= 8)
    {
        const UInt64 bits = non_ascii_bits(end - 8) >> (8 * (8 - (end - p)));
        return bits ? p + std::countr_zero(bits) / 8 : end;
    }

    while (p < end && static_cast<unsigned char>(*p) < 0x80)
        ++p;
    return p;
}

/// Writes `[begin, end)` to `out` with every invalid UTF-8 sequence replaced by U+FFFD, collapsing a run of
/// them into one. Allocates nothing of its own, unlike `WriteBufferValidUTF8`, which buffers into 4 KiB of
/// its own memory so that it can carry a sequence split across two writes. Use this where the whole value is
/// already in hand and that state is not needed.
void writeValidUTF8(const char * begin, const char * end, WriteBuffer & out);

}
