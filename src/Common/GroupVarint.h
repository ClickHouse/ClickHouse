#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <base/unaligned.h>

/// Group-varint encodings: a shared header describes the byte size of each value in the group, then
/// the values follow back-to-back. Compared to plain varint there are no per-byte continuation bits,
/// so encoding/decoding is branch-light and the values can be loaded/stored with single unaligned
/// machine words.
///
/// Both codecs are little-endian.
///
/// TODO: Try the standard value size ladder {1, 2, 3, ...} and compare size and performance.

namespace DB
{

/// Encoding for 4 32-bit numbers.
/// The 1-byte header holds four 2-bit codes selecting a byte size from the ladder {0, 1, 2, 4}:
/// value 0 uses 0 bytes (beyond the header), value <= 0xff uses 1 byte, value <= 0xffff uses 2 bytes,
/// everything else uses 4 bytes.
namespace GroupVarint4x32
{
    constexpr size_t MAX_SIZE = 1 + 4 * 4; // 17

    /// [0, 1, 2, 4]
    constexpr size_t codeToSize(uint8_t c) { return c + (c == 3); }
    constexpr uint8_t encodeCode(uint32_t v) { return v == 0 ? 0 : v <= 0xffu ? 1 : v <= 0xffffu ? 2 : 3; }

    /// Writes up to MAX_SIZE bytes and advances `out`.
    /// May write up to 4 bytes past the end, but never more than MAX_SIZE bytes in total.
    inline void encode(char *& out, uint32_t v0, uint32_t v1, uint32_t v2, uint32_t v3)
    {
        char * p = out;
        char * header = p;
        ++p;
        uint8_t h = 0;
        const uint32_t values[4] = {v0, v1, v2, v3};
        for (int i = 0; i < 4; ++i)
        {
            const uint8_t code = encodeCode(values[i]);
            h |= static_cast<uint8_t>(code << (i * 2));
            memcpy(p, &values[i], sizeof(uint32_t));
            p += codeToSize(code);
        }
        *header = static_cast<char>(h);
        out = p;
    }

    /// Number of bytes that encode() wrote / decode() will read for this group.
    inline size_t getSize(const char * in)
    {
        const uint8_t h = uint8_t(*in);
        size_t size = 1;
        for (int i = 0; i < 4; ++i)
            size += codeToSize((h >> (i * 2)) & 3);
        return size;
    }

    /// Advances `in` by `getSize(in)` bytes. May read (and ignore) up to 4 bytes beyond that, but
    /// never more than MAX_SIZE in total.
    inline void decode(const char *& in, uint32_t & v0, uint32_t & v1, uint32_t & v2, uint32_t & v3)
    {
        constexpr uint32_t masks[4] = {0u, 0xffu, 0xffffu, 0xffffffffu};
        const char * p = in;
        const uint8_t h = static_cast<uint8_t>(*p);
        ++p;
        uint32_t * values[4] = {&v0, &v1, &v2, &v3};
        for (int i = 0; i < 4; ++i) // hope the compiler unrolls this loop
        {
            const uint8_t code = (h >> (i * 2)) & 3;
            uint32_t word = 0;
            memcpy(&word, p, sizeof(uint32_t)); // may read up to 4 bytes past the encoded value
            *values[i] = word & masks[code];
            p += codeToSize(code);
        }
        in = p;
    }
}

/// Similar to GroupVarint4x32 but for 8 64-bit numbers.
/// The 3-byte header holds eight 3-bit codes selecting a byte size from the ladder
/// {0, 1, 2, 3, 4, 5, 6, 8}. A value needing 7 bytes is rounded up to the 8-byte slot.
namespace GroupVarint8x64
{
    constexpr size_t MAX_SIZE = 3 + 8 * 8; // 67

    /// Code is number of bytes occupied by value, except for code 7, which means 8 bytes.
    constexpr size_t codeToSize(uint8_t c) { return c + (c == 7); }
    constexpr uint8_t encodeCode(uint64_t v)
    {
        const int bytes = (std::bit_width(v) + 7) / 8;
        return static_cast<uint8_t>(bytes - (bytes == 8));
    }

    /// Writes up to MAX_SIZE bytes and advances `out`.
    /// May write up to 8 bytes past the end, but never more than MAX_SIZE bytes in total.
    inline void encode(
        char *& out, uint64_t v0, uint64_t v1, uint64_t v2, uint64_t v3, uint64_t v4, uint64_t v5, uint64_t v6, uint64_t v7)
    {
        char * p = out;
        char * header = p;
        p += 3;
        uint32_t h = 0;
        const uint64_t values[8] = {v0, v1, v2, v3, v4, v5, v6, v7};
        for (int i = 0; i < 8; ++i)
        {
            const uint8_t code = encodeCode(values[i]);
            h |= static_cast<uint32_t>(code) << (i * 3);
            memcpy(p, &values[i], sizeof(uint64_t));
            p += codeToSize(code);
        }
        header[0] = static_cast<char>(h & 0xff);
        header[1] = static_cast<char>((h >> 8) & 0xff);
        header[2] = static_cast<char>((h >> 16) & 0xff);
        out = p;
    }

    /// Number of bytes that encode() wrote / decode() will read for this group.
    inline size_t getSize(const char * in)
    {
        const uint32_t h = unalignedLoad<uint32_t>(in);
        size_t size = 3;
        for (int i = 0; i < 8; ++i)
            size += codeToSize((h >> (i * 3)) & 7);
        return size;
    }

    /// Advances `in` by `getSize(in)` bytes. May read (and ignore) up to 8 bytes beyond that, but
    /// never more than MAX_SIZE in total.
    inline void decode(
        const char *& in, uint64_t & v0, uint64_t & v1, uint64_t & v2, uint64_t & v3, uint64_t & v4, uint64_t & v5, uint64_t & v6,
        uint64_t & v7)
    {
        constexpr uint64_t masks[8] = {
            0x0ull,
            0xffull,
            0xffffull,
            0xffffffull,
            0xffffffffull,
            0xffffffffffull,
            0xffffffffffffull,
            0xffffffffffffffffull};
        const char * p = in;
        const uint32_t h = unalignedLoad<uint32_t>(p);
        p += 3;
        uint64_t * values[8] = {&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7};
        for (int i = 0; i < 8; ++i)
        {
            const uint8_t code = (h >> (i * 3)) & 7;
            uint64_t word = 0;
            memcpy(&word, p, sizeof(uint64_t));
            *values[i] = word & masks[code];
            p += codeToSize(code);
        }
        in = p;
    }
}

}
