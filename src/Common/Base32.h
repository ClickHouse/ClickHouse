#pragma once

#include <optional>
#include <base/types.h>
#include <base/defines.h>

namespace DB
{

struct Base32Rfc4648
{
    static constexpr char encodeChar(UInt8 c)
    {
        chassert(c < 32);
        if (c < 26)
            return 'A' + c;
        return '2' + (c - 26);
    }
    static constexpr UInt8 decodeChar(UInt8 c)
    {
        if (c >= 'A' && c <= 'Z')
            return c - 'A';

        // Handle lowercase letters the same as uppercase
        if (c >= 'a' && c <= 'z')
            return c - 'a';

        if (c >= '2' && c <= '7')
            return (c - '2') + 26;

        return 0xFF;
    }
    static constexpr Int8 padding_char = '=';
};

template <typename Traits, typename Tag>
struct Base32;

struct Base32NaiveTag;

template <typename Traits>
struct Base32<Traits, Base32NaiveTag>
{
    static size_t encodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
    {
        //  in:      [01010101] [11001100] [11110000]

        // out:      01010 | 11100 | 11001 | 11100 | 000
        //           [ 5b ]  [ 5b ]  [ 5b ]  [ 5b ] ...

        size_t ipos = 0;
        size_t opos = 0;
        uint32_t buffer = 0;
        uint8_t bits_left = 0;

        while (ipos < src_length)
        {
            buffer = (buffer << 8) | src[ipos++];
            bits_left += 8;

            while (bits_left >= 5)
            {
                dst[opos++] = Traits::encodeChar((buffer >> (bits_left - 5)) & 0x1F);
                bits_left -= 5;
            }
        }

        if (bits_left > 0)
        {
            dst[opos++] = Traits::encodeChar((buffer << (5 - bits_left)) & 0x1F);
        }

        while (opos % 8 != 0)
        {
            dst[opos++] = Traits::padding_char;
        }

        return opos;
    }

    /// This function might write into dst even if decoding fails (nullopt returned)
    static std::optional<size_t> decodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
    {
        if (src_length % 8 != 0)
        {
            return std::nullopt;
        }

        size_t dst_pos = 0;
        size_t buffer = 0;
        int bits = 0;
        size_t pad_count = 0;
        bool padding_started = false;

        for (size_t i = 0; i < src_length; ++i)
        {
            UInt8 c = src[i];

            if (c == Traits::padding_char)
            {
                padding_started = true;
                pad_count++;
                continue;
            }

            if (padding_started)
            {
                return std::nullopt; // Only padding was expected
            }

            UInt8 value = Traits::decodeChar(c);
            if (value == 0xFF)
            {
                return std::nullopt; // Invalid symbol
            }

            // Stuff in decoded bits, write out if there's enough
            buffer = (buffer << 5) | value;
            bits += 5;

            if (bits >= 8)
            {
                bits -= 8;
                dst[dst_pos++] = (buffer >> bits) & 0xFF;
            }
        }

        if (pad_count > 0)
        {
            if (!(pad_count == 1 || pad_count == 3 || pad_count == 4 || pad_count == 6))
            {
                return std::nullopt;
            }

            if (bits > 0 && (buffer & ((1 << bits) - 1)) != 0)
            {
                return std::nullopt;
            }
        }

        return dst_pos;
    }
};

inline size_t encodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    return Base32<Base32Rfc4648, Base32NaiveTag>::encodeBase32(src, src_length, dst);
}

inline std::optional<size_t> decodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    return Base32<Base32Rfc4648, Base32NaiveTag>::decodeBase32(src, src_length, dst);
}

}
