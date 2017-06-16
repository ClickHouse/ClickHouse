#include <Common/formatIPv6.h>
#include <Common/hex.h>
#include <ext/range.h>
#include <array>


namespace DB
{

/// integer logarithm, return ceil(log(value, base)) (the smallest integer greater or equal  than log(value, base)
static constexpr uint32_t int_log(const uint32_t value, const uint32_t base, const bool carry = false)
{
    return value >= base ? 1 + int_log(value / base, base, value % base || carry) : value % base > 1 || carry;
}

/// print integer in desired base, faster than sprintf
template <uint32_t base, typename T, uint32_t buffer_size = sizeof(T) * int_log(256, base, false)>
static void print_integer(char *& out, T value)
{
    if (value == 0)
        *out++ = '0';
    else
    {
        char buf[buffer_size];
        auto ptr = buf;

        while (value > 0)
        {
            *ptr++ = hexLowercase(value % base);
            value /= base;
        }

        while (ptr != buf)
            *out++ = *--ptr;
    }
}

/// print IPv4 address as %u.%u.%u.%u
static void formatIPv4(const unsigned char * src, char *& dst, UInt8 zeroed_tail_bytes_count)
{
    const auto limit = IPV4_BINARY_LENGTH - zeroed_tail_bytes_count;

    for (const auto i : ext::range(0, IPV4_BINARY_LENGTH))
    {
        UInt8 byte = (i < limit) ? src[i] : 0;
        print_integer<10, UInt8>(dst, byte);

        if (i != IPV4_BINARY_LENGTH - 1)
            *dst++ = '.';
    }
}


void formatIPv6(const unsigned char * src, char *& dst, UInt8 zeroed_tail_bytes_count)
{
    struct { int base, len; } best{-1}, cur{-1};
    std::array<uint16_t, IPV6_BINARY_LENGTH / sizeof(uint16_t)> words{};

    /** Preprocess:
        *    Copy the input (bytewise) array into a wordwise array.
        *    Find the longest run of 0x00's in src[] for :: shorthanding. */
    for (const auto i : ext::range(0, IPV6_BINARY_LENGTH - zeroed_tail_bytes_count))
        words[i / 2] |= src[i] << ((1 - (i % 2)) << 3);

    for (const auto i : ext::range(0, words.size()))
    {
        if (words[i] == 0) {
            if (cur.base == -1)
                cur.base = i, cur.len = 1;
            else
                cur.len++;
        }
        else
        {
            if (cur.base != -1)
            {
                if (best.base == -1 || cur.len > best.len)
                    best = cur;
                cur.base = -1;
            }
        }
    }

    if (cur.base != -1)
    {
        if (best.base == -1 || cur.len > best.len)
            best = cur;
    }

    if (best.base != -1 && best.len < 2)
        best.base = -1;

    /// Format the result.
    for (const int i : ext::range(0, words.size()))
    {
        /// Are we inside the best run of 0x00's?
        if (best.base != -1 && i >= best.base && i < (best.base + best.len))
        {
            if (i == best.base)
                *dst++ = ':';
            continue;
        }

        /// Are we following an initial run of 0x00s or any real hex?
        if (i != 0)
            *dst++ = ':';

        /// Is this address an encapsulated IPv4?
        if (i == 6 && best.base == 0 && (best.len == 6 || (best.len == 5 && words[5] == 0xffffu)))
        {
            formatIPv4(src + 12, dst, std::min(zeroed_tail_bytes_count, static_cast<UInt8>(IPV4_BINARY_LENGTH)));
            break;
        }

        print_integer<16>(dst, words[i]);
    }

    /// Was it a trailing run of 0x00's?
    if (best.base != -1 && (best.base + best.len) == words.size())
        *dst++ = ':';

    *dst++ = '\0';
}

}
