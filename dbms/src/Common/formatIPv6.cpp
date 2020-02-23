#include <Common/formatIPv6.h>
#include <Common/hex.h>
#include <Common/StringUtils/StringUtils.h>

#include <ext/range.h>
#include <array>
#include <algorithm>

namespace DB
{

// To be used in formatIPv4, maps a byte to it's string form prefixed with length (so save strlen call).
extern const char one_byte_to_string_lookup_table[256][4] =
{
    {1, '0'}, {1, '1'}, {1, '2'}, {1, '3'}, {1, '4'}, {1, '5'}, {1, '6'}, {1, '7'}, {1, '8'}, {1, '9'},
    {2, '1', '0'}, {2, '1', '1'}, {2, '1', '2'}, {2, '1', '3'}, {2, '1', '4'}, {2, '1', '5'}, {2, '1', '6'}, {2, '1', '7'}, {2, '1', '8'}, {2, '1', '9'},
    {2, '2', '0'}, {2, '2', '1'}, {2, '2', '2'}, {2, '2', '3'}, {2, '2', '4'}, {2, '2', '5'}, {2, '2', '6'}, {2, '2', '7'}, {2, '2', '8'}, {2, '2', '9'},
    {2, '3', '0'}, {2, '3', '1'}, {2, '3', '2'}, {2, '3', '3'}, {2, '3', '4'}, {2, '3', '5'}, {2, '3', '6'}, {2, '3', '7'}, {2, '3', '8'}, {2, '3', '9'},
    {2, '4', '0'}, {2, '4', '1'}, {2, '4', '2'}, {2, '4', '3'}, {2, '4', '4'}, {2, '4', '5'}, {2, '4', '6'}, {2, '4', '7'}, {2, '4', '8'}, {2, '4', '9'},
    {2, '5', '0'}, {2, '5', '1'}, {2, '5', '2'}, {2, '5', '3'}, {2, '5', '4'}, {2, '5', '5'}, {2, '5', '6'}, {2, '5', '7'}, {2, '5', '8'}, {2, '5', '9'},
    {2, '6', '0'}, {2, '6', '1'}, {2, '6', '2'}, {2, '6', '3'}, {2, '6', '4'}, {2, '6', '5'}, {2, '6', '6'}, {2, '6', '7'}, {2, '6', '8'}, {2, '6', '9'},
    {2, '7', '0'}, {2, '7', '1'}, {2, '7', '2'}, {2, '7', '3'}, {2, '7', '4'}, {2, '7', '5'}, {2, '7', '6'}, {2, '7', '7'}, {2, '7', '8'}, {2, '7', '9'},
    {2, '8', '0'}, {2, '8', '1'}, {2, '8', '2'}, {2, '8', '3'}, {2, '8', '4'}, {2, '8', '5'}, {2, '8', '6'}, {2, '8', '7'}, {2, '8', '8'}, {2, '8', '9'},
    {2, '9', '0'}, {2, '9', '1'}, {2, '9', '2'}, {2, '9', '3'}, {2, '9', '4'}, {2, '9', '5'}, {2, '9', '6'}, {2, '9', '7'}, {2, '9', '8'}, {2, '9', '9'},
    {3, '1', '0', '0'}, {3, '1', '0', '1'}, {3, '1', '0', '2'}, {3, '1', '0', '3'}, {3, '1', '0', '4'}, {3, '1', '0', '5'}, {3, '1', '0', '6'}, {3, '1', '0', '7'}, {3, '1', '0', '8'}, {3, '1', '0', '9'},
    {3, '1', '1', '0'}, {3, '1', '1', '1'}, {3, '1', '1', '2'}, {3, '1', '1', '3'}, {3, '1', '1', '4'}, {3, '1', '1', '5'}, {3, '1', '1', '6'}, {3, '1', '1', '7'}, {3, '1', '1', '8'}, {3, '1', '1', '9'},
    {3, '1', '2', '0'}, {3, '1', '2', '1'}, {3, '1', '2', '2'}, {3, '1', '2', '3'}, {3, '1', '2', '4'}, {3, '1', '2', '5'}, {3, '1', '2', '6'}, {3, '1', '2', '7'}, {3, '1', '2', '8'}, {3, '1', '2', '9'},
    {3, '1', '3', '0'}, {3, '1', '3', '1'}, {3, '1', '3', '2'}, {3, '1', '3', '3'}, {3, '1', '3', '4'}, {3, '1', '3', '5'}, {3, '1', '3', '6'}, {3, '1', '3', '7'}, {3, '1', '3', '8'}, {3, '1', '3', '9'},
    {3, '1', '4', '0'}, {3, '1', '4', '1'}, {3, '1', '4', '2'}, {3, '1', '4', '3'}, {3, '1', '4', '4'}, {3, '1', '4', '5'}, {3, '1', '4', '6'}, {3, '1', '4', '7'}, {3, '1', '4', '8'}, {3, '1', '4', '9'},
    {3, '1', '5', '0'}, {3, '1', '5', '1'}, {3, '1', '5', '2'}, {3, '1', '5', '3'}, {3, '1', '5', '4'}, {3, '1', '5', '5'}, {3, '1', '5', '6'}, {3, '1', '5', '7'}, {3, '1', '5', '8'}, {3, '1', '5', '9'},
    {3, '1', '6', '0'}, {3, '1', '6', '1'}, {3, '1', '6', '2'}, {3, '1', '6', '3'}, {3, '1', '6', '4'}, {3, '1', '6', '5'}, {3, '1', '6', '6'}, {3, '1', '6', '7'}, {3, '1', '6', '8'}, {3, '1', '6', '9'},
    {3, '1', '7', '0'}, {3, '1', '7', '1'}, {3, '1', '7', '2'}, {3, '1', '7', '3'}, {3, '1', '7', '4'}, {3, '1', '7', '5'}, {3, '1', '7', '6'}, {3, '1', '7', '7'}, {3, '1', '7', '8'}, {3, '1', '7', '9'},
    {3, '1', '8', '0'}, {3, '1', '8', '1'}, {3, '1', '8', '2'}, {3, '1', '8', '3'}, {3, '1', '8', '4'}, {3, '1', '8', '5'}, {3, '1', '8', '6'}, {3, '1', '8', '7'}, {3, '1', '8', '8'}, {3, '1', '8', '9'},
    {3, '1', '9', '0'}, {3, '1', '9', '1'}, {3, '1', '9', '2'}, {3, '1', '9', '3'}, {3, '1', '9', '4'}, {3, '1', '9', '5'}, {3, '1', '9', '6'}, {3, '1', '9', '7'}, {3, '1', '9', '8'}, {3, '1', '9', '9'},
    {3, '2', '0', '0'}, {3, '2', '0', '1'}, {3, '2', '0', '2'}, {3, '2', '0', '3'}, {3, '2', '0', '4'}, {3, '2', '0', '5'}, {3, '2', '0', '6'}, {3, '2', '0', '7'}, {3, '2', '0', '8'}, {3, '2', '0', '9'},
    {3, '2', '1', '0'}, {3, '2', '1', '1'}, {3, '2', '1', '2'}, {3, '2', '1', '3'}, {3, '2', '1', '4'}, {3, '2', '1', '5'}, {3, '2', '1', '6'}, {3, '2', '1', '7'}, {3, '2', '1', '8'}, {3, '2', '1', '9'},
    {3, '2', '2', '0'}, {3, '2', '2', '1'}, {3, '2', '2', '2'}, {3, '2', '2', '3'}, {3, '2', '2', '4'}, {3, '2', '2', '5'}, {3, '2', '2', '6'}, {3, '2', '2', '7'}, {3, '2', '2', '8'}, {3, '2', '2', '9'},
    {3, '2', '3', '0'}, {3, '2', '3', '1'}, {3, '2', '3', '2'}, {3, '2', '3', '3'}, {3, '2', '3', '4'}, {3, '2', '3', '5'}, {3, '2', '3', '6'}, {3, '2', '3', '7'}, {3, '2', '3', '8'}, {3, '2', '3', '9'},
    {3, '2', '4', '0'}, {3, '2', '4', '1'}, {3, '2', '4', '2'}, {3, '2', '4', '3'}, {3, '2', '4', '4'}, {3, '2', '4', '5'}, {3, '2', '4', '6'}, {3, '2', '4', '7'}, {3, '2', '4', '8'}, {3, '2', '4', '9'},
    {3, '2', '5', '0'}, {3, '2', '5', '1'}, {3, '2', '5', '2'}, {3, '2', '5', '3'}, {3, '2', '5', '4'}, {3, '2', '5', '5'},
};

/// integer logarithm, return ceil(log(value, base)) (the smallest integer greater or equal than log(value, base)
static constexpr UInt32 intLog(const UInt32 value, const UInt32 base, const bool carry)
{
    return value >= base ? 1 + intLog(value / base, base, value % base || carry) : value % base > 1 || carry;
}

/// Print integer in desired base, faster than sprintf.
/// NOTE This is not the best way. See https://github.com/miloyip/itoa-benchmark
/// But it doesn't matter here.
template <UInt32 base, typename T>
static void printInteger(char *& out, T value)
{
    if (value == 0)
        *out++ = '0';
    else
    {
        constexpr size_t buffer_size = sizeof(T) * intLog(256, base, false);

        char buf[buffer_size];
        auto ptr = buf;

        while (value > 0)
        {
            *ptr = hexDigitLowercase(value % base);
            ++ptr;
            value /= base;
        }

        /// Copy to out reversed.
        while (ptr != buf)
        {
            --ptr;
            *out = *ptr;
            ++out;
        }
    }
}

void formatIPv6(const unsigned char * src, char *& dst, uint8_t zeroed_tail_bytes_count)
{
    struct { int base, len; } best{-1, 0}, cur{-1, 0};
    std::array<UInt16, IPV6_BINARY_LENGTH / sizeof(UInt16)> words{};

    /** Preprocess:
        *    Copy the input (bytewise) array into a wordwise array.
        *    Find the longest run of 0x00's in src[] for :: shorthanding. */
    for (const auto i : ext::range(0, IPV6_BINARY_LENGTH - zeroed_tail_bytes_count))
        words[i / 2] |= src[i] << ((1 - (i % 2)) << 3);

    for (const auto i : ext::range(0, words.size()))
    {
        if (words[i] == 0)
        {
            if (cur.base == -1)
            {
                cur.base = i;
                cur.len = 1;
            }
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
            uint8_t ipv4_buffer[IPV4_BINARY_LENGTH] = {0};
            memcpy(ipv4_buffer, src + 12, IPV4_BINARY_LENGTH);
            // Due to historical reasons formatIPv4() takes ipv4 in BE format, but inside ipv6 we store it in LE-format.
            std::reverse(std::begin(ipv4_buffer), std::end(ipv4_buffer));

            formatIPv4(ipv4_buffer, dst, std::min(zeroed_tail_bytes_count, static_cast<uint8_t>(IPV4_BINARY_LENGTH)), "0");
            // formatIPv4 has already added a null-terminator for us.
            return;
        }

        printInteger<16>(dst, words[i]);
    }

    /// Was it a trailing run of 0x00's?
    if (best.base != -1 && size_t(best.base) + size_t(best.len) == words.size())
        *dst++ = ':';

    *dst++ = '\0';
}

}
