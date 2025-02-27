#include <Common/formatIPv6.h>
#include <base/hex.h>
#include <Common/StringUtils.h>

#include <base/range.h>
#include <array>
#include <algorithm>

namespace DB
{

/** Further we want to generate constexpr array of strings with sizes from sequence of unsigned ints [0..N)
 *  in order to use this arrey for fast conversion of unsigned integers to strings
 */
namespace detail
{
    template <unsigned... digits>
    struct ToChars
    {
        static const char value[];
        static const size_t size;
     };

    template <unsigned... digits>
    constexpr char ToChars<digits...>::value[] = {('0' + digits)..., 0};

    template <unsigned... digits>
    constexpr size_t ToChars<digits...>::size = sizeof...(digits);

    template <unsigned rem, unsigned... digits>
    struct Decompose : Decompose<rem / 10, rem % 10, digits...> {};

    template <unsigned... digits>
    struct Decompose<0, digits...> : ToChars<digits...> {};

    template <>
    struct Decompose<0> : ToChars<0> {};

    template <unsigned num>
    struct NumToString : Decompose<num> {};

    template <class T, T... ints>
    consteval std::array<std::pair<const char *, size_t>, sizeof...(ints)> str_make_array_impl(std::integer_sequence<T, ints...>)
    {
        return std::array<std::pair<const char *, size_t>, sizeof...(ints)> { std::pair<const char *, size_t> {NumToString<ints>::value, NumToString<ints>::size}... };
    }
}

/** str_make_array<N>() - generates static array of std::pair<const char *, size_t> for numbers [0..N), where:
 *      first - null-terminated string representing number
 *      second - size of the string as would returned by strlen()
 */
template <size_t N>
consteval std::array<std::pair<const char *, size_t>, N> str_make_array()
{
    return detail::str_make_array_impl(std::make_integer_sequence<int, N>{});
}

/// This will generate static array of pair<const char *, size_t> for [0..255] at compile time
extern constexpr auto one_byte_to_string_lookup_table = str_make_array<256>();

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
    struct { Int64 base, len; } best{-1, 0}, cur{-1, 0};
    std::array<UInt16, IPV6_BINARY_LENGTH / sizeof(UInt16)> words{};

    /** Preprocess:
        *    Copy the input (bytewise) array into a wordwise array.
        *    Find the longest run of 0x00's in src[] for :: shorthanding. */
    for (const auto i : collections::range(0, IPV6_BINARY_LENGTH - zeroed_tail_bytes_count))
        words[i / 2] |= src[i] << ((1 - (i % 2)) << 3);

    for (const auto i : collections::range(0, words.size()))
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
    for (const size_t i : collections::range(0, words.size()))
    {
        /// Are we inside the best run of 0x00's?
        if (best.base != -1)
        {
            size_t best_base = static_cast<size_t>(best.base);
            if (i >= best_base && i < (best_base + best.len))
            {
                if (i == best_base)
                    *dst++ = ':';
                continue;
            }
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
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            std::reverse(std::begin(ipv4_buffer), std::end(ipv4_buffer));
#endif
            formatIPv4(ipv4_buffer, dst, std::min(zeroed_tail_bytes_count, static_cast<uint8_t>(IPV4_BINARY_LENGTH)), "0");
            // formatIPv4 has already added a null-terminator for us.
            return;
        }

        printInteger<16>(dst, words[i]);
    }

    /// Was it a trailing run of 0x00's?
    if (best.base != -1 && static_cast<size_t>(best.base) + static_cast<size_t>(best.len) == words.size())
        *dst++ = ':';

    *dst++ = '\0';
}

}
