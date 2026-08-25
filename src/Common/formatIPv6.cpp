#include <Common/formatIPv6.h>

#include <base/range.h>
#include <array>
#include <algorithm>

#if defined(__SSE4_1__)
#include <x86intrin.h>
#endif


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
    static consteval std::array<std::pair<const char *, size_t>, sizeof...(ints)> str_make_array_impl(std::integer_sequence<T, ints...>)
    {
        return std::array<std::pair<const char *, size_t>, sizeof...(ints)> { std::pair<const char *, size_t> {NumToString<ints>::value, NumToString<ints>::size}... };
    }
}

/** str_make_array<N>() - generates static array of std::pair<const char *, size_t> for numbers [0..N), where:
 *      first - null-terminated string representing number
 *      second - size of the string as would returned by strlen()
 */
template <size_t N>
static consteval std::array<std::pair<const char *, size_t>, N> str_make_array()
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
            if constexpr (std::endian::native == std::endian::little)
                std::ranges::reverse(ipv4_buffer);
            formatIPv4(ipv4_buffer, dst, std::min(zeroed_tail_bytes_count, static_cast<uint8_t>(IPV4_BINARY_LENGTH)), "0");
            return;
        }

        printInteger<16>(dst, words[i]);
    }

    /// Was it a trailing run of 0x00's?
    if (best.base != -1 && static_cast<size_t>(best.base) + static_cast<size_t>(best.len) == words.size())
        *dst++ = ':';
}

#if defined(__SSE4_1__)

// Author: Daniel Lemire, public domain.
// https://github.com/lemire/Code-used-on-Daniel-Lemire-s-blog/blob/master/2023/06/08/include/sse_inet_aton.h
//
// convert IPv4 from text to binary form.
//
// ipv4_string points to a character string containing an IPv4 network address in dotted-decimal format
// "ddd.ddd.ddd.ddd" of length ipv4_string_length (the string does not have to be null terminated),
// where ddd is a decimal number of up to three digits in the range 0 to 255.
// The address is converted to a 32-bit integer (destination) (in  network byte order).
//
// Important: the function will systematically read 16 bytes at the provided address (ipv4_string). However,
// only the first ipv4_string_length bytes are processed.
//
// returns 1 on success (network address was successfully converted).
//
// This function assumes that the processor supports SSE 4.1 instructions or better. That's true of most
// processors in operation today (June 2023).
//
int parseIPv4SSE(const char * ipv4_string, const size_t ipv4_string_length, uint32_t * destination)
{
    static const uint8_t patterns_id[256] =
        {
            38,  65,  255, 56,  73,  255, 255, 255, 255, 255, 255, 3,   255, 255, 6,
            255, 255, 9,   255, 27,  255, 12,  30,  255, 255, 255, 255, 15,  255, 33,
            255, 255, 255, 255, 18,  36,  255, 255, 255, 54,  21,  255, 39,  255, 255,
            57,  255, 255, 255, 255, 255, 255, 255, 255, 24,  42,  255, 255, 255, 60,
            255, 255, 255, 255, 255, 255, 255, 255, 45,  255, 255, 63,  255, 255, 255,
            255, 255, 255, 255, 255, 255, 48,  53,  255, 255, 66,  71,  255, 255, 16,
            255, 34,  255, 255, 255, 255, 255, 255, 255, 52,  255, 255, 22,  70,  40,
            255, 255, 58,  51,  255, 255, 69,  255, 255, 255, 255, 255, 255, 255, 255,
            255, 5,   255, 255, 255, 255, 255, 255, 11,  29,  46,  255, 255, 64,  255,
            255, 72,  0,   77,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 76,  255, 255, 255, 255, 255, 255, 255, 75,  255,
            80,  255, 255, 255, 26,  255, 44,  255, 7,   62,  255, 255, 25,  255, 43,
            13,  31,  61,  255, 255, 255, 255, 255, 255, 255, 255, 255, 2,   19,  37,
            255, 255, 50,  55,  79,  68,  255, 255, 255, 255, 49,  255, 255, 67,  255,
            255, 255, 255, 17,  255, 35,  78,  255, 4,   255, 255, 255, 255, 255, 255,
            10,  23,  28,  41,  255, 255, 59,  255, 255, 255, 8,   255, 255, 255, 255,
            255, 1,   14,  32,  255, 255, 255, 255, 255, 255, 255, 255, 74,  255, 47,
            20,
        };

    static const uint8_t patterns[81][16] =
        {
            {0, 128, 2, 128, 4, 128, 6, 128, 128, 128, 128, 128, 128, 128, 128, 128},
            {0, 128, 2, 128, 4, 128, 7, 6, 128, 128, 128, 128, 128, 128, 128, 6},
            {0, 128, 2, 128, 4, 128, 8, 7, 128, 128, 128, 128, 128, 128, 6, 6},
            {0, 128, 2, 128, 5, 4, 7, 128, 128, 128, 128, 128, 128, 4, 128, 128},
            {0, 128, 2, 128, 5, 4, 8, 7, 128, 128, 128, 128, 128, 4, 128, 7},
            {0, 128, 2, 128, 5, 4, 9, 8, 128, 128, 128, 128, 128, 4, 7, 7},
            {0, 128, 2, 128, 6, 5, 8, 128, 128, 128, 128, 128, 4, 4, 128, 128},
            {0, 128, 2, 128, 6, 5, 9, 8, 128, 128, 128, 128, 4, 4, 128, 8},
            {0, 128, 2, 128, 6, 5, 10, 9, 128, 128, 128, 128, 4, 4, 8, 8},
            {0, 128, 3, 2, 5, 128, 7, 128, 128, 128, 128, 2, 128, 128, 128, 128},
            {0, 128, 3, 2, 5, 128, 8, 7, 128, 128, 128, 2, 128, 128, 128, 7},
            {0, 128, 3, 2, 5, 128, 9, 8, 128, 128, 128, 2, 128, 128, 7, 7},
            {0, 128, 3, 2, 6, 5, 8, 128, 128, 128, 128, 2, 128, 5, 128, 128},
            {0, 128, 3, 2, 6, 5, 9, 8, 128, 128, 128, 2, 128, 5, 128, 8},
            {0, 128, 3, 2, 6, 5, 10, 9, 128, 128, 128, 2, 128, 5, 8, 8},
            {0, 128, 3, 2, 7, 6, 9, 128, 128, 128, 128, 2, 5, 5, 128, 128},
            {0, 128, 3, 2, 7, 6, 10, 9, 128, 128, 128, 2, 5, 5, 128, 9},
            {0, 128, 3, 2, 7, 6, 11, 10, 128, 128, 128, 2, 5, 5, 9, 9},
            {0, 128, 4, 3, 6, 128, 8, 128, 128, 128, 2, 2, 128, 128, 128, 128},
            {0, 128, 4, 3, 6, 128, 9, 8, 128, 128, 2, 2, 128, 128, 128, 8},
            {0, 128, 4, 3, 6, 128, 10, 9, 128, 128, 2, 2, 128, 128, 8, 8},
            {0, 128, 4, 3, 7, 6, 9, 128, 128, 128, 2, 2, 128, 6, 128, 128},
            {0, 128, 4, 3, 7, 6, 10, 9, 128, 128, 2, 2, 128, 6, 128, 9},
            {0, 128, 4, 3, 7, 6, 11, 10, 128, 128, 2, 2, 128, 6, 9, 9},
            {0, 128, 4, 3, 8, 7, 10, 128, 128, 128, 2, 2, 6, 6, 128, 128},
            {0, 128, 4, 3, 8, 7, 11, 10, 128, 128, 2, 2, 6, 6, 128, 10},
            {0, 128, 4, 3, 8, 7, 12, 11, 128, 128, 2, 2, 6, 6, 10, 10},
            {1, 0, 3, 128, 5, 128, 7, 128, 128, 0, 128, 128, 128, 128, 128, 128},
            {1, 0, 3, 128, 5, 128, 8, 7, 128, 0, 128, 128, 128, 128, 128, 7},
            {1, 0, 3, 128, 5, 128, 9, 8, 128, 0, 128, 128, 128, 128, 7, 7},
            {1, 0, 3, 128, 6, 5, 8, 128, 128, 0, 128, 128, 128, 5, 128, 128},
            {1, 0, 3, 128, 6, 5, 9, 8, 128, 0, 128, 128, 128, 5, 128, 8},
            {1, 0, 3, 128, 6, 5, 10, 9, 128, 0, 128, 128, 128, 5, 8, 8},
            {1, 0, 3, 128, 7, 6, 9, 128, 128, 0, 128, 128, 5, 5, 128, 128},
            {1, 0, 3, 128, 7, 6, 10, 9, 128, 0, 128, 128, 5, 5, 128, 9},
            {1, 0, 3, 128, 7, 6, 11, 10, 128, 0, 128, 128, 5, 5, 9, 9},
            {1, 0, 4, 3, 6, 128, 8, 128, 128, 0, 128, 3, 128, 128, 128, 128},
            {1, 0, 4, 3, 6, 128, 9, 8, 128, 0, 128, 3, 128, 128, 128, 8},
            {1, 0, 4, 3, 6, 128, 10, 9, 128, 0, 128, 3, 128, 128, 8, 8},
            {1, 0, 4, 3, 7, 6, 9, 128, 128, 0, 128, 3, 128, 6, 128, 128},
            {1, 0, 4, 3, 7, 6, 10, 9, 128, 0, 128, 3, 128, 6, 128, 9},
            {1, 0, 4, 3, 7, 6, 11, 10, 128, 0, 128, 3, 128, 6, 9, 9},
            {1, 0, 4, 3, 8, 7, 10, 128, 128, 0, 128, 3, 6, 6, 128, 128},
            {1, 0, 4, 3, 8, 7, 11, 10, 128, 0, 128, 3, 6, 6, 128, 10},
            {1, 0, 4, 3, 8, 7, 12, 11, 128, 0, 128, 3, 6, 6, 10, 10},
            {1, 0, 5, 4, 7, 128, 9, 128, 128, 0, 3, 3, 128, 128, 128, 128},
            {1, 0, 5, 4, 7, 128, 10, 9, 128, 0, 3, 3, 128, 128, 128, 9},
            {1, 0, 5, 4, 7, 128, 11, 10, 128, 0, 3, 3, 128, 128, 9, 9},
            {1, 0, 5, 4, 8, 7, 10, 128, 128, 0, 3, 3, 128, 7, 128, 128},
            {1, 0, 5, 4, 8, 7, 11, 10, 128, 0, 3, 3, 128, 7, 128, 10},
            {1, 0, 5, 4, 8, 7, 12, 11, 128, 0, 3, 3, 128, 7, 10, 10},
            {1, 0, 5, 4, 9, 8, 11, 128, 128, 0, 3, 3, 7, 7, 128, 128},
            {1, 0, 5, 4, 9, 8, 12, 11, 128, 0, 3, 3, 7, 7, 128, 11},
            {1, 0, 5, 4, 9, 8, 13, 12, 128, 0, 3, 3, 7, 7, 11, 11},
            {2, 1, 4, 128, 6, 128, 8, 128, 0, 0, 128, 128, 128, 128, 128, 128},
            {2, 1, 4, 128, 6, 128, 9, 8, 0, 0, 128, 128, 128, 128, 128, 8},
            {2, 1, 4, 128, 6, 128, 10, 9, 0, 0, 128, 128, 128, 128, 8, 8},
            {2, 1, 4, 128, 7, 6, 9, 128, 0, 0, 128, 128, 128, 6, 128, 128},
            {2, 1, 4, 128, 7, 6, 10, 9, 0, 0, 128, 128, 128, 6, 128, 9},
            {2, 1, 4, 128, 7, 6, 11, 10, 0, 0, 128, 128, 128, 6, 9, 9},
            {2, 1, 4, 128, 8, 7, 10, 128, 0, 0, 128, 128, 6, 6, 128, 128},
            {2, 1, 4, 128, 8, 7, 11, 10, 0, 0, 128, 128, 6, 6, 128, 10},
            {2, 1, 4, 128, 8, 7, 12, 11, 0, 0, 128, 128, 6, 6, 10, 10},
            {2, 1, 5, 4, 7, 128, 9, 128, 0, 0, 128, 4, 128, 128, 128, 128},
            {2, 1, 5, 4, 7, 128, 10, 9, 0, 0, 128, 4, 128, 128, 128, 9},
            {2, 1, 5, 4, 7, 128, 11, 10, 0, 0, 128, 4, 128, 128, 9, 9},
            {2, 1, 5, 4, 8, 7, 10, 128, 0, 0, 128, 4, 128, 7, 128, 128},
            {2, 1, 5, 4, 8, 7, 11, 10, 0, 0, 128, 4, 128, 7, 128, 10},
            {2, 1, 5, 4, 8, 7, 12, 11, 0, 0, 128, 4, 128, 7, 10, 10},
            {2, 1, 5, 4, 9, 8, 11, 128, 0, 0, 128, 4, 7, 7, 128, 128},
            {2, 1, 5, 4, 9, 8, 12, 11, 0, 0, 128, 4, 7, 7, 128, 11},
            {2, 1, 5, 4, 9, 8, 13, 12, 0, 0, 128, 4, 7, 7, 11, 11},
            {2, 1, 6, 5, 8, 128, 10, 128, 0, 0, 4, 4, 128, 128, 128, 128},
            {2, 1, 6, 5, 8, 128, 11, 10, 0, 0, 4, 4, 128, 128, 128, 10},
            {2, 1, 6, 5, 8, 128, 12, 11, 0, 0, 4, 4, 128, 128, 10, 10},
            {2, 1, 6, 5, 9, 8, 11, 128, 0, 0, 4, 4, 128, 8, 128, 128},
            {2, 1, 6, 5, 9, 8, 12, 11, 0, 0, 4, 4, 128, 8, 128, 11},
            {2, 1, 6, 5, 9, 8, 13, 12, 0, 0, 4, 4, 128, 8, 11, 11},
            {2, 1, 6, 5, 10, 9, 12, 128, 0, 0, 4, 4, 8, 8, 128, 128},
            {2, 1, 6, 5, 10, 9, 13, 12, 0, 0, 4, 4, 8, 8, 128, 12},
            {2, 1, 6, 5, 10, 9, 14, 13, 0, 0, 4, 4, 8, 8, 12, 12},
        };

    // This function always reads 16 bytes. With AVX-512 we can do a mask
    // load, but it is not generally available with SSE 4.1.
    __msan_unpoison(ipv4_string, sizeof(__m128i));
    const __m128i input = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ipv4_string));
    if (ipv4_string_length > 15)
        return 0;

    // locate dots
    uint16_t dotmask;
    {
        const __m128i dot = _mm_set1_epi8('.');
        const __m128i t0 = _mm_cmpeq_epi8(input, dot);
        dotmask = static_cast<uint16_t>(_mm_movemask_epi8(t0));
        uint16_t mask = static_cast<uint16_t>(1) << ipv4_string_length;
        dotmask &= mask - 1;
        dotmask |= mask;
    }

    // build a hashcode
    const uint8_t hashcode = static_cast<uint8_t>((6639 * dotmask) >> 13);

    // grab the index of the shuffle mask
    const uint8_t id = patterns_id[hashcode];
    if (id >= 81)
        return 0;

    const uint8_t * pat = &patterns[id][0];

    const __m128i pattern = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pat));
    // The value of the shuffle mask at a specific index points at the last digit,
    // we check that it matches the length of the input.
    const __m128i ascii0 = _mm_set1_epi8('0');
    const __m128i t0 = input;

    __m128i t1 = _mm_shuffle_epi8(t0, pattern);
    // check that leading digits of 2- 3- numbers are not zeros.
    const __m128i eq0 = _mm_cmpeq_epi8(t1, ascii0);
    if (!_mm_testz_si128(eq0, _mm_set_epi8(-1, 0, -1, 0, -1, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        return 0;

    // replace null values with '0'
    __m128i t1b = _mm_blendv_epi8(t1, ascii0, pattern);

    // subtract '0'
    const __m128i t2 = _mm_sub_epi8(t1b, ascii0);

    // check that everything was in the range '0' to '9'
    const __m128i t2z = _mm_add_epi8(t2, _mm_set1_epi8(-128));
    const __m128i c9 = _mm_set1_epi8('9' - '0' - 128);
    const __m128i t2me = _mm_cmpgt_epi8(t2z, c9);
    if (!_mm_test_all_zeros(t2me, t2me))
        return 0;

    // We do the computation, the Mula way.
    const __m128i weights = _mm_setr_epi8(1, 10, 1, 10, 1, 10, 1, 10, 100, 0, 100, 0, 100, 0, 100, 0);
    const __m128i t3 = _mm_maddubs_epi16(t2, weights);

    // In t3, we have 8 16-bit values, the first four combine the two first digits, and
    // the 4 next 16-bit valued are made of the third digits.
    const __m128i t4 = _mm_alignr_epi8(t3, t3, 8);
    const __m128i t5 = _mm_add_epi16(t4, t3);

    // Test that we don't overflow (over 255)
    if (!_mm_testz_si128(t5, _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, -1, 0, -1, 0, -1, 0, -1, 0)))
        return 0;

    // pack and we are done!
    const __m128i t6 = _mm_packus_epi16(t5, t5);
    *destination = __builtin_bswap32(static_cast<uint32_t>(_mm_cvtsi128_si32(t6)));
    return ipv4_string_length - pat[6];
}

#endif

}
