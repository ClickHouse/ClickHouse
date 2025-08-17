#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <type_traits>
#include <utility>
#include <base/hex.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <base/MemorySanitizer.h>
#include <Common/StringUtils.h>

#if defined(__SSE4_1__)
#include <x86intrin.h>
#endif

constexpr size_t IPV4_BINARY_LENGTH = 4;
constexpr size_t IPV6_BINARY_LENGTH = 16;
constexpr size_t IPV4_MAX_TEXT_LENGTH = 15;
constexpr size_t IPV6_MAX_TEXT_LENGTH = 45;

namespace DB
{

extern const std::array<std::pair<const char *, size_t>, 256> one_byte_to_string_lookup_table;

/** Rewritten inet_ntop6 from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
  *  performs significantly faster than the reference implementation due to the absence of sprintf calls,
  *  bounds checking, unnecessary string copying and length calculation.
  */
void formatIPv6(const unsigned char * src, char *& dst, uint8_t zeroed_tail_bytes_count = 0);

/** An optimized version of parsing IPv4 string.
 *
 * Parses the input string `src` and stores binary host-endian value into buffer pointed by `dst`,
 * which should accommodate four bytes.
 * That is "127.0.0.1" becomes 0x7f000001.
 *
 * In case of failure doesn't modify buffer pointed by `dst`.
 *
 * WARNING - this function is adapted to work with ReadBuffer, where src is the position reference (ReadBuffer::position())
 *           and eof is the ReadBuffer::eof() - therefore algorithm below does not rely on buffer's continuity.
 *           To parse strings use overloads below.
 *
 * @param src         - iterator (reference to pointer) over input string - warning - continuity is not guaranteed.
 * @param eof         - function returning true if iterator reached the end - warning - can break iterator's continuity.
 * @param dst         - where to put output bytes, expected to be non-null and at IPV4_BINARY_LENGTH-long.
 * @param first_octet - preparsed first octet
 * @return            - true if parsed successfully, false otherwise.
 */
template <typename T, typename IsEOF>
inline bool parseIPv4(T & src, IsEOF eof, unsigned char * dst, int32_t first_octet = -1)
{
    if (first_octet > 255)
        return false;

    UInt32 result = 0;
    int offset = 24;
    if (first_octet >= 0)
    {
        result |= first_octet << offset;
        offset -= 8;
    }

    while (true)
    {
        if (eof())
            return false;

        UInt32 value = 0;
        size_t len = 0;
        while (isNumericASCII(*src) && len <= 3)
        {
            value = value * 10 + (*src - '0');
            ++len;
            ++src;
            if (eof())
                break;
        }
        if (len == 0 || value > 255 || (offset > 0 && (eof() || *src != '.')))
            return false;
        result |= value << offset;

        if (offset == 0)
            break;

        offset -= 8;
        ++src;
    }

    memcpy(dst, &result, sizeof(result));
    return true;
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
static inline int parseIPv4SSE(const char * ipv4_string, const size_t ipv4_string_length, uint32_t * destination)
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


/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char * parseIPv4(const char * src, const char * end, unsigned char * dst)
{
#if defined(__SSE4_1__)
    if (parseIPv4SSE(src, end - src, reinterpret_cast<uint32_t *>(dst)))
        return end;
    return nullptr;
#else
    if (parseIPv4(src, [&src, end](){ return src == end; }, dst))
        return src;
    return nullptr;
#endif
}

/// returns true if whole buffer was parsed successfully
inline bool parseIPv4whole(const char * src, const char * end, unsigned char * dst)
{
    return parseIPv4(src, end, dst) == end;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char * parseIPv4(const char * src, unsigned char * dst)
{
    if (parseIPv4(src, [](){ return false; }, dst))
        return src;
    return nullptr;
}

/** An optimized version of parsing IPv6 string.
*
* Parses the input string `src` and stores binary big-endian value into buffer pointed by `dst`,
* which should be long enough. In case of failure zeroes IPV6_BINARY_LENGTH bytes of buffer pointed by `dst`.
*
* WARNING - this function is adapted to work with ReadBuffer, where src is the position reference (ReadBuffer::position())
*           and eof is the ReadBuffer::eof() - therefore algorithm below does not rely on buffer's continuity.
*           To parse strings use overloads below.
*
* @param src         - iterator (reference to pointer) over input string - warning - continuity is not guaranteed.
* @param eof         - function returning true if iterator riched the end - warning - can break iterator's continuity.
* @param dst         - where to put output bytes, expected to be non-null and at IPV6_BINARY_LENGTH-long.
* @param first_block - preparsed first block
* @return            - true if parsed successfully, false otherwise.
*/
template <typename T, typename IsEOF>
inline bool parseIPv6(T & src, IsEOF eof, unsigned char * dst, int32_t first_block = -1)
{
    const auto clear_dst = [dst]()
    {
        memset(dst, '\0', IPV6_BINARY_LENGTH);
        return false;
    };

    if (eof())
        return clear_dst();

    int groups = 0;                 /// number of parsed groups
    unsigned char * iter = dst;     /// iterator over dst buffer
    unsigned char * zptr = nullptr; /// pointer into dst buffer array where all-zeroes block ("::") is started

    if (first_block >= 0)
    {
        *iter++ = static_cast<unsigned char>((first_block >> 8) & 0xffu);
        *iter++ = static_cast<unsigned char>(first_block & 0xffu);
        if (*src == ':')
        {
            zptr = iter;
            ++src;
        }
        ++groups;
    }

    bool group_start = true;

    while (!eof() && groups < 8)
    {
        if (*src == ':')
        {
            ++src;
            if (eof()) /// trailing colon is not allowed
                return clear_dst();

            group_start = true;

            if (*src == ':')
            {
                if (zptr != nullptr) /// multiple all-zeroes blocks are not allowed
                    return clear_dst();
                zptr = iter;
                ++src;
                continue;
            }
            if (groups == 0) /// leading colon is not allowed
                return clear_dst();
        }

        if (*src == '.') /// mixed IPv4 parsing
        {
            if (groups <= 1 && zptr == nullptr) /// IPv4 block can't be the first
                return clear_dst();

            if (group_start) /// first octet of IPv4 should be already parsed as an IPv6 group
                return clear_dst();

            ++src;
            if (eof())
                return clear_dst();

            /// last parsed group should be reinterpreted as a decimal value - it's the first octet of IPv4
            --groups;
            iter -= 2;

            UInt16 num = 0;
            for (int i = 0; i < 2; ++i)
            {
                unsigned char first = (iter[i] >> 4) & 0x0fu;
                unsigned char second = iter[i] & 0x0fu;
                if (first > 9 || second > 9)
                    return clear_dst();
                (num *= 100) += first * 10 + second;
            }
            if (num > 255)
                return clear_dst();

            /// parse IPv4 with known first octet
            if (!parseIPv4(src, eof, iter, num))
                return clear_dst();

            if constexpr (std::endian::native == std::endian::little)
                std::reverse(iter, iter + IPV4_BINARY_LENGTH);

            iter += 4;
            groups += 2;
            break; /// IPv4 block is the last - end of parsing
        }

        if (!group_start) /// end of parsing
            break;
        group_start = false;

        UInt16 val = 0;   /// current decoded group
        int xdigits = 0;  /// number of decoded hex digits in the current group

        for (; !eof() && xdigits < 4; ++src, ++xdigits)
        {
            UInt8 num = unhex(*src);
            if (num == 0xFF)
                break;
            (val <<= 4) |= num;
        }

        if (xdigits == 0) /// end of parsing
            break;

        *iter++ = static_cast<unsigned char>((val >> 8) & 0xffu);
        *iter++ = static_cast<unsigned char>(val & 0xffu);
        ++groups;
    }

    /// either all 8 groups or all-zeroes block should be present
    if (groups < 8 && zptr == nullptr)
        return clear_dst();

    if (zptr != nullptr) /// process all-zeroes block
    {
        size_t msize = iter - zptr;
        memmove(dst + IPV6_BINARY_LENGTH - msize, zptr, msize);
        memset(zptr, '\0', IPV6_BINARY_LENGTH - (iter - dst));
    }

    return true;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char * parseIPv6(const char * src, const char * end, unsigned char * dst)
{
    if (parseIPv6(src, [&src, end](){ return src == end; }, dst))
        return src;
    return nullptr;
}

/// returns true if whole buffer was parsed successfully
inline bool parseIPv6Whole(const char * src, const char * end, unsigned char * dst)
{
    return parseIPv6(src, end, dst) == end;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char * parseIPv6(const char * src, unsigned char * dst)
{
    if (parseIPv6(src, [](){ return false; }, dst))
        return src;
    return nullptr;
}

/** An optimized version of parsing IPv6 string.
*
* Parses the input string `src` IPv6 or possible IPv4 into IPv6 and stores binary big-endian value into buffer pointed by `dst`,
* which should be long enough. In case of failure zeroes IPV6_BINARY_LENGTH bytes of buffer pointed by `dst`.
*
* WARNING - this function is adapted to work with ReadBuffer, where src is the position reference (ReadBuffer::position())
*           and eof is the ReadBuffer::eof() - therefore algorithm below does not rely on buffer's continuity.
*
* @param src - iterator (reference to pointer) over input string - warning - continuity is not guaranteed.
* @param eof - function returning true if iterator riched the end - warning - can break iterator's continuity.
* @param dst - where to put output bytes, expected to be non-null and at IPV6_BINARY_LENGTH-long.
* @return    - true if parsed successfully, false otherwise.
*/
template <typename T, typename IsEOF>
inline bool parseIPv6orIPv4(T & src, IsEOF eof, unsigned char * dst)
{
    const auto clear_dst = [dst]()
    {
        memset(dst, '\0', IPV6_BINARY_LENGTH);
        return false;
    };

    if (src == nullptr)
        return clear_dst();

    bool leading_zero = false;
    uint16_t val = 0;
    int digits = 0;
    /// parse up to 4 first digits as hexadecimal
    for (; !eof() && digits < 4; ++src, ++digits)
    {
        if (*src == ':' || *src == '.')
            break;

        if (digits == 0 && *src == '0')
            leading_zero = true;

        UInt8 num = unhex(*src);
        if (num == 0xFF)
            return clear_dst();
        (val <<= 4) |= num;
    }

    if (eof())
        return clear_dst();

    if (*src == ':') /// IPv6
    {
        if (digits == 0) /// leading colon - no preparsed group
            return parseIPv6(src, eof, dst);
        ++src;
        return parseIPv6(src, eof, dst, val); /// parse with first preparsed group
    }

    if (*src == '.') /// IPv4
    {
        /// should has some digits
        if (digits == 0)
            return clear_dst();
        /// should not has leading zeroes, should has no more than 3 digits
        if ((leading_zero && digits > 1) || digits > 3)
            return clear_dst();

        /// recode first group as decimal
        UInt16 num = 0;
        for (int exp = 1; exp < 1000; exp *= 10)
        {
            int n = val & 0x0fu;
            if (n > 9)
                return clear_dst();
            num += n * exp;
            val >>= 4;
        }
        if (num > 255)
            return clear_dst();

        ++src;
        if (!parseIPv4(src, eof, dst, num)) /// try to parse as IPv4 with preparsed first octet
            return clear_dst();

        /// convert into IPv6
        if constexpr (std::endian::native == std::endian::little)
        {
            dst[15] = dst[0]; dst[0] = 0;
            dst[14] = dst[1]; dst[1] = 0;
            dst[13] = dst[2]; dst[2] = 0;
            dst[12] = dst[3]; dst[3] = 0;
        }
        else
        {
            dst[15] = dst[3]; dst[3] = 0;
            dst[14] = dst[2]; dst[2] = 0;
            dst[13] = dst[1]; dst[1] = 0;
            dst[12] = dst[0]; dst[0] = 0;
        }

        dst[11] = 0xff;
        dst[10] = 0xff;

        return true;
    }

    return clear_dst();
}

/** Format 4-byte binary sequesnce as IPv4 text: 'aaa.bbb.ccc.ddd',
  * expects in out to be in BE-format, that is 0x7f000001 => "127.0.0.1".
  *
  * Any number of the tail bytes can be masked with given mask string.
  *
  * Assumptions:
  *     src is IPV4_BINARY_LENGTH long,
  *     dst is IPV4_MAX_TEXT_LENGTH long,
  *     mask_tail_octets <= IPV4_BINARY_LENGTH
  *     mask_string is NON-NULL, if mask_tail_octets > 0.
  *
  * Examples:
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 0, nullptr);
  *         > dst == "127.0.0.1"
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 1, "xxx");
  *         > dst == "127.0.0.xxx"
  *     formatIPv4(&0x7f000001, dst, mask_tail_octets = 1, "0");
  *         > dst == "127.0.0.0"
  */
inline void formatIPv4(const unsigned char * src, size_t src_size, char *& dst, uint8_t mask_tail_octets = 0, const char * mask_string = "xxx")
{
    const size_t mask_length = mask_string ? strlen(mask_string) : 0;
    const size_t limit = std::min(IPV4_BINARY_LENGTH, IPV4_BINARY_LENGTH - mask_tail_octets);
    const size_t padding = std::min(4 - src_size, limit);

    for (size_t octet = 0; octet < padding; ++octet)
    {
        *dst++ = '0';
        if (octet < 3)
            *dst++ = '.';
    }

    for (size_t octet = 4 - src_size; octet < limit; ++octet)
    {
        uint8_t value = 0;
        if constexpr (std::endian::native == std::endian::little)
            value = static_cast<uint8_t>(src[IPV4_BINARY_LENGTH - octet - 1]);
        else
            value = static_cast<uint8_t>(src[octet]);
        const uint8_t len = one_byte_to_string_lookup_table[value].second;
        const char* str = one_byte_to_string_lookup_table[value].first;

        memcpy(dst, str, len);
        dst += len;
        if (octet < 3)
            *dst++ = '.';
    }

    for (size_t mask = 0; mask < mask_tail_octets; ++mask)
    {
        if (mask > 0)
            *dst++ = '.';
        memcpy(dst, mask_string, mask_length);
        dst += mask_length;
    }
}

inline void formatIPv4(const unsigned char * src, char *& dst, uint8_t mask_tail_octets = 0, const char * mask_string = "xxx")
{
    formatIPv4(src, 4, dst, mask_tail_octets, mask_string);
}

}
