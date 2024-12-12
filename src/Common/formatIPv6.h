#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <type_traits>
#include <utility>
#include <base/hex.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/StringUtils.h>

constexpr size_t IPV4_BINARY_LENGTH = 4;
constexpr size_t IPV6_BINARY_LENGTH = 16;
constexpr size_t IPV4_MAX_TEXT_LENGTH = 15;     /// Does not count tail zero byte.
constexpr size_t IPV6_MAX_TEXT_LENGTH = 45;     /// Does not count tail zero byte.

namespace DB
{

extern const std::array<std::pair<const char *, size_t>, 256> one_byte_to_string_lookup_table;

/** Rewritten inet_ntop6 from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
  *  performs significantly faster than the reference implementation due to the absence of sprintf calls,
  *  bounds checking, unnecessary string copying and length calculation.
  */
void formatIPv6(const unsigned char * src, char *& dst, uint8_t zeroed_tail_bytes_count = 0);

/** Unsafe (no bounds-checking for src nor dst), optimized version of parsing IPv4 string.
 *
 * Parses the input string `src` and stores binary host-endian value into buffer pointed by `dst`,
 * which should be long enough.
 * That is "127.0.0.1" becomes 0x7f000001.
 *
 * In case of failure doesn't modify buffer pointed by `dst`.
 *
 * WARNING - this function is adapted to work with ReadBuffer, where src is the position reference (ReadBuffer::position())
 *           and eof is the ReadBuffer::eof() - therefore algorithm below does not rely on buffer's continuity.
 *           To parse strings use overloads below.
 *
 * @param src         - iterator (reference to pointer) over input string - warning - continuity is not guaranteed.
 * @param eof         - function returning true if iterator riched the end - warning - can break iterator's continuity.
 * @param dst         - where to put output bytes, expected to be non-null and at IPV4_BINARY_LENGTH-long.
 * @param first_octet - preparsed first octet
 * @return            - true if parsed successfully, false otherwise.
 */
template <typename T, typename EOFfunction>
requires (std::is_same_v<std::remove_cv_t<T>, char>)
inline bool parseIPv4(T * &src, EOFfunction eof, unsigned char * dst, int32_t first_octet = -1)
{
    if (src == nullptr || first_octet > 255)
        return false;

    UInt32 result = 0;
    int offset = 24;
    if (first_octet >= 0)
    {
        result |= first_octet << offset;
        offset -= 8;
    }

    for (; true; offset -= 8, ++src)
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
    }

    memcpy(dst, &result, sizeof(result));
    return true;
}

/// returns pointer to the right after parsed sequence or null on failed parsing
inline const char * parseIPv4(const char * src, const char * end, unsigned char * dst)
{
    if (parseIPv4(src, [&src, end](){ return src == end; }, dst))
        return src;
    return nullptr;
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

/// returns true if whole null-terminated string was parsed successfully
inline bool parseIPv4whole(const char * src, unsigned char * dst)
{
    const char * end = parseIPv4(src, dst);
    return end != nullptr && *end == '\0';
}

/** Unsafe (no bounds-checking for src nor dst), optimized version of parsing IPv6 string.
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
template <typename T, typename EOFfunction>
requires (std::is_same_v<typename std::remove_cv_t<T>, char>)
inline bool parseIPv6(T * &src, EOFfunction eof, unsigned char * dst, int32_t first_block = -1)
{
    const auto clear_dst = [dst]()
    {
        std::memset(dst, '\0', IPV6_BINARY_LENGTH);
        return false;
    };

    if (src == nullptr || eof())
        return clear_dst();

    int groups = 0;                 /// number of parsed groups
    unsigned char * iter = dst;     /// iterator over dst buffer
    unsigned char * zptr = nullptr; /// pointer into dst buffer array where all-zeroes block ("::") is started

    std::memset(dst, '\0', IPV6_BINARY_LENGTH);

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
        int xdigits = 0;  /// number of decoded hex digits in current group

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
        std::memmove(dst + IPV6_BINARY_LENGTH - msize, zptr, msize);
        std::memset(zptr, '\0', IPV6_BINARY_LENGTH - (iter - dst));
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
inline bool parseIPv6whole(const char * src, const char * end, unsigned char * dst)
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

/// returns true if whole null-terminated string was parsed successfully
inline bool parseIPv6whole(const char * src, unsigned char * dst)
{
    const char * end = parseIPv6(src, dst);
    return end != nullptr && *end == '\0';
}

/** Unsafe (no bounds-checking for src nor dst), optimized version of parsing IPv6 string.
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
template <typename T, typename EOFfunction>
requires (std::is_same_v<typename std::remove_cv_t<T>, char>)
inline bool parseIPv6orIPv4(T * &src, EOFfunction eof, unsigned char * dst)
{
    const auto clear_dst = [dst]()
    {
        std::memset(dst, '\0', IPV6_BINARY_LENGTH);
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
        *dst++ = '.';
    }

    for (size_t mask = 0; mask < mask_tail_octets; ++mask)
    {
        memcpy(dst, mask_string, mask_length);
        dst += mask_length;

        *dst++ = '.';
    }

    dst[-1] = '\0';
}

inline void formatIPv4(const unsigned char * src, char *& dst, uint8_t mask_tail_octets = 0, const char * mask_string = "xxx")
{
    formatIPv4(src, 4, dst, mask_tail_octets, mask_string);
}

}
