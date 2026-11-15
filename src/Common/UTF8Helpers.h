#pragma once

#include <algorithm>
#include <cstring>
#include <optional>
#include <base/types.h>
#include <Common/BitHelpers.h>


namespace DB
{


namespace UTF8
{

static const UInt8 CONTINUATION_OCTET_MASK = 0b11000000u;
static const UInt8 CONTINUATION_OCTET = 0b10000000u;

/// return true if `octet` binary repr starts with 10 (octet is a UTF-8 sequence continuation)
inline bool isContinuationOctet(const UInt8 octet)
{
    return (octet & CONTINUATION_OCTET_MASK) == CONTINUATION_OCTET;
}

/// moves `s` backward until either first non-continuation octet or begin
inline void syncBackward(const UInt8 * & s, const UInt8 * const begin)
{
    while (isContinuationOctet(*s) && s > begin)
        --s;
}

/// moves `s` forward until either first non-continuation octet or string end is met
inline void syncForward(const UInt8 * & s, const UInt8 * const end)
{
    while (s < end && isContinuationOctet(*s))
        ++s;
}

/// returns UTF-8 code point sequence length judging by it's first octet
inline size_t seqLength(const UInt8 first_octet)
{
    if (first_octet < 0x80 || first_octet >= 0xF8)  /// The specs of UTF-8.
        return 1;

    const size_t bits = 8;
    const auto first_zero = bitScanReverse(static_cast<UInt8>(~first_octet));

    return bits - 1 - first_zero;
}

/// Every byte except a continuation byte (`0b10xxxxxx`) starts a code point: as signed bytes, those above `0xBF`.
inline size_t countCodePoints(const UInt8 * data, size_t size)
{
    using Bytes64 = Int8 __attribute__((ext_vector_type(64)));
    using Counters64 = UInt8 __attribute__((ext_vector_type(64)));
    using WideCounters64 = UInt16 __attribute__((ext_vector_type(64)));
    using Mask64 = bool __attribute__((ext_vector_type(64)));
    using Bytes16 = Int8 __attribute__((ext_vector_type(16)));
    using Counters16 = UInt8 __attribute__((ext_vector_type(16)));
    using Mask16 = bool __attribute__((ext_vector_type(16)));

    constexpr auto threshold = static_cast<Int8>(0xBF);
    /// `(byte ^ 0x80) & 0xC0` is nonzero exactly where `byte > threshold` is true: a comparison would
    /// depend on `-faltivec-src-compat` on PowerPC.
    constexpr auto flip = static_cast<Int8>(0x80);
    constexpr auto top_two_bits = static_cast<Int8>(0xC0);

    size_t res = 0;
    const UInt8 * end = data + size;

    /// One counter per byte of a 64-byte block: `vpcmpgtb` plus `vpsubb` on x86, `cmgt` plus `sub` on NEON.
    /// A counter is one byte, so the counters are summed every 255 blocks, and 64 * 255 fits in `UInt16`.
    while (end - data >= 64)
    {
        const size_t blocks = std::min<size_t>(255, (end - data) / 64);
        Counters64 counters = {};
        for (size_t i = 0; i < blocks; ++i, data += 64)
        {
            Bytes64 bytes;
            memcpy(&bytes, data, sizeof(bytes));
            counters += __builtin_convertvector(__builtin_convertvector((bytes ^ flip) & top_two_bits, Mask64), Counters64);
        }
        res += __builtin_reduce_add(__builtin_convertvector(counters, WideCounters64));
    }

    for (; end - data >= 16; data += 16)
    {
        Bytes16 bytes;
        memcpy(&bytes, data, sizeof(bytes));
        res += __builtin_reduce_add(__builtin_convertvector(__builtin_convertvector((bytes ^ flip) & top_two_bits, Mask16), Counters16));
    }

    for (; data < end; ++data)
        res += static_cast<Int8>(*data) > threshold;

    return res;
}


size_t convertCodePointToUTF8(int code_point, char * out_bytes, size_t out_length);
std::optional<uint32_t> convertUTF8ToCodePoint(const char * in_bytes, size_t in_length);

/// Surrogate code points are reserved for UTF-16 and are not Unicode scalar values,
/// so they have no valid UTF-8 encoding. `convertCodePointToUTF8` doesn't check that,
/// it encodes them as CESU-8, so callers must reject them beforehand.
constexpr bool isSurrogateCodePoint(UInt32 code_point)
{
    return code_point >= 0xD800 && code_point <= 0xDFFF;
}


/// returns UTF-8 wcswidth. Invalid sequence is treated as zero width character.
/// `prefix` is used to compute the `\t` width which extends the string before
/// and include `\t` to the nearest longer length with multiple of eight.
size_t computeWidth(const UInt8 * data, size_t size, size_t prefix = 0) noexcept;


/** Calculate the maximum number of bytes, so that substring of this size fits in 'limit' width.
  *
  * For example, we have string "x你好", it has 3 code points and visible width of 5 and byte size of 7.

  * Suppose we have limit = 3.
  * Then we have to return 4 as maximum number of bytes
  *  and the truncated string will be "x你": two code points, visible width 3, byte size 4.
  *
  * The same result will be for limit 4, because the last character would not fit.
  */
size_t computeBytesBeforeWidth(const UInt8 * data, size_t size, size_t prefix, size_t limit) noexcept;

/** Calculate the number of bytes before limit-th code point.
  */
size_t computeBytesBeforeCodePoint(const UInt8 * data, size_t size, size_t limit) noexcept;

/// True if `Poco::Unicode::toLower` maps some non-ASCII code point onto the ASCII character `c`, or onto its
/// other case. Today that set is {'k', 'K'}, reachable from U+212A KELVIN SIGN.
/// Case-insensitive UTF-8 search folds per code point with that same rule, so a caller comparing bytes cannot
/// reproduce it and must not answer a needle containing such a character.
bool isASCIIReachableByCaseFolding(char c);

}

}
