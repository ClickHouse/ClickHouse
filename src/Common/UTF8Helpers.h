#pragma once

#include <algorithm>
#include <optional>
#include <base/types.h>
#include <base/simd.h>
#include <Common/BitHelpers.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#      pragma clang diagnostic ignored "-Wreserved-identifier"
#endif


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

inline size_t countCodePoints(const UInt8 * data, size_t size)
{
    const UInt8 * p   = data;
    const UInt8 * end = data + size;
    size_t        res = 0;

    /* ---------- SIMD fast path ---------- */
#ifdef __SSE2__
    constexpr size_t V = 16;
    const UInt8 * simd_end = p + size / V * V;

    const __m128i thr = _mm_set1_epi8(0xBF);
    for (; p < simd_end; p += V)
        res += __builtin_popcount(
                 _mm_movemask_epi8(
                     _mm_cmpgt_epi8(_mm_loadu_si128(
                                        reinterpret_cast<const __m128i *>(p)),
                                    thr)));

#elif defined(__aarch64__) && defined(__ARM_NEON)
    constexpr size_t V = 16;
    const UInt8 * simd_end = p + size / V * V;

    const int8x16_t thr = vdupq_n_s8(0xBF);
    for (; p < simd_end; p += V)
    {
        uint8x16_t gt = vcgtq_s8(vld1q_s8(reinterpret_cast<const int8_t *>(p)), thr);
        res += std::popcount(getNibbleMask(gt));   // 4 bits per byte
    }
    res >>= 2;   // convert nibble mask to 1‑bit mask, matching popcount
#endif

    /* ---------- tail fix‑up (≤19 bytes) ---------- */
    const UInt8 * tail = (p >= data + 3) ? p - 3 : data;   // rewind 3 bytes

    /* Remove any counts for the overlap bytes (they’ll be re‑parsed) */
    for (const UInt8 * q = tail; q < p; ++q)
        res -= static_cast<Int8>(*q) > static_cast<Int8>(0xBF);

    while (tail < end)
    {
        size_t len = seqLength(*tail);
        if (tail + len > end) len = 1;   // truncated / invalid
        ++res;
        tail += len;
    }
    return res;
}

size_t convertCodePointToUTF8(int code_point, char * out_bytes, size_t out_length);
std::optional<uint32_t> convertUTF8ToCodePoint(const char * in_bytes, size_t in_length);


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

}

}
