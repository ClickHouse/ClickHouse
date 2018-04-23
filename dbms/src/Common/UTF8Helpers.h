#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>

#if __SSE2__
#include <emmintrin.h>
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
    if (first_octet < 0x80u)
        return 1;

    const size_t bits = 8;
    const auto first_zero = bitScanReverse(static_cast<UInt8>(~first_octet));

    return bits - 1 - first_zero;
}

inline size_t countCodePoints(const UInt8 * data, size_t size)
{
    size_t res = 0;
    const auto end = data + size;

#if __SSE2__
    constexpr auto bytes_sse = sizeof(__m128i);
    const auto src_end_sse = data + size / bytes_sse * bytes_sse;

    const auto threshold = _mm_set1_epi8(0xBF);

    for (; data < src_end_sse; data += bytes_sse)
        res += __builtin_popcount(_mm_movemask_epi8(
            _mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(data)), threshold)));
#endif

    for (; data < end; ++data) /// Skip UTF-8 continuation bytes.
        res += static_cast<Int8>(*data) > static_cast<Int8>(0xBF);

    return res;
}

}


}
