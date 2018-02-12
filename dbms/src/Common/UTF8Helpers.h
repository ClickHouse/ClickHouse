#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>

#if __SSE2__
#include <emmintrin.h>
#endif

#if __SSE4_1__
#include <smmintrin.h>
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

#if __SSE2__ && __SSE4_1__
    const auto bytes_sse = sizeof(__m128i);
    const auto src_end_sse = (data + size) - (size % bytes_sse);

    const auto one_sse = _mm_set1_epi8(1);
    const auto zero_sse = _mm_set1_epi8(0);

    const auto align_sse = _mm_set1_epi8(0xFF - 0xC0 + 1);
    const auto upper_bound = _mm_set1_epi8(0x7F + (0xFF - 0xC0 + 1) + 1);

    for (; data < src_end_sse;)
    {
        auto sse_res = _mm_set1_epi8(0);

        for (int i = 0, limit = 0xFF; i < limit && data < src_end_sse; ++i, data += bytes_sse)
        {
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data));

            ///Align to zero for the solve two case
            const auto align_res = _mm_add_epi8(chars, align_sse);
            ///Because _mm_cmmpgt_epid8 return 0xFF if true
            const auto choose_res = _mm_blendv_epi8(zero_sse, one_sse, _mm_cmpgt_epi8(align_res, upper_bound));

            sse_res = _mm_add_epi8(sse_res, choose_res);
        }

        UInt16 mem_res[8] = {0};
        auto horizontal_add = _mm_sad_epu8(sse_res, zero_sse);
        _mm_store_si128(reinterpret_cast<__m128i *>(mem_res), horizontal_add);

        /// Hack,Because only bytes MSB to LSB
        res += mem_res[0];
        res += mem_res[4];
    }

#endif

    for (; data < end; ++data) /// Skip UTF-8 continuation bytes.
        res += (*data <= 0x7F || *data >= 0xC0);

    return res;
}

}


}
