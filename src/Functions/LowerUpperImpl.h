#pragma once
#include <Columns/ColumnString.h>
#include <Common/TargetSpecific.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif


namespace DB
{

DECLARE_DEFAULT_CODE(
template <char not_case_lower_bound, char not_case_upper_bound>
static void arrayImpl(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';
     for (; src < src_end; ++src, ++dst)
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst = *src ^ flip_case_mask;
        else
            *dst = *src;
})

DECLARE_AVX512BW_SPECIFIC_CODE(
template <char not_case_lower_bound, char not_case_upper_bound>
static void arrayImpl(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';

    const auto byte_avx512 = sizeof(__m512i);
    const auto src_end_avx = src_end - (src_end - src) % byte_avx512;
    if (src < src_end_avx)
    {
        const __m512i v_not_case_lower_bound = _mm512_set1_epi8(not_case_lower_bound - 1);
        const __m512i v_not_case_upper_bound = _mm512_set1_epi8(not_case_upper_bound + 1);

        for (; src < src_end_avx; src += byte_avx512, dst += byte_avx512)
        {
            /// load 64 sequential 8-bit characters
            const __m512i chars = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(src));

            const __mmask64 is_not_case = _mm512_mask_cmplt_epi8_mask(_mm512_cmpgt_epi8_mask(chars, v_not_case_lower_bound), chars, v_not_case_upper_bound);

            const __m512i xor_mask = _mm512_maskz_set1_epi8(is_not_case, flip_case_mask);

            /// flip case by applying calculated mask
            const __m512i cased_chars = _mm512_xor_si512(chars, xor_mask);

            /// store result back to destination
            _mm512_storeu_si512(reinterpret_cast<__m512i *>(dst), cased_chars);
        }
    }

    /// Handle the tail
    for (; src < src_end; ++src, ++dst)
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst = *src ^ flip_case_mask;
        else
            *dst = *src;
})

DECLARE_AVX2_SPECIFIC_CODE(
template <char not_case_lower_bound, char not_case_upper_bound>
static void arrayImpl(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';

    const auto bytes_avx = sizeof(__m256i);
    const auto * src_end_avx = src_end - (src_end - src) % bytes_avx;
    if (src < src_end_avx)
    {
        const __m256i v_not_case_lower_bound = _mm256_set1_epi8(not_case_lower_bound - 1);
        const __m256i v_not_case_upper_bound = _mm256_set1_epi8(not_case_upper_bound + 1);
        const __m256i v_flip_case_mask = _mm256_set1_epi8(flip_case_mask);

        for (; src < src_end_avx; src += bytes_avx, dst += bytes_avx)
        {
            /// load 32 sequential 8-bit characters
            const __m256i chars = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src));

            /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
            const __m256i is_not_case = _mm256_and_si256(_mm256_cmpgt_epi8(chars, v_not_case_lower_bound), _mm256_cmpgt_epi8(v_not_case_upper_bound, chars));

            /// keep `flip_case_mask` only where necessary, zero out elsewhere
            const __m256i xor_mask = _mm256_and_si256(v_flip_case_mask, is_not_case);

            /// flip case by applying calculated mask
            const __m256i cased_chars = _mm256_xor_si256(chars, xor_mask);

            /// store result back to destination
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst), cased_chars);
        }
    }

    /// Handle the tail
    for (; src < src_end; ++src, ++dst)
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst = *src ^ flip_case_mask;
        else
            *dst = *src;
})

DECLARE_SSE42_SPECIFIC_CODE(
template <char not_case_lower_bound, char not_case_upper_bound>
static void arrayImpl(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';

    const auto bytes_sse = sizeof(__m128i);
    const auto * src_end_sse = src_end - (src_end - src) % bytes_sse;
    if (src < src_end_sse)
    {
        const __m128i v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const __m128i v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const __m128i v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
        {
            /// load 16 sequential 8-bit characters
            const __m128i chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
            const __m128i is_not_case = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

            /// keep `flip_case_mask` only where necessary, zero out elsewhere
            const __m128i xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

            /// flip case by applying calculated mask
            const __m128i cased_chars = _mm_xor_si128(chars, xor_mask);

            /// store result back to destination
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
        }
    }

    /// Handle the tail
    for (; src < src_end; ++src, ++dst)
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst = *src ^ flip_case_mask;
        else
            *dst = *src;
})

template <char not_case_lower_bound, char not_case_upper_bound>
static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
        TargetSpecific::AVX512BW::arrayImpl<not_case_lower_bound, not_case_upper_bound>(src, src_end, dst);
    else if (isArchSupported(TargetArch::AVX2))
        TargetSpecific::AVX2::arrayImpl<not_case_lower_bound, not_case_upper_bound>(src, src_end, dst);
    else if (isArchSupported(TargetArch::SSE42))
        TargetSpecific::SSE42::arrayImpl<not_case_lower_bound, not_case_upper_bound>(src, src_end, dst);
    else
#endif
    TargetSpecific::Default::arrayImpl<not_case_lower_bound, not_case_upper_bound>(src, src_end, dst);
}

template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array<not_case_lower_bound, not_case_upper_bound>(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t /*n*/, ColumnString::Chars & res_data)
    {
        res_data.resize(data.size());
        array<not_case_lower_bound, not_case_upper_bound>(data.data(), data.data() + data.size(), res_data.data());
    }
};

}
