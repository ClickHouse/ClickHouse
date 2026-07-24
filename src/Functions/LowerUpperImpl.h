#pragma once
#include <Columns/ColumnString.h>

namespace DB
{

template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t /*input_rows_count*/)
    {
        res_data.resize_exact(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t /*n*/, ColumnString::Chars & res_data, size_t /*input_rows_count*/)
    {
        res_data.resize_exact(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        const auto flip_case_mask = 'A' ^ 'a';

#if defined(__AVX512F__) && defined(__AVX512BW__) /// check if avx512 instructions are compiled
        if (isArchSupported(TargetArch::AVX512BW))
        {
            /// check if cpu support avx512 dynamically, haveAVX512BW contains check of haveAVX512F
            const auto byte_avx512 = sizeof(__m512i);
            const auto src_end_avx = src_end - (src_end - src) % byte_avx512;
            if (src < src_end_avx)
            {
                const auto v_not_case_lower_bound = _mm512_set1_epi8(not_case_lower_bound - 1);
                const auto v_not_case_upper_bound = _mm512_set1_epi8(not_case_upper_bound + 1);

                for (; src < src_end_avx; src += byte_avx512, dst += byte_avx512)
                {
                    const auto chars = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(src));

                    const auto is_not_case
                            = _mm512_mask_cmplt_epi8_mask(_mm512_cmpgt_epi8_mask(chars, v_not_case_lower_bound),
                            chars, v_not_case_upper_bound);

                    const auto xor_mask = _mm512_maskz_set1_epi8(is_not_case, flip_case_mask);

                    const auto cased_chars = _mm512_xor_si512(chars, xor_mask);

                    _mm512_storeu_si512(reinterpret_cast<__m512i *>(dst), cased_chars);
                }
            }
        }
#endif

#ifdef __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto * src_end_sse = src_end - (src_end - src) % bytes_sse;
        if (src < src_end_sse)
        {
            const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
            const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
            const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

            for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
            {
                /// load 16 sequential 8-bit characters
                const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

                /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
                const auto is_not_case
                    = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

                /// keep `flip_case_mask` only where necessary, zero out elsewhere
                const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                /// flip case by applying calculated mask
                const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                /// store result back to destination
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
            }
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};

}
