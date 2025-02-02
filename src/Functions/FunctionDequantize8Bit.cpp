#include "FunctionDequantize8Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    float F32FromSFP8(uint32_t sfp) {
        const uint32_t masked_sfp = sfp & 0x7F;
        if (masked_sfp == 0)
        {
            return 0.0f;
        }
        const uint32_t sign32 = (sfp & 0x80) << 24;
        const uint32_t large_e = masked_sfp >> 6;
        const uint32_t m_bits = 2 + large_e;
        const uint32_t m = masked_sfp & ((1u << m_bits) - 1u);
        const uint32_t e = masked_sfp >> m_bits;
        const uint32_t exp32 = (104 + e + 8 * large_e) << 23;
        const uint32_t m_shift = 21 - large_e;
        const uint32_t mnt32 = m << m_shift;
        const uint32_t binary32 = sign32 | exp32 | mnt32;
        float result;
        __builtin_memcpy(&result, &binary32, sizeof(uint32_t));

        return result;
    }

    float Dequantize8BitImpl::dequantize(UInt8 x) { return F32FromSFP8(x); }

    void Dequantize8BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = F32FromSFP8(input[i]);
        }
    }

)

DECLARE_AVX2_SPECIFIC_CODE(

    float Dequantize8BitImpl::dequantize(UInt8 x) { return Default::F32FromSFP8(x); }

    void Dequantize8BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        size_t i = 0;
        const size_t num_simd = size & ~7ULL;

        for (; i < num_simd; i += 8)
        {
            __m128i input_vec8 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(input + i));
            __m256i sfp = _mm256_cvtepu8_epi32(input_vec8);
            __m256i masked_sfp = _mm256_and_si256(sfp, _mm256_set1_epi32(0x7F));
            __m256i zero_mask = _mm256_cmpeq_epi32(masked_sfp, _mm256_setzero_si256());
            __m256i sign32 = _mm256_slli_epi32(_mm256_and_si256(sfp, _mm256_set1_epi32(0x80)), 24);
            __m256i large_e = _mm256_srli_epi32(masked_sfp, 6);
            __m256i m_mask = _mm256_or_si256(_mm256_set1_epi32(3), _mm256_slli_epi32(large_e, 2));
            __m256i m = _mm256_and_si256(masked_sfp, m_mask);
            __m256i m_bits = _mm256_add_epi32(_mm256_set1_epi32(2), large_e);
            __m256i e = _mm256_srlv_epi32(masked_sfp, m_bits);

            __m256i exp_term = _mm256_add_epi32(_mm256_set1_epi32(104), e);
            exp_term = _mm256_add_epi32(exp_term, _mm256_slli_epi32(large_e, 3));
            __m256i exp32 = _mm256_slli_epi32(exp_term, 23);

            __m256i m_shift = _mm256_sub_epi32(_mm256_set1_epi32(21), large_e);
            __m256i mnt32 = _mm256_sllv_epi32(m, m_shift);

            __m256i binary32 = _mm256_or_si256(sign32, exp32);
            binary32 = _mm256_or_si256(binary32, mnt32);

            binary32 = _mm256_blendv_epi8(binary32, _mm256_setzero_si256(), zero_mask);
            _mm256_storeu_ps(output + i, _mm256_castsi256_ps(binary32));
        }

        for (; i < size; ++i)
        {
            output[i] = Default::F32FromSFP8(input[i]);
        }
    }

)

REGISTER_FUNCTION(Dequantize8Bit)
{
    factory.registerFunction<FunctionDequantize8Bit>();
}

} // namespace DB
