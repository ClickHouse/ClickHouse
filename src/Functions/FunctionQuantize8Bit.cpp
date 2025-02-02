#include "FunctionQuantize8Bit.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>
#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif

namespace DB
{

DECLARE_DEFAULT_CODE(

    uint32_t Float32ToFloat8(float f) {
        constexpr uint32_t k_mask_m = 0x007FFFFFu;
        uint32_t binary32;
        __builtin_memcpy(&binary32, &f, sizeof(float));
        const uint32_t s = (binary32 >> 24) & 0x80;
        binary32 &= 0x7FFFFFFF;
        __builtin_memcpy(&f, &binary32, sizeof(uint32_t));

        bool large_e = (f >= 0.007568359375f);
        bool round_1_111 = (f >= 0.00732421875f);

        const uint32_t m32 = binary32 & k_mask_m;
        const size_t m_bits = large_e + 2;
        const uint32_t is_odd = (m32 >> (23 - m_bits)) & 1;
        const uint32_t round = is_odd + (1u << (22 - m_bits)) - 1;
        const uint32_t rounded = binary32 + round;

        const size_t final_m_bits = (round_1_111 * 3) | (!round_1_111 * m_bits);

        uint32_t m = (k_mask_m & rounded) >> (23 - final_m_bits);
        int32_t e_p23 = (rounded >> 23) - 104;

        bool cond = !(e_p23 | m);
        m = cond | (!cond * m);

        uint32_t e_sfp = e_p23 - round_1_111 * 8;

        const uint32_t encoded = -(e_p23 >= 0) & ((e_sfp << final_m_bits) | m | s);
        return encoded;
    }

    void Quantize8BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(Float32ToFloat8(input[i]));
        }
    }

)

DECLARE_AVX2_SPECIFIC_CODE(

    __m256i SFP8FromF32_AVX2(__m256 f) {
        static const __m256i kMaskM = _mm256_set1_epi32(0x007FFFFF);
        static const __m256 kZeroThresholdLargeE = _mm256_set1_ps(0.007568359375f);
        static const __m256 kZeroThresholdRound = _mm256_set1_ps(0.00732421875f);
        static const __m256i kExpBias = _mm256_set1_epi32(104);
        static const __m256i kOne = _mm256_set1_epi32(1);

        __m256i binary32 = _mm256_castps_si256(f);
        __m256i sign = _mm256_slli_epi32(_mm256_srli_epi32(binary32, 31), 7);
        binary32 = _mm256_and_si256(binary32, _mm256_set1_epi32(0x7FFFFFFF));

        __m256 f_abs = _mm256_castsi256_ps(binary32);
        __m256 ge_large = _mm256_cmp_ps(f_abs, kZeroThresholdLargeE, _CMP_GE_OQ);
        __m256 ge_round = _mm256_cmp_ps(f_abs, kZeroThresholdRound, _CMP_GE_OQ);
        __m256i large_e = _mm256_castps_si256(ge_large);
        __m256i round = _mm256_castps_si256(ge_round);

        __m256i m_bits = _mm256_add_epi32(_mm256_and_si256(large_e, kOne), _mm256_set1_epi32(2));
        __m256i final_m_bits = _mm256_or_si256(m_bits, _mm256_and_si256(round, kOne));

        __m256i m32 = _mm256_and_si256(binary32, kMaskM);
        __m256i shift_amt = _mm256_sub_epi32(_mm256_set1_epi32(23), m_bits);

        __m256i is_odd = _mm256_and_si256(_mm256_srlv_epi32(m32, shift_amt), kOne);
        __m256i shift_round = _mm256_sub_epi32(shift_amt, kOne);
        __m256i round_val = _mm256_add_epi32(_mm256_sub_epi32(_mm256_sllv_epi32(kOne, shift_round), kOne), is_odd);

        __m256i rounded = _mm256_add_epi32(binary32, round_val);
        __m256i mantissa = _mm256_srlv_epi32(_mm256_and_si256(rounded, kMaskM), _mm256_sub_epi32(_mm256_set1_epi32(23), final_m_bits));

        __m256i e_p23 = _mm256_sub_epi32(_mm256_srli_epi32(rounded, 23), kExpBias);
        __m256i e_sfp = _mm256_sub_epi32(e_p23, _mm256_and_si256(round, _mm256_set1_epi32(0x8)));

        __m256i is_zero = _mm256_cmpeq_epi32(_mm256_or_si256(e_p23, mantissa), _mm256_setzero_si256());
        __m256i m_final = _mm256_blendv_epi8(mantissa, kOne, is_zero);

        __m256i non_neg = _mm256_cmpgt_epi32(e_p23, _mm256_set1_epi32(-1));
        return _mm256_and_si256(_mm256_or_si256(_mm256_or_si256(_mm256_sllv_epi32(e_sfp, final_m_bits), m_final), sign), non_neg);
    }

    void Quantize8BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        size_t i = 0;

        static const __m256i perm = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);
        static const __m256i zero = _mm256_setzero_si256();

        for (; i + 8 <= size; i += 8)
        {
            __m256 fvals = _mm256_loadu_ps(input + i);
            __m256i sfp8_vals = SFP8FromF32_AVX2(fvals);

            __m256i packed16 = _mm256_packus_epi32(sfp8_vals, zero);
            __m256i packed8 = _mm256_packus_epi16(packed16, zero);

            packed8 = _mm256_permutevar8x32_epi32(packed8, perm);

            uint64_t val = _mm256_extract_epi64(packed8, 0);
            memcpy(output + i, &val, sizeof(uint64_t));
        }

        for (; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(Default::Float32ToFloat8(input[i]));
        }
    }

)


REGISTER_FUNCTION(Quantize8Bit)
{
    factory.registerFunction<FunctionQuantize8Bit>();
}

} // namespace DB
