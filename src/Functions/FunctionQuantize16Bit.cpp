#include "FunctionQuantize16Bit.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>
#include "Common/TargetSpecific.h"
#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif

namespace DB
{

DECLARE_DEFAULT_CODE(

    uint16_t Float32ToFloat16(float value) {
        uint32_t f;
        std::memcpy(&f, &value, sizeof(float));

        uint16_t sign = (f >> 16) & 0x8000;

        int32_t exp = (f >> 23) & 0xFF;
        uint32_t mantissa = f & 0x7FFFFF;

        if (exp == 0xFF)
        {
            if (mantissa)
            {
                return static_cast<uint16_t>(sign | 0x7E00 | (mantissa >> 13));
            }
            else
            {
                return static_cast<uint16_t>(sign | 0x7C00);
            }
        }
        else if (exp == 0)
        {
            if (mantissa == 0)
            {
                return static_cast<uint16_t>(sign);
            }
            else
            {
                while ((mantissa & 0x800000) == 0)
                {
                    mantissa <<= 1;
                    exp--;
                }
                exp++;
                mantissa &= 0x7FFFFF;
            }
        }

        exp += 112;

        if (exp >= 0x1F)
        {
            return static_cast<uint16_t>(sign | 0x7C00);
        }

        if (exp <= 0)
        {
            mantissa |= 0x800000;
            mantissa >>= (1 - exp);
            exp = 0;
        }

        return static_cast<uint16_t>(sign | (exp << 10) | (mantissa >> 13));
    }


    void Quantize16BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        auto out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = Float32ToFloat16(input[i]);
        }
    }

)

DECLARE_AVX512F_SPECIFIC_CODE(

    void Quantize16BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        size_t i = 0;

        for (; i + 8 <= size; i += 8)
        {
            __m256 input_vec = _mm256_loadu_ps(input + i);
            __m128i fp16 = _mm256_cvtps_ph(input_vec, 0);
            _mm_storeu_si128(reinterpret_cast<__m128i *>(output + i * 2), fp16);
        }

        for (; i < size; ++i)
        {
            __m128 xmm = _mm_load_ss(input + i);
            __m128i fp16 = _mm_cvtps_ph(xmm, 0);
            uint16_t half = static_cast<uint16_t>(_mm_extract_epi16(fp16, 0));

            output[i * 2] = half & 0xFF;
            output[i * 2 + 1] = (half >> 8) & 0xFF;
        }
    }

)


REGISTER_FUNCTION(Quantize16Bit)
{
    factory.registerFunction<FunctionQuantize16Bit>();
}

} // namespace DB
