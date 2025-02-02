#include "FunctionDequantize16Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

float Float16ToFloat32(uint16_t half)
{
    uint32_t sign = (half >> 15) & 0x1;
    uint32_t exp = (half >> 10) & 0x1F;
    uint32_t mantissa = half & 0x3FF;

    uint32_t f;
    if (exp == 0)
    {
        if (mantissa == 0)
        {
            f = sign << 31;
        }
        else
        {
            while ((mantissa & 0x400) == 0)
            {
                mantissa <<= 1;
                exp--;
            }
            exp++;
            mantissa &= 0x3FF;
            f = (sign << 31) | ((exp + 112) << 23) | (mantissa << 13);
        }
    }
    else if (exp == 0x1F)
    {
        f = (sign << 31) | (0xFF << 23) | (mantissa << 13);
    }
    else
    {
        f = (sign << 31) | ((exp + 112) << 23) | (mantissa << 13);
    }
    float result;
    std::memcpy(&result, &f, sizeof(float));
    return result;
}

DECLARE_DEFAULT_CODE(

    void Dequantize16BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            uint16_t half = static_cast<uint16_t>(input[i * 2]) | (static_cast<uint16_t>(input[i * 2 + 1]) << 8);
            output[i] = Float16ToFloat32(half);
        }
    }

)

DECLARE_AVX512F_SPECIFIC_CODE(

    void Dequantize16BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        size_t i = 0;
        for (; i + 8 <= size; i += 8)
        {
            __m128i fp16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(input + i * 2));
            __m256 output_vec = _mm256_cvtph_ps(fp16);
            _mm256_storeu_ps(output + i, output_vec);
        }

        for (; i < size; ++i)
        {
            uint16_t half = static_cast<uint16_t>(input[i * 2]) | (static_cast<uint16_t>(input[i * 2 + 1]) << 8);
            __m128i fp16 = _mm_set1_epi16(half);
            __m128 single = _mm_cvtph_ps(fp16);
            _mm_store_ss(output + i, single);
        }
    }

)

REGISTER_FUNCTION(Dequantize16Bit)
{
    factory.registerFunction<FunctionDequantize16Bit>();
}

} // namespace DB
