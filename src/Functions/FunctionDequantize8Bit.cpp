#include "FunctionDequantize8Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    void Dequantize8BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = Lookup8Bit::dequantize_lookup[input[i]];
        }
    }

)

DECLARE_AVX2_SPECIFIC_CODE(

    void Dequantize8BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        size_t i = 0;
        const size_t num_simd = size & ~7ULL;
        const float * table = Lookup8Bit::dequantize_lookup.data();

        for (; i < num_simd; i += 8)
        {
            __m128i input_vec = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(input + i));
            __m256i indices = _mm256_cvtepu8_epi32(input_vec);
            __m256 gathered = _mm256_i32gather_ps(table, indices, 4);
            _mm256_storeu_ps(output + i, gathered);
        }

        for (; i < size; ++i)
        {
            output[i] = table[input[i]];
        }
    }

)

REGISTER_FUNCTION(Dequantize8Bit)
{
    factory.registerFunction<FunctionDequantize8Bit>();
}

} // namespace DB
