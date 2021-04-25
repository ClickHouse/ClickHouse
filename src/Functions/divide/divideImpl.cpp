/// This translation unit should be compiled multiple times
/// with different values of NAMESPACE and machine flags (sse2, avx2).

#if !defined(NAMESPACE)
    #if defined(ARCADIA_BUILD)
        #define NAMESPACE Generic
    #else
        #error "NAMESPACE macro must be defined"
    #endif
#endif

#if defined(__AVX2__)
    #define REG_SIZE 32
    #define LIBDIVIDE_AVX2
#elif defined(__SSE2__)
    #define REG_SIZE 16
    #define LIBDIVIDE_SSE2
#endif

#include <libdivide.h>


namespace NAMESPACE
{

template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
    libdivide::divider<A> divider(b);
    const A * a_end = a_pos + size;

#if defined(__SSE2__)
    static constexpr size_t values_per_simd_register = REG_SIZE / sizeof(A);
    const A * a_end_simd = a_pos + size / values_per_simd_register * values_per_simd_register;

    while (a_pos < a_end_simd)
    {
#if defined(__AVX2__)
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(c_pos),
            _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a_pos)) / divider);
#else
        _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);
#endif

        a_pos += values_per_simd_register;
        c_pos += values_per_simd_register;
    }
#endif

    while (a_pos < a_end)
    {
        *c_pos = *a_pos / divider;
        ++a_pos;
        ++c_pos;
    }
}

template void divideImpl<uint64_t, uint64_t, uint64_t>(const uint64_t * __restrict, uint64_t, uint64_t * __restrict, size_t);
template void divideImpl<uint64_t, uint32_t, uint64_t>(const uint64_t * __restrict, uint32_t, uint64_t * __restrict, size_t);
template void divideImpl<uint64_t, uint16_t, uint64_t>(const uint64_t * __restrict, uint16_t, uint64_t * __restrict, size_t);
template void divideImpl<uint64_t, char8_t, uint64_t>(const uint64_t * __restrict, char8_t, uint64_t * __restrict, size_t);

template void divideImpl<uint32_t, uint64_t, uint32_t>(const uint32_t * __restrict, uint64_t, uint32_t * __restrict, size_t);
template void divideImpl<uint32_t, uint32_t, uint32_t>(const uint32_t * __restrict, uint32_t, uint32_t * __restrict, size_t);
template void divideImpl<uint32_t, uint16_t, uint32_t>(const uint32_t * __restrict, uint16_t, uint32_t * __restrict, size_t);
template void divideImpl<uint32_t, char8_t, uint32_t>(const uint32_t * __restrict, char8_t, uint32_t * __restrict, size_t);

template void divideImpl<int64_t, int64_t, int64_t>(const int64_t * __restrict, int64_t, int64_t * __restrict, size_t);
template void divideImpl<int64_t, int32_t, int64_t>(const int64_t * __restrict, int32_t, int64_t * __restrict, size_t);
template void divideImpl<int64_t, int16_t, int64_t>(const int64_t * __restrict, int16_t, int64_t * __restrict, size_t);
template void divideImpl<int64_t, int8_t, int64_t>(const int64_t * __restrict, int8_t, int64_t * __restrict, size_t);

template void divideImpl<int32_t, int64_t, int32_t>(const int32_t * __restrict, int64_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, int32_t, int32_t>(const int32_t * __restrict, int32_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, int16_t, int32_t>(const int32_t * __restrict, int16_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, int8_t, int32_t>(const int32_t * __restrict, int8_t, int32_t * __restrict, size_t);

}
