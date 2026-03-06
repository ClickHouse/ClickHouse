#include <libdivide-config.h>
#include <libdivide.h>

#include <base/types.h>

namespace DB
{

#if defined(LIBDIVIDE_AVX2)
    #define REG_SIZE 32
#elif defined(LIBDIVIDE_SSE2)
    #define REG_SIZE 16
#endif

template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
    libdivide::divider<A> divider(static_cast<A>(b));
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

template void divideImpl<UInt64, UInt64, UInt64>(const UInt64 * __restrict, UInt64, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt32, UInt64>(const UInt64 * __restrict, UInt32, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt16, UInt64>(const UInt64 * __restrict, UInt16, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt8, UInt64>(const UInt64 * __restrict, UInt8, UInt64 * __restrict, size_t);

template void divideImpl<UInt32, UInt64, UInt32>(const UInt32 * __restrict, UInt64, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt32, UInt32>(const UInt32 * __restrict, UInt32, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt16, UInt32>(const UInt32 * __restrict, UInt16, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt8, UInt32>(const UInt32 * __restrict, UInt8, UInt32 * __restrict, size_t);

template void divideImpl<Int64, Int64, Int64>(const Int64 * __restrict, Int64, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int32, Int64>(const Int64 * __restrict, Int32, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int16, Int64>(const Int64 * __restrict, Int16, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int8, Int64>(const Int64 * __restrict, Int8, Int64 * __restrict, size_t);

template void divideImpl<Int32, Int64, Int32>(const Int32 * __restrict, Int64, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int32, Int32>(const Int32 * __restrict, Int32, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int16, Int32>(const Int32 * __restrict, Int16, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int8, Int32>(const Int32 * __restrict, Int8, Int32 * __restrict, size_t);

}
