#include "divide.h"
#include <Common/CPUID.h>

#if defined(__x86_64__)
namespace SSE2
{
    template <typename A, typename B, typename ResultType>
    void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size);
}

namespace AVX2
{
    template <typename A, typename B, typename ResultType>
    void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size);
}
#else
namespace Generic
{
    template <typename A, typename B, typename ResultType>
    void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size);
}
#endif


template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
#if defined(__x86_64__)
    if (DB::CPU::CPUFlagsCache::have_AVX2)
        AVX2::divideImpl(a_pos, b, c_pos, size);
    else if (DB::CPU::CPUFlagsCache::have_SSE2)
        SSE2::divideImpl(a_pos, b, c_pos, size);
#else
    Generic::divideImpl(a_pos, b, c_pos, size);
#endif
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
template void divideImpl<int64_t, Int8, int64_t>(const int64_t * __restrict, Int8, int64_t * __restrict, size_t);

template void divideImpl<int32_t, int64_t, int32_t>(const int32_t * __restrict, int64_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, int32_t, int32_t>(const int32_t * __restrict, int32_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, int16_t, int32_t>(const int32_t * __restrict, int16_t, int32_t * __restrict, size_t);
template void divideImpl<int32_t, Int8, int32_t>(const int32_t * __restrict, Int8, int32_t * __restrict, size_t);
