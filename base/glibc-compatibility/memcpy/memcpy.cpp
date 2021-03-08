#include "memcpy.h"

#include <cpuid.h>
#include <cstdint>


bool have_avx = false;

void init_memcpy()
{
    uint32_t eax;
    uint32_t ebx;
    uint32_t ecx;
    uint32_t edx;
    __cpuid(1, eax, ebx, ecx, edx);
    have_avx = (ecx >> 28) & 1;
}

extern "C" void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return inline_memcpy(reinterpret_cast<char *>(dst), reinterpret_cast<const char *>(src), size);
}
