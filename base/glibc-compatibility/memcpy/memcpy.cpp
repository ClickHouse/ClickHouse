#include "memcpy.h"

__attribute__((no_sanitize("coverage")))
extern "C" void * memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
    return inline_memcpy(dst, src, size);
}
