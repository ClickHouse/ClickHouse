#include "FastMemcpy.h"

extern "C" void * memcpy(void * __restrict destination, const void * __restrict source, size_t size)
{
    return memcpy_fast(destination, source, size);
}
