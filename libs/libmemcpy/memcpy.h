#pragma once

#ifdef __cplusplus
extern "C"
{
#endif

#include "impl/FastMemcpy.h"

void * __attribute__((__weak__)) memcpy(void * __restrict destination, const void * __restrict source, size_t size)
{
    return memcpy_fast(destination, source, size);
}

#ifdef __cplusplus
}
#endif
