#pragma once

#include <base/extended_types.h>

#define FOR_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256) \
    M(int8_t) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256)

#define INSTANTIATION(T) char * itoa(T i, char * p);
FOR_INTEGER_TYPES(INSTANTIATION)

#if defined(OS_DARWIN)
INSTANTIATION(unsigned long)
INSTANTIATION(long)
#endif

#undef FOR_INTEGER_TYPES
#undef INSTANTIATION
