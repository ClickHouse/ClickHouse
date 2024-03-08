#pragma once

#include <base/extended_types.h>

template <typename T> char * itoa(T i, char * p);

template <> char * itoa(UInt8 i, char * p);
template <> char * itoa(Int8 i, char * p);
template <> char * itoa(UInt128 i, char * p);
template <> char * itoa(Int128 i, char * p);
template <> char * itoa(UInt256 i, char * p);
template <> char * itoa(Int256 i, char * p);

#define FOR_MISSING_INTEGER_TYPES(M) \
    M(int8_t) \
    M(uint8_t) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int16) \
    M(Int32) \
    M(Int64)

#define INSTANTIATION(T) \
    extern template char * itoa(T i, char * p);
FOR_MISSING_INTEGER_TYPES(INSTANTIATION)

#if defined(OS_DARWIN)
INSTANTIATION(size_t)
#endif

#undef FOR_MISSING_INTEGER_TYPES
#undef INSTANTIATION


template <typename T> int digits10(T x);

#define DIGITS_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256)
#define INSTANTIATION(T) \
    extern template int digits10(T x);
DIGITS_INTEGER_TYPES(INSTANTIATION)
#undef DIGITS_INTEGER_TYPES
#undef INSTANTIATION
