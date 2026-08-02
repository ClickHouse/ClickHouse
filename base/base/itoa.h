#pragma once

#include <base/defines.h>
#include <base/extended_types.h>

#include <type_traits>

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

/// `long` is not covered by the list above where it is a distinct type.
/// Naming the type is the whole point here, so `google-runtime-int` has nothing to suggest.
// NOLINTBEGIN(google-runtime-int)
#if defined(LONG_IS_A_DISTINCT_TYPE)
static_assert(
    !std::is_same_v<long, Int32> && !std::is_same_v<long, Int64>,
    "`long` is one of the fixed-width types here, so it must not be instantiated separately");
INSTANTIATION(unsigned long)
INSTANTIATION(long)
#else
static_assert(
    std::is_same_v<long, Int32> || std::is_same_v<long, Int64>,
    "`long` is a distinct type here and needs an instantiation of its own");
#endif
// NOLINTEND(google-runtime-int)

#undef FOR_INTEGER_TYPES
#undef INSTANTIATION
