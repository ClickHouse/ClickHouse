#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/Concepts.h>

#include <type_traits>

/// Replacement for std::iota, optimized via auto-vectorization.
/// Only defined for containers with contiguous memory.

namespace DB
{

/// Make sure to add any new type to the extern declaration at the end of the file and instantiate it in iota.cpp

template <typename T>
concept iota_supported_types = (is_any_of<
                                T,
                                UInt8,
                                UInt32,
                                UInt64
#if defined(SIZE_T_IS_A_DISTINCT_TYPE)
                                ,
                                size_t
#endif
                                >);

#if defined(SIZE_T_IS_A_DISTINCT_TYPE)
static_assert(
    !std::is_same_v<size_t, UInt32> && !std::is_same_v<size_t, UInt64>,
    "`size_t` is one of the fixed-width types here, so it must not be instantiated separately");
#else
static_assert(
    std::is_same_v<size_t, UInt32> || std::is_same_v<size_t, UInt64>,
    "`size_t` is a distinct type here and needs an instantiation of its own");
#endif

template <iota_supported_types T> void iota(T * begin, size_t count, T first_value);

extern template void iota(UInt8 * begin, size_t count, UInt8 first_value);
extern template void iota(UInt32 * begin, size_t count, UInt32 first_value);
extern template void iota(UInt64 * begin, size_t count, UInt64 first_value);
#if defined(SIZE_T_IS_A_DISTINCT_TYPE)
extern template void iota(size_t * begin, size_t count, size_t first_value);
#endif

template <iota_supported_types T>
void iotaWithStep(T * begin, size_t count, T first_value, T step);

extern template void iotaWithStep(UInt8 * begin, size_t count, UInt8 first_value, UInt8 step);
extern template void iotaWithStep(UInt32 * begin, size_t count, UInt32 first_value, UInt32 step);
extern template void iotaWithStep(UInt64 * begin, size_t count, UInt64 first_value, UInt64 step);
#if defined(SIZE_T_IS_A_DISTINCT_TYPE)
extern template void iotaWithStep(size_t * begin, size_t count, size_t first_value, size_t step);
#endif
}
