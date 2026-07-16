#pragma once

#include <base/types.h>
#include <Common/Concepts.h>

/// This is a replacement for std::iota to use dynamic dispatch
/// Note that is only defined for containers with contiguous memory only

namespace DB
{

/// Make sure to add any new type to the extern declaration at the end of the file and instantiate it in iota.cpp

template <typename T>
concept iota_supported_types = (is_any_of<
                                T,
                                UInt8,
                                UInt32,
                                UInt64
#if defined(OS_DARWIN)
                                ,
                                size_t
#endif
                                >);

template <iota_supported_types T> void iota(T * begin, size_t count, T first_value);

extern template void iota(UInt8 * begin, size_t count, UInt8 first_value);
extern template void iota(UInt32 * begin, size_t count, UInt32 first_value);
extern template void iota(UInt64 * begin, size_t count, UInt64 first_value);
#if defined(OS_DARWIN)
extern template void iota(size_t * begin, size_t count, size_t first_value);
#endif

template <iota_supported_types T>
void iotaWithStep(T * begin, size_t count, T first_value, T step);

extern template void iotaWithStep(UInt8 * begin, size_t count, UInt8 first_value, UInt8 step);
extern template void iotaWithStep(UInt32 * begin, size_t count, UInt32 first_value, UInt32 step);
extern template void iotaWithStep(UInt64 * begin, size_t count, UInt64 first_value, UInt64 step);
#if defined(OS_DARWIN)
extern template void iotaWithStep(size_t * begin, size_t count, size_t first_value, size_t step);
#endif
}
