#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/Concepts.h>
#include <Common/TargetSpecific.h>

/// This is a replacement for std::iota to use dynamic dispatch
/// Note that is only defined for containers with contiguous memory only

namespace DB
{

/// Make sure to add any new type to the extern declaration at the end of the file and instantiate it in iota.cpp
template <typename T>
concept iota_supported_types = (is_any_of<T, UInt64>);

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(template <iota_supported_types T> void NO_INLINE),
    iotaImpl, MULTITARGET_FUNCTION_BODY((T * begin, size_t count, T first_value) /// NOLINT
    {
        for (size_t i = 0; i < count; i++)
            *(begin + i) = first_value + i;
    })
)

template <iota_supported_types T>
void iota(T * begin, size_t count, T first_value)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX2))
        return iotaImplAVX2(begin, count, first_value);

    if (isArchSupported(TargetArch::SSE42))
        return iotaImplSSE42(begin, count, first_value);
#endif
    return iotaImpl(begin, count, first_value);
}

extern template void iota(UInt64 * begin, size_t count, UInt64 first_value);

}
