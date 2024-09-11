#include <base/defines.h>
#include <Common/iota.h>
#include <Common/TargetSpecific.h>

namespace DB
{

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(template <iota_supported_types T> void NO_INLINE),
    iotaImpl, MULTITARGET_FUNCTION_BODY((T * begin, size_t count, T first_value) /// NOLINT
    {
        for (size_t i = 0; i < count; i++)
            *(begin + i) = static_cast<T>(first_value + i);
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

MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(template <iota_supported_types T> void NO_INLINE),
    iotaWithStepImpl, MULTITARGET_FUNCTION_BODY((T * begin, size_t count, T first_value, T step) /// NOLINT
    {
        for (size_t i = 0; i < count; i++)
            *(begin + i) = static_cast<T>(first_value + i * step);
    })
)

template <iota_supported_types T>
void iotaWithStep(T * begin, size_t count, T first_value, T step)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX2))
        return iotaWithStepImplAVX2(begin, count, first_value, step);

    if (isArchSupported(TargetArch::SSE42))
        return iotaWithStepImplSSE42(begin, count, first_value, step);
#endif
    return iotaWithStepImpl(begin, count, first_value, step);
}

template void iota(UInt8 * begin, size_t count, UInt8 first_value);
template void iota(UInt32 * begin, size_t count, UInt32 first_value);
template void iota(UInt64 * begin, size_t count, UInt64 first_value);
#if defined(OS_DARWIN)
template void iota(size_t * begin, size_t count, size_t first_value);
#endif

template void iotaWithStep(UInt8 * begin, size_t count, UInt8 first_value, UInt8 step);
template void iotaWithStep(UInt32 * begin, size_t count, UInt32 first_value, UInt32 step);
template void iotaWithStep(UInt64 * begin, size_t count, UInt64 first_value, UInt64 step);
#if defined(OS_DARWIN)
template void iotaWithStep(size_t * begin, size_t count, size_t first_value, size_t step);
#endif
}
