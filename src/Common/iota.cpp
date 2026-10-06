#include <base/defines.h>
#include <Common/iota.h>

namespace DB
{

/// NO_INLINE prevents LTO from inlining these into callers. When inlined, the
/// hot loop's alignment depends on the surrounding code and can land on a
/// 64-byte boundary crossing, causing ~5% regression on modern CPUs. As a
/// separate function the compiler aligns the loop independently.
/// (Previously the multi-target dispatch mechanism achieved this implicitly.)

template <iota_supported_types T>
void NO_INLINE iota(T * begin, size_t count, T first_value)
{
    T value = first_value;
    for (size_t i = 0; i < count; i++)
    {
        *(begin + i) = value;
        ++value;
    }
}

template <iota_supported_types T>
void NO_INLINE iotaWithStep(T * begin, size_t count, T first_value, T step)
{
    T value = first_value;
    for (size_t i = 0; i < count; i++)
    {
        *(begin + i) = value;
        value += step;
    }
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
