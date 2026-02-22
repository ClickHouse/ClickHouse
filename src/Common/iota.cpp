#include <base/defines.h>
#include <Common/iota.h>
#include <Common/TargetSpecific.h>

namespace DB
{

template <iota_supported_types T>
void iota(T * begin, size_t count, T first_value)
{
    T value = first_value;
    for (size_t i = 0; i < count; i++)
    {
        *(begin + i) = value;
        ++value;
    }
}

template <iota_supported_types T>
void iotaWithStep(T * begin, size_t count, T first_value, T step)
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
