#pragma once

#ifdef OS_LINUX

#include <base/types.h>

#include <bit>

#include <linux/futex.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace DB
{

inline Int64 futexWait(void * address, UInt32 value)
{
    return syscall(SYS_futex, address, FUTEX_WAIT_PRIVATE, value, nullptr, nullptr, 0);
}

inline Int64 futexWake(void * address, int count)
{
    return syscall(SYS_futex, address, FUTEX_WAKE_PRIVATE, count, nullptr, nullptr, 0);
}

inline void futexWaitFetch(std::atomic<UInt32> & address, UInt32 & value)
{
    futexWait(&address, value);
    value = address.load();
}

inline void futexWakeOne(std::atomic<UInt32> & address)
{
    futexWake(&address, 1);
}

inline void futexWakeAll(std::atomic<UInt32> & address)
{
     futexWake(&address, INT_MAX);
}

constexpr UInt32 lowerHalf(UInt64 value)
{
    return static_cast<UInt32>(value & 0xffffffffull);
}

constexpr UInt32 upperHalf(UInt64 value)
{
    return static_cast<UInt32>(value >> 32ull);
}

inline UInt32 * lowerHalfAddress(void * address)
{
    return reinterpret_cast<UInt32 *>(address) + (std::endian::native == std::endian::big);
}

inline UInt32 * upperHalfAddress(void * address)
{
    return reinterpret_cast<UInt32 *>(address) + (std::endian::native == std::endian::little);
}

inline void futexWaitLowerFetch(std::atomic<UInt64> & address, UInt64 & value)
{
    futexWait(lowerHalfAddress(&address), lowerHalf(value));
    value = address.load();
}

inline void futexWakeLowerOne(std::atomic<UInt64> & address)
{
    futexWake(lowerHalfAddress(&address), 1);
}

inline void futexWakeLowerAll(std::atomic<UInt64> & address)
{
    futexWake(lowerHalfAddress(&address), INT_MAX);
}

inline void futexWaitUpperFetch(std::atomic<UInt64> & address, UInt64 & value)
{
    futexWait(upperHalfAddress(&address), upperHalf(value));
    value = address.load();
}

inline void futexWakeUpperOne(std::atomic<UInt64> & address)
{
    futexWake(upperHalfAddress(&address), 1);
}

inline void futexWakeUpperAll(std::atomic<UInt64> & address)
{
    futexWake(upperHalfAddress(&address), INT_MAX);
}

}

#endif
