#pragma once

#include <cstring>
#include <type_traits>


template <typename T>
inline T unalignedLoad(const void * address)
{
    T res {};
    memcpy(&res, address, sizeof(res));
    return res;
}

/// We've had troubles before with wrong store size due to integral promotions
/// (e.g., unalignedStore(dest, uint16_t + uint16_t) stores an uint32_t).
/// To prevent this, make the caller specify the stored type explicitly.
/// To disable deduction of T, wrap the argument type with std::enable_if.
template <typename T>
inline void unalignedStore(void * address,
                           const typename std::enable_if<true, T>::type & src)
{
    static_assert(std::is_trivially_copyable_v<T>);
    memcpy(address, &src, sizeof(src));
}
