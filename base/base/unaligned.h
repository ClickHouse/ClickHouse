#pragma once

#include <cstring>
#include <type_traits>
#include <bit>


inline void reverseMemcpy(void * dst, const void * src, size_t size)
{
    uint8_t * uint_dst = reinterpret_cast<uint8_t *>(dst);
    const uint8_t * uint_src = reinterpret_cast<const uint8_t *>(src);

    uint_dst += size;
    while (length)
    {
        --uint_dst;
        *uint_dst = *uint_src;
        ++uint_src;
        --size;
    }
}

template <typename T>
inline T unalignedLoadLE(const void * address)
{
    T res {};
    if constexpr (std::endian::native == std::endian::little)
        memcpy(&res, address, sizeof(res));
    else
        reverseMemcpy(&res, address, sizeof(res));
    return res;
}


template <typename T>
inline void unalignedStoreLE(void * address,
                           const typename std::enable_if<true, T>::type & src)
{
    static_assert(std::is_trivially_copyable_v<T>);
    if constexpr (std::endian::native == std::endian::little)
        memcpy(address, &src, sizeof(src));
    else
        reverseMemcpy(address, &src, sizeof(src));
}

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
