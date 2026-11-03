#pragma once

#include <cstring>
#include <type_traits>
#include <bit>


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


inline void reverseMemcpy(void * dst, const void * src, size_t size)
{
    uint8_t * uint_dst = reinterpret_cast<uint8_t *>(dst);
    const uint8_t * uint_src = reinterpret_cast<const uint8_t *>(src);

    uint_dst += size;
    while (size)
    {
        --uint_dst;
        *uint_dst = *uint_src;
        ++uint_src;
        --size;
    }
}

template <std::endian endian, typename T>
inline T unalignedLoadEndian(const void * address)
{
    T res {};
    if constexpr (std::endian::native == endian)
        memcpy(&res, address, sizeof(res));
    else
        reverseMemcpy(&res, address, sizeof(res));
    return res;
}


template <std::endian endian, typename T>
inline void unalignedStoreEndian(void * address, T & src)
{
    static_assert(std::is_trivially_copyable_v<T>);
    if constexpr (std::endian::native == endian)
        memcpy(address, &src, sizeof(src));
    else
        reverseMemcpy(address, &src, sizeof(src));
}


template <typename T>
inline T unalignedLoadLittleEndian(const void * address)
{
    return unalignedLoadEndian<std::endian::little, T>(address);
}


template <typename T>
inline void unalignedStoreLittleEndian(void * address,
    const typename std::enable_if<true, T>::type & src)
{
    unalignedStoreEndian<std::endian::little>(address, src);
}

template <typename T>
inline T unalignedLoadBigEndian(const void * address)
{
    return unalignedLoadEndian<std::endian::big, T>(address);
}


template <typename T>
inline void unalignedStoreBigEndian(void * address,
    const typename std::enable_if<true, T>::type & src)
{
    unalignedStoreEndian<std::endian::big>(address, src);
}
