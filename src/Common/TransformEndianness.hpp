#pragma once

#include <base/Decimal_fwd.h>
#include <base/extended_types.h>

#include <utility>

namespace DB
{
template <std::endian endian, typename T>
requires std::is_integral_v<T>
inline void transformEndianness(T & value)
{
    if constexpr (endian != std::endian::native)
        value = std::byteswap(value);
}

template <std::endian endian, typename T>
requires is_big_int_v<T>
inline void transformEndianness(T & x)
{
    if constexpr (std::endian::native != endian)
    {
        auto & items = x.items;
        std::transform(std::begin(items), std::end(items), std::begin(items), [](auto & item) { return std::byteswap(item); });
        std::reverse(std::begin(items), std::end(items));
    }
}

template <std::endian endian, typename T>
requires is_decimal<T>
inline void transformEndianness(T & x)
{
    transformEndianness<endian>(x.value);
}

template <std::endian endian, typename T>
requires std::is_floating_point_v<T>
inline void transformEndianness(T & value)
{
    if constexpr (std::endian::native != endian)
    {
        auto * start = reinterpret_cast<std::byte *>(&value);
        std::reverse(start, start + sizeof(T));
    }
}

template <std::endian endian, typename T>
requires std::is_scoped_enum_v<T>
inline void transformEndianness(T & x)
{
    using UnderlyingType = std::underlying_type_t<T>;
    transformEndianness<endian>(reinterpret_cast<UnderlyingType &>(x));
}

template <std::endian endian, typename A, typename B>
inline void transformEndianness(std::pair<A, B> & pair)
{
    transformEndianness<endian>(pair.first);
    transformEndianness<endian>(pair.second);
}

template <std::endian endian, typename T, typename Tag>
inline void transformEndianness(StrongTypedef<T, Tag> & x)
{
    transformEndianness<endian>(x.toUnderType());
}
}
