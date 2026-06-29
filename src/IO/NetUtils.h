#pragma once

#include <concepts>
#include <bit>


namespace DB
{

template<std::integral T>
constexpr T netToHost(T value) noexcept
{
    if constexpr (std::endian::native != std::endian::big)
        return std::byteswap(value);
    return value;
}

template<std::integral T>
constexpr T hostToNet(T value) noexcept
{
    if constexpr (std::endian::native != std::endian::big)
        return std::byteswap(value);
    return value;
}

template<std::integral T>
constexpr T toLittleEndian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::big)
        return std::byteswap(value);
    return value;
}

template<std::integral T>
constexpr T toBigEndian(T value) noexcept
{
    if constexpr (std::endian::native != std::endian::big)
        return std::byteswap(value);
    return value;
}

template<std::integral T>
constexpr T fromLittleEndian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::big)
        return std::byteswap(value);
    return value;
}

template<std::integral T>
constexpr T fromBigEndian(T value) noexcept
{
    if constexpr (std::endian::native != std::endian::big)
        return std::byteswap(value);
    return value;
}

}
