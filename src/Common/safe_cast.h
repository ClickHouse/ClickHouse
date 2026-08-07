#pragma once

#include <Common/Exception.h>

#include <concepts>
#include <limits>
#include <type_traits>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Safe integral cast with overflow check
template <std::integral To, std::integral From>
[[nodiscard]] constexpr To safe_cast(From value)
{
    if constexpr (std::is_signed_v<From> == std::is_signed_v<To> &&
        std::numeric_limits<To>::digits >= std::numeric_limits<From>::digits)
    {
        return static_cast<To>(value);
    }
    if constexpr (!std::is_signed_v<From> && std::is_signed_v<To> &&
        std::numeric_limits<To>::digits > std::numeric_limits<From>::digits)
    {
        return static_cast<To>(value);
    }

    if (!std::in_range<To>(value))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric cast overflow: value {} is out of range of target type {}", value, typeid(To).name());

    return static_cast<To>(value);
}

}
