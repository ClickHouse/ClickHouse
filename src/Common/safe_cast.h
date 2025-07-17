#pragma once

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Cast integer checking for overflow
template <typename To, typename From>
requires std::integral<To> && std::integral<From>
inline To safe_cast(From value)
{
    if constexpr (std::is_signed_v<From> && std::is_unsigned_v<To>)
    {
        if (value < 0 || static_cast<std::make_unsigned_t<From>>(value) > std::numeric_limits<To>::max())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric cast overflow: signed to unsigned conversion error");
    }
    else if constexpr (std::is_unsigned_v<From> && std::is_signed_v<To>)
    {
        if (value > static_cast<From>(std::numeric_limits<To>::max()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric cast overflow: unsigned to signed conversion error");
    }
    else
    {
        if (value < std::numeric_limits<To>::min() || value > std::numeric_limits<To>::max())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric cast overflow: value out of target type range");
    }
    return static_cast<To>(value);
}

}
