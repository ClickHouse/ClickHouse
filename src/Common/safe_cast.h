#pragma once

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <class To, class From>
To safe_cast(From from)
{
    constexpr auto max = std::numeric_limits<To>::max();
    if (from > max)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Overflow ({} > {})", from, max);
    return static_cast<To>(from);
}

}
