#pragma once

#include <Common/Exception.h>

#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Runtime::Wyrd
{

constexpr bool enabled()
{
#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
    return true;
#else
    return false;
#endif
}

constexpr void unreachable([[maybe_unused]] const std::string & message)
{
#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", message);
#endif
}

}

}
