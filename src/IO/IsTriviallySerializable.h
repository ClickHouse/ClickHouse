#pragma once

#include <type_traits>

#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/LocalTime.h>

#include <Core/Types.h>
#include <base/IPv4andIPv6.h>

namespace DB
{

template <typename T>
concept is_trivially_serializable =
    is_arithmetic_v<T>
    || is_decimal<T>
    || std::is_same_v<T, LocalDate>
    || std::is_same_v<T, LocalDateTime>
    || std::is_same_v<T, LocalTime>
    || std::is_same_v<T, IPv4>
    || std::is_same_v<T, IPv6>;

}
