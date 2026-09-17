#pragma once

#include <type_traits>

#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/LocalTime.h>

#include <Core/Types.h>
#include <base/IPv4andIPv6.h>

namespace DB
{

/// A type is trivially serializable if its binary representation is a sequence of POD bytes,
/// i.e. it is serialized by simply copying `sizeof(T)` bytes of its in-memory layout (without
/// endianness conversion, indirection, or any element-wise transformation). This allows a
/// contiguous container of such elements to be (de)serialized with a single bulk memory copy.
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
