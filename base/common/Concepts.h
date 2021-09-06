#pragma once

#include <type_traits>
#include <concepts>

#include "extended_types.h"

namespace DB
{
// Concept for enums can't be named just "enum", so they all start with is_ for consistency

/// C++ integral types as stated in standard + wide integer types [U]Int[128, 256]
template <class T> concept is_integer = is_integer_v<T>;

template <class T> concept is_floating_point = std::is_floating_point_v<T>;
template <class T> concept is_enum = std::is_enum_v<T>;
}
