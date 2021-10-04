#pragma once

#include <magic_enum.hpp>
#include <fmt/format.h>
#include "static_for.h"

template <class T> concept Enum = std::is_enum_v<T>;

/// Iterate over enum values in compile time. See Common/tests/gtest_enum_reflection.cpp for examples
template <Enum E>
constexpr bool static_for(auto && f)
{
    constexpr auto arr = CTArray(magic_enum::enum_values<E>());
    return static_for<arr>(std::forward<decltype(f)>(f));
}

/// Print enum values as strings via fmt + magic_enum
template <Enum T>
struct fmt::formatter<T> : fmt::formatter<std::string_view>
{
    constexpr auto format(T value, auto& format_context)
    {
        return formatter<string_view>::format(magic_enum::enum_name(value), format_context);
    }
};
