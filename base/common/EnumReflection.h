#pragma once

#include <magic_enum.hpp>
#include <fmt/format.h>
#include "Concepts.h"

/// Enable printing enum values as strings via fmt + magic_enum
template <DB::is_enum T>
struct fmt::formatter<T> : fmt::formatter<std::string_view>
{
    constexpr auto format(T enum_value, auto& format_context)
    {
        return formatter<string_view>::format(
            magic_enum::enum_name(enum_value), format_context);
    }
};
