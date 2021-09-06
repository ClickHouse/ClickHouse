#pragma once

#include <type_traits>
#include <fmt/format.h>
#include <../../contrib/magic_enum/include/magic_enum.hpp> //FIXME

// Can't name concept is_enum as is would conflict with type trait
template <class T> concept type_is_enum = std::is_enum_v<T>;

// Enable printing enum values as strings via fmt + magic_enum
template <type_is_enum T>
struct fmt::formatter<T> : fmt::formatter<std::string_view> {
    constexpr auto format(T enum_value, auto& format_context) {
        return formatter<string_view>::format(
            magic_enum::enum_name(enum_value), format_context);
    }
};

namespace DB
{

/**
 * Some enums have multiple-word values like FLUSH_DICT. However, we should parse user input like FLUSH DICT.
 * magic_enum::enum_names returns names with underscore, so we have to replace underscore with space.
 */
std::string UnderscoreToSpace(std::string_view str)
{
    std::string out(str.data(), str.size());
    std::replace(out.begin(), out.end(), '_', ' ');
    return out;
}
}
