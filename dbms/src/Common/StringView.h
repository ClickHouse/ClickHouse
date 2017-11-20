#pragma once

#include <string_view>

using StringView = std::string_view;

/// It creates StringView from literal constant at compile time.
template <typename TChar, size_t size>
constexpr inline std::basic_string_view<TChar> makeStringView(const TChar (&str)[size])
{
    return std::basic_string_view<TChar>(str, size - 1);
}
