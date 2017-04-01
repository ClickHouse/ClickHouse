#pragma once

#include <experimental/string_view>

using StringView = std::experimental::string_view;

/// It creates StringView from literal constant at compile time.
template <typename TChar, size_t size>
constexpr inline std::experimental::basic_string_view<TChar> makeStringView(const TChar (&str)[size])
{
    return std::experimental::basic_string_view<TChar>(str, size - 1);
}
