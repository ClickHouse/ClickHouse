#pragma once

#include <string_view>

namespace DB
{

class StaticString
{
    std::string_view stored_view;
public:
    template <size_t n>
    consteval StaticString(const char(&s)[n]) : stored_view(s) {}  /// NOLINT(google-explicit-constructor)
    explicit consteval StaticString(const char* s) : stored_view(s) {}
    explicit consteval StaticString(std::string_view sv) : stored_view(sv) {}

    StaticString() = default;

    bool operator==(const StaticString & other) const = default;

    constexpr bool empty() const { return stored_view.empty(); }

    constexpr std::string_view view() const { return stored_view; }
};

}
