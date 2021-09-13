#pragma once

#include <magic_enum.hpp>
#include <fmt/format.h>
#include "static_for.h"

template <class T> concept is_enum = std::is_enum_v<T>;

/**
 * Iterate over enum values in compile time. See common/static_for.h
 *
 * @code{.cpp}
 * enum class E { A, B, C };
 * template <E v> void foo();
 *
 * static_for<E>([](auto enum_value) {
 *     foo<enum_value>();
 * });
 * @endcode
 */
template <is_enum E, class F>
constexpr bool static_for(F && f)
{
    return static_for<magic_enum::enum_values<E>()>(std::forward<F>(f));
}

/// Enable printing enum values as strings via fmt + magic_enum
template <is_enum T>
struct fmt::formatter<T> : fmt::formatter<std::string_view>
{
    constexpr auto format(T value, auto& format_context)
    {
        return formatter<string_view>::format(magic_enum::enum_name(value), format_context);
    }
};
