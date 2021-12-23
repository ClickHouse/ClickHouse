#pragma once

#include <magic_enum.hpp>
#include <fmt/format.h>

template <class T> concept is_enum = std::is_enum_v<T>;

namespace detail
{
template <is_enum E, class F, size_t ...I>
constexpr void static_for(F && f, std::index_sequence<I...>)
{
    (std::forward<F>(f)(std::integral_constant<E, magic_enum::enum_value<E>(I)>()) , ...);
}
}

/**
 * Iterate over enum values in compile-time (compile-time switch/case, loop unrolling).
 *
 * @example static_for<E>([](auto enum_value) { return template_func<enum_value>(); }
 * ^ enum_value can be used as a template parameter
 */
template <is_enum E, class F>
constexpr void static_for(F && f)
{
    constexpr size_t count = magic_enum::enum_count<E>();
    detail::static_for<E>(std::forward<F>(f), std::make_index_sequence<count>());
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
