#pragma once

#include <type_traits>
#include <utility>
#include <concepts>
#include "CTArray.h"

//template <auto Val, decltype(Val)... List>
//constexpr bool static_in_v = std::disjunction_v<std::bool_constant<Val == List>...>;

template <class F, auto Elem, class R>
concept returns_on_elem = requires (F f)
{
    { f(std::integral_constant<decltype(Elem), Elem>()) } -> std::same_as<R>;
};

namespace detail
{
template <typename T, T Begin, T... Is>
constexpr bool static_for(auto && f, std::integer_sequence<T, Is...>)
{
    return (
        std::forward<decltype(f)>(f)(
            std::integral_constant<T, Begin + Is>())
        || ...);
}

template <CTArray Container, size_t ...I>
constexpr bool static_for(auto && f, std::index_sequence<I...>)
{
    return (
        std::forward<decltype(f)>(f)(
            std::integral_constant<decltype(Container[I]), Container[I]>())
        || ...);
}
}

/**
 * Compile-time iteration over values. Iterates over range [@a Begin; @a End).
 * @a f is invoked with <tt>std::integral_constant<decltype(Begin), Item></tt> so inside functor call it can be used in
 * compile-time context (e.g. template parameter).
 *
 * @param f Functor that is invoked on elements within @a Begin and @a End. Should return @c true if iteration should
 *   stop or @c false if iteration should continue.
 *
 * See tests/gtest_static_for.cpp for examples.
 */
template <auto Begin, decltype(Begin) End>
constexpr bool static_for(returns_on_elem<Begin, bool> auto && f)
{
    using T = decltype(Begin);

    return detail::static_for<T, Begin>(
        std::forward<decltype(f)>(f),
        std::make_integer_sequence<T, End - Begin>{});
}

/**
 * Same as <tt>static_for<Begin, End>(f)</tt>, but without need of explicit specification of [begin;end) range.
 * See tests/gtest_static_for.cpp for examples.
 */
template <CTArray Container>
constexpr bool static_for(returns_on_elem<Container[0], bool> auto && f)
{
    return detail::static_for<Container>(
        std::forward<decltype(f)>(f),
        std::make_index_sequence<Container.size>{});
}
