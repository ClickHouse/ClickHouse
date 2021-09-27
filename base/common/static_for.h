#pragma once

#include <type_traits>
#include <utility>
#include "FnTraits.h"
#include "CTArray.h"

namespace detail
{
template <class T, T Begin, T... Is>
constexpr bool static_for(auto && f, std::integer_sequence<decltype(Begin), Is...>)
{
    return (
        std::forward<decltype(f)>(f)(
            Constant<Begin + Is>())
        || ...);
}

template <CTArray Container, size_t ...I>
constexpr bool static_for(auto && f, std::index_sequence<I...>)
{
    return (
        std::forward<decltype(f)>(f)(
            Constant<Container[I]>())
        || ...);
}
}

/**
 * Compile-time iteration over values.
 * Iterates over range [@a Begin; @a End).
 * Inside @a f call its argument can be used in compile-time context as it's an integral constant.
 *
 * @param f Functor that is invoked on elements within @a Begin and @a End. Should return @c true if iteration should
 *   stop or @c false if iteration should continue.
 *
 * See Common/tests/gtest_static_for.cpp for examples.
 */
template <auto Begin, decltype(Begin) End>
//constexpr bool static_for(Fn<bool(Constant<Begin>)> auto && f)
constexpr bool static_for(auto && f)
{
    using T = decltype(Begin);

    return detail::static_for<T, Begin>(
        std::forward<decltype(f)>(f),
        std::make_integer_sequence<T, End - Begin>{});
}

/**
 * Same as <tt>static_for<Begin, End>(f)</tt>, but without need of explicit specification of [begin;end) range.
 * See Common/tests/gtest_static_for.cpp for examples.
 */
template <CTArray Container>
//constexpr bool static_for(Fn<bool(Constant<Container[0]>)> auto && f)
constexpr bool static_for( auto && f)
{
    return detail::static_for<Container>(
        std::forward<decltype(f)>(f),
        std::make_index_sequence<Container.size>{});
}
