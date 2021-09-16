#pragma once

#include <Common/typeid_cast.h>

namespace DB
{

namespace detail
{
template <typename Typelist, size_t ...I>
static bool castTypeToEither(const auto * type, auto && f, std::index_sequence<I...>)
{
    return (
        (typeid_cast<const typename Typelist::template At<I> *>(type)
            ? std::forward<decltype(f)>(f)(
                *typeid_cast<const typename Typelist::template At<I> *>(type))
            : false)
        || ...);
}
}

template <typename... Ts, typename T, typename F>
static bool castTypeToEither(const T * type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type)) : false) || ...);
}

/// Use Common/TypeList as template argument
template <class Typelist>
static constexpr bool castTypeToEitherTL(const auto * type, auto && f)
{
    return detail::castTypeToEither<Typelist>(
            type, std::forward<decltype(f)>(f),
            std::make_index_sequence<Typelist::size>());
}
}
