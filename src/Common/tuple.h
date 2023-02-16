#pragma once

#include <tuple>
#include <utility>

namespace detail
{
template <typename C, std::size_t... Is>
auto to_tuple_impl(C && c, std::index_sequence<Is...>)
{
    return std::tie(c[Is]...);
}
}

template <std::size_t N, typename C>
auto to_tuple(C && c)
{
    return detail::to_tuple_impl(c, std::make_index_sequence<N>());
}
