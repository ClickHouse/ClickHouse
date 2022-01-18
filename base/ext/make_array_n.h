#pragma once

#include <utility>
#include <type_traits>
#include <array>


/** \brief Produces std::array of specified size, containing copies of provided object.
  *    Copy is performed N-1 times, and the last element is being moved.
  * This helper allows to initialize std::array in place.
  */
namespace ext
{
    namespace detail
    {

        template<std::size_t size, typename T, std::size_t... indexes>
        constexpr auto make_array_n_impl(T && value, std::index_sequence<indexes...>)
        {
            /// Comma is used to make N-1 copies of value
            return std::array<std::decay_t<T>, size>{ (static_cast<void>(indexes), value)..., std::forward<T>(value) };
        }

    }

    template<typename T>
    constexpr auto make_array_n(std::integral_constant<std::size_t, 0>, T &&)
    {
        return std::array<std::decay_t<T>, 0>{};
    }

    template<std::size_t size, typename T>
    constexpr auto make_array_n(std::integral_constant<std::size_t, size>, T && value)
    {
        return detail::make_array_n_impl<size>(std::forward<T>(value), std::make_index_sequence<size - 1>{});
    }

    template<std::size_t size, typename T>
    constexpr auto make_array_n(T && value)
    {
        return make_array_n(std::integral_constant<std::size_t, size>{}, std::forward<T>(value));
    }
}
