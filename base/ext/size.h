#pragma once

#include <cstdlib>


namespace ext
{
    /** \brief Returns number of elements in an automatic array. */
    template <typename T, std::size_t N>
    constexpr std::size_t size(const T (&)[N]) noexcept { return N; }

    /** \brief Returns number of in a container providing size() member function. */
    template <typename T> constexpr auto size(const T & t) { return t.size(); }
}
