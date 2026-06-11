#pragma once

#include <cstdlib>

/** \brief Returns number of elements in an automatic array. */
template <typename T, std::size_t N>
constexpr size_t arraySize(const T (&)[N]) noexcept { return N; }
