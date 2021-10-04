#pragma once
#include <type_traits>

template <class T, class ...A>
constexpr bool is_any = (std::is_same_v<T, A> || ...);
