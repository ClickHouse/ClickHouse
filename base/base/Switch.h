#pragma once

#include <type_traits>

template <bool V, class T> struct Case : std::bool_constant<V> { using type = T; };

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true, D if none.
template <class D, class... T>
using Switch = typename std::disjunction<T..., Case<true, D>>::type;
