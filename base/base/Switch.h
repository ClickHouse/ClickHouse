#pragma once

#include <type_traits>

template <bool V, class T> struct Case : std::bool_constant<V> { using type = T; };
template <class T> using DefaultCase = Case<true, T>;

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true
template <class ...Cases> using Switch = typename std::disjunction<Cases...>::type;
