#pragma once

#include <type_traits>
#include <utility>

//template <auto Val, decltype(Val)... List>
//constexpr bool static_in_v = std::disjunction_v<std::bool_constant<Val == List>...>;

namespace detail
{
template <auto Container>
concept has_begin_end = requires(decltype(Container) a) {
    std::begin(a);
    std::end(a);
};

template <class F, auto Elem, class R>
concept returns_elem = std::is_same_v<
    R
    std::invoke_result_t<F,
        std::integral_constant<decltype(Elem), Elem>>;
};

template <typename T, T Begin, T... Is>
constexpr bool static_for(auto && f, std::integer_sequence<T, Is...>)
{
    return (
        std::forward<decltype(f)>(f)(
            std::integral_constant<T, Begin + Is>())
        || ...);
}

/**
 * Compile-time iteration over values. Iterates over range [Begin; End), invokes @a f on every item.
 * @a f is invoked with <tt>std::integral_constant<decltype(Begin), Item></tt> so inside functor call it can be used in
 * compile-time context (e.g. template parameter).
 *
 * @param f Functor that is invoked on elements within @a Begin and @a End.
 *  It should either return @c void (in which case it is invoked on every element in range) or @c bool
 *  (so when functor returns @c true, iteration will stop).
 *
 * @code{.cpp}
 * template <size_t I> void foo();
 *
 * constexpr auto Ints = {1, 2, 3, 4, 5};
 *
 * static_for<Ints.begin(), Ints.end()>([](auto int_constant) {
 *     foo<int_constant>(); /// Will invoke foo<1>() ... foo<5>();
 *     return false;
 * });
 *
 * static_for<Ints.begin(), Ints.end()>([](auto int_constant) {
 *     foo<int_constant>(); /// Will invoke foo<1>() ... foo<4>();
 *
 *     if constexpr (int_constant > 4)
 *         return true;
 *     else
 *         return false;
 * });
 * @endcode
 */
template <auto Begin, decltype(Begin) End>
constexpr bool static_for(detail::returns_elem<Begin, bool> && f)
{
    using T = decltype(Begin);

    return detail::static_for<T, Begin>(
        std::forward<decltype(f)>(f),
        std::make_integer_sequence<T, End - Begin>{});
}

/**
 * Same as <tt>static_for<Begin, End>(func)</tt>, but without need of explicit specification of @a [begin;end) range.
 *
 * @code{.cpp}
 * template <size_t I> void foo() { std::cout << I; }
 *
 * constexpr auto Ints = {1, 2, 3, 5};
 *
 * static_for<Ints>([](auto int_constant) { /// Will print 1235
 *     foo<int_constant>();
 *     return false;
 * }
 * @endcode
 */
template <auto Container>
constexpr bool static_for(auto && func) requires detail::has_begin_end<Container>
{
    return static_for<Container.begin(), Container.end()>(
        std::forward<decltype(func)>(func));
}
