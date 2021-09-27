#pragma once

#include <tuple>
#include <type_traits>

namespace detail
{
template <class T>
struct FnTraits;

template <class R, class ...Args>
struct FnTraits<R(*)(Args...)>
{
    using Ret = R;
    using DecayedArgs = std::tuple<typename std::decay<Args>::type...>;
};

template<class F, class> struct Fn;
template<class F, class R, class ...Args> struct Fn<F, R(Args...)>
{
    static constexpr bool value = std::is_invocable_r_v<R, F, Args...>;
};
}

template <class T> using FnTraits = detail::FnTraits<T>;

/**
 * A less-typing alias for std::is_invokable_r_v.
 * @example void foo(Fn<bool(int, char)> auto && functor)
 */
template <class F, class ArgsAndRet>
concept Fn = detail::Fn<F, ArgsAndRet>::value;

template <auto Value>
using Constant = std::integral_constant<decltype(Value), Value>;
