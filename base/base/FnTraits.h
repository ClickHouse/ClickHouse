#pragma once

#include "TypeList.h"

namespace detail
{
template <class T>
struct FnTraits { template <class> static constexpr bool value = false; };

template <class R, class ...A>
struct FnTraits<R(A...)>
{
    template <class F>
    static constexpr bool value = std::is_invocable_r_v<R, F, A...>;

    using Ret = R;
    using Args = TypeList<A...>;
};

template <class R, class ...A>
struct FnTraits<R(*)(A...)> : FnTraits<R(A...)> {};
}

template <class T> using FnTraits = detail::FnTraits<T>;

/**
 * A less-typing alias for std::is_invokable_r_v.
 * @example void foo(Fn<bool(int, char)> auto && functor)
 */
template <class F, class FS>
concept Fn = FnTraits<FS>::template value<F>;

template <auto Value>
using Constant = std::integral_constant<decltype(Value), Value>;
