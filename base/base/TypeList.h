#pragma once

#include <cstddef>
#include <type_traits>
#include <utility>
#include "defines.h"
#include "TypePair.h"

/// General-purpose typelist. Easy on compilation times as it does not use recursion.
template <typename ...Args>
struct TypeList { static constexpr size_t size = sizeof...(Args); };

namespace TypeListUtils /// In some contexts it's more handy to use functions instead of aliases
{
    template <typename ...LArgs, typename ...RArgs>
    constexpr TypeList<LArgs..., RArgs...> concat(TypeList<LArgs...>, TypeList<RArgs...>) { return {}; }

    template <typename T, typename ...Args>
    constexpr TypeList<T, Args...> prepend(TypeList<Args...>) { return {}; }

    template <typename T, typename ...Args>
    constexpr TypeList<Args..., T> append(TypeList<Args...>) { return {}; }

    template <template <typename> typename F, typename ...Args>
    constexpr TypeList<F<Args>...> map(TypeList<Args...>) { return {}; }

    template <template <typename...> typename Root, typename ...Args>
    constexpr Root<Args...> changeRoot(TypeList<Args...>) { return {}; }

    template <typename F, typename ...Args>
    constexpr void forEach(TypeList<Args...>, F && f) { (std::forward<F>(f)(Id<Args>{}), ...); }
}

template <typename TypeListLeft, typename TypeListRight>
using TypeListConcat = decltype(TypeListUtils::concat(TypeListLeft{}, TypeListRight{}));

template <typename T, typename List> using TypeListPrepend = decltype(TypeListUtils::prepend<T>(List{}));
template <typename T, typename List> using TypeListAppend = decltype(TypeListUtils::append<T>(List{}));

template <template <typename> typename F, typename List>
using TypeListMap = decltype(TypeListUtils::map<F>(List{}));

template <template <typename...> typename Root, typename List>
using TypeListChangeRoot = decltype(TypeListUtils::changeRoot<Root>(List{}));
