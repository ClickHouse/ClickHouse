#pragma once

#include <cstddef>
#include <type_traits>
#include <utility>
#include "defines.h"
#include "TypePair.h"

/// General-purpose typelist. Easy on compilation times as it does not use recursion.
template <class ...Args>
struct Typelist { static constexpr size_t size = sizeof...(Args); };

namespace TLUtils /// In some contexts it's more handy to use functions instead of aliases
{
template <class ...LArgs, class ...RArgs>
constexpr Typelist<LArgs..., RArgs...> concat(Typelist<LArgs...>, Typelist<RArgs...>) { return {}; }

template <class T, class ...Args>
constexpr Typelist<T, Args...> prepend(Typelist<Args...>) { return {}; }

template <class T, class ...Args>
constexpr Typelist<Args..., T> append(Typelist<Args...>) { return {}; }

template <template<class> class F, class ...Args>
constexpr Typelist<F<Args>...> map(Typelist<Args...>) { return {}; }

template <template<class...> class Root, class ...Args>
constexpr Root<Args...> changeRoot(Typelist<Args...>) { return {}; }

template <class F, class ...Args>
constexpr void forEach(Typelist<Args...>, F && f) { (std::forward<F>(f)(Id<Args>{}), ...); }
}

template <class TLLeft, class TLRight>
using TLConcat = decltype(TLUtils::concat(TLLeft{}, TLRight{}));

template <class T, class Typelist> using TLPrepend = decltype(TLUtils::prepend<T>(Typelist{}));
template <class T, class Typelist> using TLAppend = decltype(TLUtils::append<T>(Typelist{}));

template <template<class> class F, class Typelist>
using TLMap = decltype(TLUtils::map<F>(Typelist{}));

template <template<class...> class Root, class Typelist>
using TLChangeRoot = decltype(TLUtils::changeRoot<Root>(Typelist{}));
