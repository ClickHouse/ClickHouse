#pragma once

#include <cstddef>
#include <type_traits>
#include <utility>
#include "defines.h"
#include "TypePair.h"

/// General-purpose typelist. Easy on compilation times as it does not use recursion.
template <class ...Args>
struct TL { static constexpr size_t size = sizeof...(Args); };

namespace TLUtils /// In some contexts it's more handy to use functions instead of aliases
{
template <class ...LArgs, class ...RArgs>
constexpr TL<LArgs..., RArgs...> concat(TL<LArgs...>, TL<RArgs...>) { return {}; }

template <class T, class ...Args>
constexpr TL<T, Args...> prepend(TL<Args...>) { return {}; }

template <class T, class ...Args>
constexpr TL<Args..., T> append(TL<Args...>) { return {}; }

template <template<class> class F, class ...Args>
constexpr TL<F<Args>...> map(TL<Args...>) { return {}; }

template <template<class...> class Root, class ...Args>
constexpr Root<Args...> changeRoot(TL<Args...>) { return {}; }

template <class F, class ...Args>
constexpr void forEach(TL<Args...>, F && f) { (std::forward<F>(f)(Id<Args>{}), ...); }
}

template <class TLLeft, class TLRight>
using TLConcat = decltype(TLUtils::concat(TLLeft{}, TLRight{}));

template <class T, class TL> using TLPrepend = decltype(TLUtils::prepend<T>(TL{}));
template <class T, class TL> using TLAppend = decltype(TLUtils::append<T>(TL{}));

template <template<class> class F, class TL>
using TLMap = decltype(TLUtils::map<F>(TL{}));

template <template<class...> class Root, class TL>
using TLChangeRoot = decltype(TLUtils::changeRoot<Root>(TL{}));
