#pragma once

#include <utility>

namespace ext
{

template <class F> class scope_guard {
    const F function;

public:
    constexpr scope_guard(const F & function) : function{function} {}
    constexpr scope_guard(F && function) : function{std::move(function)} {}
    ~scope_guard() { function(); }
};

template <class F>
inline scope_guard<F> make_scope_guard(F && function) { return std::forward<F>(function); }

}

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = ext::make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

