#pragma once

#include <functional>
#include <memory>
#include <utility>


namespace ext
{
template <class F>
class [[nodiscard]] basic_scope_guard
{
public:
    constexpr basic_scope_guard() = default;
    constexpr basic_scope_guard(basic_scope_guard && src) : function{std::exchange(src.function, {})} {}

    constexpr basic_scope_guard & operator=(basic_scope_guard && src)
    {
        if (this != &src)
        {
            invoke();
            function = std::exchange(src.function, {});
        }
        return *this;
    }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(basic_scope_guard<G> && src) : function{std::exchange(src.function, {})} {}

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard & operator=(basic_scope_guard<G> && src)
    {
        if (this != &src)
        {
            invoke();
            function = std::exchange(src.function, {});
        }
        return *this;
    }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(const G & function_) : function{function_} {}

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(G && function_) : function{std::move(function_)} {}

    ~basic_scope_guard() { invoke(); }

    explicit operator bool() const
    {
        if constexpr (std::is_constructible_v<bool, F>)
            return static_cast<bool>(function);
        return true;
    }

    void reset() { function = {}; }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    basic_scope_guard<F> & join(basic_scope_guard<G> && other)
    {
        if (other.function)
        {
            if (function)
            {
                function = [x = std::make_shared<std::pair<F, G>>(std::move(function), std::exchange(other.function, {}))]()
                {
                    std::move(x->first)();
                    std::move(x->second)();
                };
            }
            else
                function = std::exchange(other.function, {});
        }
        return *this;
    }

private:
    void invoke()
    {
        if constexpr (std::is_constructible_v<bool, F>)
        {
            if (!function)
                return;
        }
        std::move(function)();
    }

    F function = F{};
};

using scope_guard = basic_scope_guard<std::function<void(void)>>;


template <class F>
inline basic_scope_guard<F> make_scope_guard(F && function_) { return std::forward<F>(function_); }
}

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = ext::make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

