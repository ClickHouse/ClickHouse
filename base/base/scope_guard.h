#pragma once

#include <functional>
#include <memory>
#include <utility>

template <class F>
class [[nodiscard]] basic_scope_guard
{
public:
    constexpr basic_scope_guard() = default;
    constexpr basic_scope_guard(basic_scope_guard && src) : function{src.release()} {}

    constexpr basic_scope_guard & operator=(basic_scope_guard && src)
    {
        if (this != &src)
        {
            invoke();
            function = src.release();
        }
        return *this;
    }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(basic_scope_guard<G> && src) : function{src.release()} {}

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard & operator=(basic_scope_guard<G> && src)
    {
        if (this != &src)
        {
            invoke();
            function = src.release();
        }
        return *this;
    }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(const G & function_) : function{function_} {}

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    constexpr basic_scope_guard(G && function_) : function{std::move(function_)} {}

    ~basic_scope_guard() { invoke(); }

    static constexpr bool is_nullable = std::is_constructible_v<bool, F>;

    explicit operator bool() const
    {
        if constexpr (is_nullable)
            return static_cast<bool>(function);
        return true;
    }

    void reset()
    {
        invoke();
        release();
    }

    F release()
    {
        static_assert(is_nullable);
        return std::exchange(function, {});
    }

    template <typename G, typename = std::enable_if_t<std::is_convertible_v<G, F>, void>>
    basic_scope_guard<F> & join(basic_scope_guard<G> && other)
    {
        if (other.function)
        {
            if (function)
            {
                function = [x = std::make_shared<std::pair<F, G>>(std::move(function), other.release())]()
                {
                    std::move(x->first)();
                    std::move(x->second)();
                };
            }
            else
                function = other.release();
        }
        return *this;
    }

private:
    void invoke()
    {
        if constexpr (is_nullable)
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

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

