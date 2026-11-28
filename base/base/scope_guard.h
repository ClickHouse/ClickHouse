#pragma once

#include <exception>
#include <functional>
#include <memory>
#include <utility>
#include <Common/Exception.h>

/// `may_throw` makes the destructor `noexcept(false)`, so that on a normal scope exit an exception from the function
/// propagates to the caller instead of calling `std::terminate`. Guards made by `make_scope_guard` (`SCOPE_EXIT`) have it.
/// The `scope_guard` type does not: it is kept as a class member, and a potentially throwing member destructor
/// makes the owner's destructor potentially throwing too, which is not allowed to override a virtual `noexcept` one.
/// Nothing is lost, because an owner's destructor is `noexcept` and would terminate on a throw anyway.
template <class F, bool may_throw = false>
class [[nodiscard]] BasicScopeGuard
{
public:
    constexpr BasicScopeGuard() = default;
    constexpr BasicScopeGuard(BasicScopeGuard && src) : function{src.release()} {} // NOLINT(hicpp-noexcept-move, performance-noexcept-move-constructor, cppcoreguidelines-noexcept-move-operations)

    constexpr BasicScopeGuard & operator=(BasicScopeGuard && src) // NOLINT(hicpp-noexcept-move, performance-noexcept-move-constructor, cppcoreguidelines-noexcept-move-operations)
    {
        if (this != &src)
        {
            invoke();
            function = src.release();
        }
        return *this;
    }

    template <typename G, bool other_may_throw>
    requires std::is_convertible_v<G, F>
    constexpr BasicScopeGuard(BasicScopeGuard<G, other_may_throw> && src) : function{src.release()} {} // NOLINT(google-explicit-constructor, cppcoreguidelines-rvalue-reference-param-not-moved, cppcoreguidelines-noexcept-move-operations)

    template <typename G, bool other_may_throw>
    requires std::is_convertible_v<G, F>
    constexpr BasicScopeGuard & operator=(BasicScopeGuard<G, other_may_throw> && src) // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved, cppcoreguidelines-noexcept-move-operations)
    {
        if constexpr (std::is_same_v<G, F> && other_may_throw == may_throw)
        {
            if (this == &src)
                return *this;
        }
        invoke();
        function = src.release();
        return *this;
    }

    template <typename G>
    requires std::is_convertible_v<G, F>
    constexpr BasicScopeGuard(const G & function_) : function{function_} {} // NOLINT(google-explicit-constructor)

    template <typename G>
    requires std::is_convertible_v<G, F>
    constexpr BasicScopeGuard(G && function_) : function{std::move(function_)} {} // NOLINT(google-explicit-constructor, bugprone-forwarding-reference-overload, bugprone-move-forwarding-reference, cppcoreguidelines-missing-std-forward)

    /// While another exception is in flight, an exception from the function is logged and swallowed,
    /// because a throw there would call `std::terminate`.
    ~BasicScopeGuard() noexcept(!may_throw)
    {
        if (exceptions == std::uncaught_exceptions())
        {
            invoke();
        }
        else
        {
            try
            {
                invoke();
            }
            catch (...)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

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

    template <typename G, bool other_may_throw>
    requires std::is_convertible_v<G, F>
    BasicScopeGuard & join(BasicScopeGuard<G, other_may_throw> && other) // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    {
        if (other)
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
    int exceptions{std::uncaught_exceptions()};
};

using scope_guard = BasicScopeGuard<std::function<void()>>;


template <class F>
inline BasicScopeGuard<F, /* may_throw = */ true> make_scope_guard(F && function_) { return std::forward<F>(function_); }

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

/// This is made exclusively for OpenTelemetry tracing handling
/// Runs the function only if the enclosing scope is left by an exception (a "scope fail" guard).
/// Compares with the number of uncaught exceptions at construction rather than with zero.
///
/// it can only react to the fact that the scope failed. It must not throw.
template <class Function>
class [[nodiscard]] BasicScopeFail
{
public:
    explicit BasicScopeFail(Function function_) : function(std::move(function_)) {}
    // no assignment allowed
    BasicScopeFail(const BasicScopeFail &) = delete;
    BasicScopeFail & operator=(const BasicScopeFail &) = delete;

    ~BasicScopeFail()
    {
        /// If there was any exception we run the proper handling
        if (std::uncaught_exceptions() != exceptions_level)
            function();
    }

private:
    Function function;
    int exceptions_level = std::uncaught_exceptions();
};

template <class F>
inline BasicScopeFail<F> make_scope_fail(F && function_) { return BasicScopeFail<F>(std::forward<F>(function_)); }

#define SCOPE_FAIL_CONCAT(n, ...) \
const auto scope_fail##n = make_scope_fail([&] { __VA_ARGS__; })
#define SCOPE_FAIL_FWD(n, ...) SCOPE_FAIL_CONCAT(n, __VA_ARGS__)
#define SCOPE_FAIL(...) SCOPE_FAIL_FWD(__LINE__, __VA_ARGS__)
