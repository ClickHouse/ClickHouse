#pragma once


#include <atomic>
#include <limits>
#include <source_location>
#include <string>
#include <type_traits>
#include <utility>

#include <fmt/format.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/InjectFault.h>
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <optional>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

template <typename T>
auto formatArgument(const T & value)
{
    if constexpr (std::is_pointer_v<T>)
        return fmt::ptr(value);
    else if constexpr (std::is_enum_v<T>)
        return formatArgument(fmt::underlying(value));
    else if constexpr (std::is_same_v<T, char8_t>)
        return unsigned{value};
    else
        return value;
}

template <typename T>
class Atomic
{
public:
    Atomic() = default;

    constexpr Atomic(T desired) /// NOLINT(google-explicit-constructor)
        : impl(desired)
    {
    }

    Atomic(const Atomic &) = delete;
    Atomic & operator=(const Atomic &) = delete;

    T load(std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) const
    {
        injectFault("atomic::load", location);
        T result = impl.load(order);
        trace(fmt::format("{}: atomic::load -> {}", name, formatArgument(result)));
        return result;
    }

    void store(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::store", location);
        impl.store(desired, order);
        trace(fmt::format("{}: atomic::store -> {}", name, formatArgument(desired)));
    }

    T exchange(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::exchange", location);
        T result = impl.exchange(desired, order);
        trace(fmt::format("{}: atomic::exchange -> {}", name, formatArgument(result)));
        return result;
    }

    bool compare_exchange_weak(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_weak", location);
        bool result = !spuriousFailure(location) && impl.compare_exchange_weak(expected, desired, success, failure);
        if (!result)
            expected = impl.load(failure);
        trace(fmt::format("{}: atomic::compare_exchange_weak -> {}, saw {}", name, result, formatArgument(expected)));
        return result;
    }

    bool compare_exchange_strong(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_strong", location);
        bool result = impl.compare_exchange_strong(expected, desired, success, failure);
        trace(fmt::format("{}: atomic::compare_exchange_strong -> {}, saw {}", name, result, formatArgument(expected)));
        return result;
    }

    T fetch_add(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_add", location);
        T result = impl.fetch_add(delta, order);
        trace(fmt::format("{}: atomic::fetch_add -> {}", name, formatArgument(result)));
        return result;
    }

    T fetch_sub(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_sub", location);
        T result = impl.fetch_sub(delta, order);
        trace(fmt::format("{}: atomic::fetch_sub -> {}", name, formatArgument(result)));
        return result;
    }

    void wait(T old, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::wait", location);
        while (impl.load(order) == old)
            futexWait(this, location);
        trace(fmt::format("{}: atomic::wait -> {}", name, formatArgument(impl.load(order))));
    }

    void notify_one(std::source_location location = std::source_location::current())
    {
        injectFault("atomic::notify_one", location);
        futexWake(this, 1, location);
    }

    void notify_all(std::source_location location = std::source_location::current())
    {
        injectFault("atomic::notify_all", location);
        futexWake(this, std::numeric_limits<size_t>::max(), location);
    }

    std::atomic<T> & underlying()
    {
        return impl;
    }

    void setName(std::string new_name)
    {
        name = std::move(new_name);
    }

    [[nodiscard]] const std::string & getName() const
    {
        return name;
    }

private:
    std::atomic<T> impl{};
    std::string name = fmt::format("{}", fmt::ptr(this));
};

template <typename T, typename... Args>
void setName(Atomic<T> & atomic, fmt::format_string<Args...> format, Args &&... args)
{
    atomic.setName(fmt::format(format, std::forward<Args>(args)...));
}

}

namespace AdversaryMode
{

template <typename T>
class Atomic
{
public:
    Atomic() = default;

    constexpr Atomic(T desired) /// NOLINT(google-explicit-constructor)
        : impl(desired)
    {
    }

    Atomic(const Atomic &) = delete;
    Atomic & operator=(const Atomic &) = delete;

    T load(std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) const
    {
        injectFault("atomic::load", location);
        return impl.load(order);
    }

    void store(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::store", location);
        impl.store(desired, order);
    }

    T exchange(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::exchange", location);
        return impl.exchange(desired, order);
    }

    bool compare_exchange_weak(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_weak", location);
        bool result = !spuriousFailure(location) && impl.compare_exchange_weak(expected, desired, success, failure);
        if (!result)
            expected = impl.load(failure);
        return result;
    }

    bool compare_exchange_strong(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_strong", location);
        return impl.compare_exchange_strong(expected, desired, success, failure);
    }

    T fetch_add(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_add", location);
        return impl.fetch_add(delta, order);
    }

    T fetch_sub(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_sub", location);
        return impl.fetch_sub(delta, order);
    }

    void wait(T old, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::wait", location);
        impl.wait(old, order);
    }

    void notify_one(std::source_location location = std::source_location::current())
    {
        injectFault("atomic::notify_one", location);
        impl.notify_one();
    }

    void notify_all(std::source_location location = std::source_location::current())
    {
        injectFault("atomic::notify_all", location);
        impl.notify_all();
    }

    std::atomic<T> & underlying()
    {
        return impl;
    }

private:
    std::atomic<T> impl{};
};

template <typename T, typename... Args>
void setName(Atomic<T> &, fmt::format_string<Args...>, Args &&...)
{
}

}

template <typename T>
class WyrdAtomic
{
public:
    WyrdAtomic()
    {
        if (getMode() == Mode::Simulation)
            simulation_impl.emplace();
        else
            adversary_impl.emplace();
    }

    WyrdAtomic(T desired) /// NOLINT(google-explicit-constructor)
    {
        if (getMode() == Mode::Simulation)
            simulation_impl.emplace(desired);
        else
            adversary_impl.emplace(desired);
    }

    WyrdAtomic(const WyrdAtomic &) = delete;
    WyrdAtomic & operator=(const WyrdAtomic &) = delete;

    T load(std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) const
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->load(order, location);
        else
            return adversary_impl->load(order, location);
    }

    void store(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            simulation_impl->store(desired, order, location);
        else
            adversary_impl->store(desired, order, location);
    }

    T exchange(T desired, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->exchange(desired, order, location);
        else
            return adversary_impl->exchange(desired, order, location);
    }

    bool compare_exchange_weak(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->compare_exchange_weak(expected, desired, success, failure, location);
        else
            return adversary_impl->compare_exchange_weak(expected, desired, success, failure, location);
    }

    bool compare_exchange_strong(T & expected, T desired, std::memory_order success = std::memory_order::seq_cst, std::memory_order failure = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->compare_exchange_strong(expected, desired, success, failure, location);
        else
            return adversary_impl->compare_exchange_strong(expected, desired, success, failure, location);
    }

    T fetch_add(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->fetch_add(delta, order, location);
        else
            return adversary_impl->fetch_add(delta, order, location);
    }

    T fetch_sub(T delta, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->fetch_sub(delta, order, location);
        else
            return adversary_impl->fetch_sub(delta, order, location);
    }

    void wait(T old, std::memory_order order = std::memory_order::seq_cst, std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            simulation_impl->wait(old, order, location);
        else
            adversary_impl->wait(old, order, location);
    }

    void notify_one(std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            simulation_impl->notify_one(location);
        else
            adversary_impl->notify_one(location);
    }

    void notify_all(std::source_location location = std::source_location::current())
    {
        if (getMode() == Mode::Simulation)
            simulation_impl->notify_all(location);
        else
            adversary_impl->notify_all(location);
    }

    std::atomic<T> & underlying()
    {
        if (getMode() == Mode::Simulation)
            return simulation_impl->underlying();
        else
            return adversary_impl->underlying();
    }

    SimulationMode::Atomic<T> & simulation()
    {
        return *simulation_impl;
    }

    AdversaryMode::Atomic<T> & adversary()
    {
        return *adversary_impl;
    }

private:
    std::optional<SimulationMode::Atomic<T>> simulation_impl;
    std::optional<AdversaryMode::Atomic<T>> adversary_impl;
};

template <typename T>
using Atomic = WyrdAtomic<T>;

template <typename T, typename... Args>
void setName(Atomic<T> & atomic, fmt::format_string<Args...> format, Args &&... args)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::setName(atomic.simulation(), format, std::forward<Args>(args)...);
    else
        AdversaryMode::setName(atomic.adversary(), format, std::forward<Args>(args)...);
}

#else

template <typename T>
using Atomic = std::atomic<T>;

template <typename T, typename... Args>
void setName(Atomic<T> &, fmt::format_string<Args...>, Args &&...)
{
}

#endif

}
