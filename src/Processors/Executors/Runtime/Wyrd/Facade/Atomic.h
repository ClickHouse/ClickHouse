#pragma once


#include <atomic>
#include <limits>
#include <source_location>
#include <type_traits>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/InjectFault.h>
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

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

    T load(std::source_location location = std::source_location::current()) const
    {
        injectFault("atomic::load", location);
        T result = impl.load();
        return result;
    }

    void store(T desired, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::store", location);
        impl.store(desired);
    }

    T exchange(T desired, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::exchange", location);
        T result = impl.exchange(desired);
        return result;
    }

    bool compare_exchange_weak(T & expected, T desired, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_weak", location);
        bool result = !spuriousFailure(location) && impl.compare_exchange_weak(expected, desired);
        if (!result)
            expected = impl.load();
        return result;
    }

    bool compare_exchange_strong(T & expected, T desired, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::compare_exchange_strong", location);
        bool result = impl.compare_exchange_strong(expected, desired);
        return result;
    }

    T fetch_add(T delta, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_add", location);
        T result = impl.fetch_add(delta);
        return result;
    }

    T fetch_sub(T delta, std::source_location location = std::source_location::current()) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_sub", location);
        T result = impl.fetch_sub(delta);
        return result;
    }

#if defined(WYRD_SIMULATION)

    void wait(T old, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::wait", location);
        while (impl.load() == old)
            futexWait(this, location);
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

#else

    void wait(T old, std::source_location location = std::source_location::current())
    {
        injectFault("atomic::wait", location);
        impl.wait(old);
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

#endif

    operator T() const /// NOLINT(google-explicit-constructor)
    {
        return load();
    }

    T operator=(T desired) /// NOLINT(misc-unconventional-assign-operator)
    {
        store(desired);
        return desired;
    }

    std::atomic<T> & underlying()
    {
        return impl;
    }

private:
    std::atomic<T> impl{};
};

#else

template <typename T>
using Atomic = std::atomic<T>;

#endif

}
