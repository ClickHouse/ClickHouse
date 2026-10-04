#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>

#include <atomic>
#include <type_traits>

#if defined(EXECUTOR_SIM)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/InjectFault.h>
#elif defined(EXECUTOR_FAULT_INJECTION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM) || defined(EXECUTOR_FAULT_INJECTION)

template <typename T>
class Atomic
{
public:
    Atomic() = default;

    constexpr Atomic(T desired)
        : impl(desired)
    {
    }

    Atomic(const Atomic &) = delete;
    Atomic & operator=(const Atomic &) = delete;

    T load() const
    {
        injectFaultBefore("atomic::load");
        T result = impl.load();
        injectFaultAfter("atomic::load");
        return result;
    }

    void store(T desired)
    {
        injectFaultBefore("atomic::store");
        impl.store(desired);
        injectFaultAfter("atomic::store");
    }

    T exchange(T desired)
    {
        injectFaultBefore("atomic::exchange");
        T result = impl.exchange(desired);
        injectFaultAfter("atomic::exchange");
        return result;
    }

    bool compare_exchange_weak(T & expected, T desired)
    {
        injectFaultBefore("atomic::compare_exchange_weak");
        bool result = !spuriousFailure() && impl.compare_exchange_weak(expected, desired);
        if (!result)
            expected = impl.load();
        injectFaultAfter("atomic::compare_exchange_weak");
        return result;
    }

    bool compare_exchange_strong(T & expected, T desired)
    {
        injectFaultBefore("atomic::compare_exchange_strong");
        bool result = impl.compare_exchange_strong(expected, desired);
        injectFaultAfter("atomic::compare_exchange_strong");
        return result;
    }

    T fetch_add(T delta) requires std::is_integral_v<T>
    {
        injectFaultBefore("atomic::fetch_add");
        T result = impl.fetch_add(delta);
        injectFaultAfter("atomic::fetch_add");
        return result;
    }

    T fetch_sub(T delta) requires std::is_integral_v<T>
    {
        injectFaultBefore("atomic::fetch_sub");
        T result = impl.fetch_sub(delta);
        injectFaultAfter("atomic::fetch_sub");
        return result;
    }

    T operator++() requires std::is_integral_v<T>
    {
        return fetch_add(1) + 1;
    }

    T operator++(int) requires std::is_integral_v<T>
    {
        return fetch_add(1);
    }

    T operator--() requires std::is_integral_v<T>
    {
        return fetch_sub(1) - 1;
    }

    T operator--(int) requires std::is_integral_v<T>
    {
        return fetch_sub(1);
    }

    operator T() const
    {
        return load();
    }

    T operator=(T desired)
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
