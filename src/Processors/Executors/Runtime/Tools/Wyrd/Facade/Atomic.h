#pragma once


#include <atomic>
#include <type_traits>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/InjectFault.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

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
        injectFault("atomic::load");
        T result = impl.load();
        return result;
    }

    void store(T desired)
    {
        injectFault("atomic::store");
        impl.store(desired);
    }

    T exchange(T desired)
    {
        injectFault("atomic::exchange");
        T result = impl.exchange(desired);
        return result;
    }

    bool compare_exchange_weak(T & expected, T desired)
    {
        injectFault("atomic::compare_exchange_weak");
        bool result = !spuriousFailure() && impl.compare_exchange_weak(expected, desired);
        if (!result)
            expected = impl.load();
        return result;
    }

    bool compare_exchange_strong(T & expected, T desired)
    {
        injectFault("atomic::compare_exchange_strong");
        bool result = impl.compare_exchange_strong(expected, desired);
        return result;
    }

    T fetch_add(T delta) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_add");
        T result = impl.fetch_add(delta);
        return result;
    }

    T fetch_sub(T delta) requires std::is_integral_v<T>
    {
        injectFault("atomic::fetch_sub");
        T result = impl.fetch_sub(delta);
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
