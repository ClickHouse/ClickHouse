#pragma once

#include <Common/SharedMutex.h>

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

namespace DB
{

template <typename FirstAccessor, typename SecondAccessor>
class [[nodiscard]] LockOrderedAccessorPair;

/// Provides scoped access to a mutex-protected object while holding a lock guard.
template <typename T, typename LockGuard>
class MutexProtectedAccessor
{
public:
    template <typename Mutex>
    MutexProtectedAccessor(Mutex & mutex_, T & object_)
        : lock(mutex_)
        , object(object_)
    {
    }

    MutexProtectedAccessor(const MutexProtectedAccessor &) = delete;
    MutexProtectedAccessor & operator=(const MutexProtectedAccessor &) = delete;
    MutexProtectedAccessor(MutexProtectedAccessor &&) = delete;
    MutexProtectedAccessor & operator=(MutexProtectedAccessor &&) = delete;

    T * operator->() &
    {
        return std::addressof(object);
    }

    const T * operator->() const &
    {
        return std::addressof(object);
    }

    T * operator->() && = delete;
    const T * operator->() const && = delete;

    T & operator*() &
    {
        return object;
    }

    const T & operator*() const &
    {
        return object;
    }

    T & operator*() && = delete;
    const T & operator*() const && = delete;

private:
    using ObjectType = T;
    using LockGuardType = LockGuard;

    MutexProtectedAccessor(LockGuard && lock_, T & object_)
        : lock(std::move(lock_))
        , object(object_)
    {
    }

    template <typename, typename>
    friend class LockOrderedAccessorPair;

    LockGuard lock;
    T & object;
};

namespace detail
{

template <typename Accessor, typename Protected>
struct AccessRequest
{
    Protected & protected_object;
};

}

/// Acquires two requested locks in address order and transfers them to accessors when decomposed with a structured binding.
template <typename FirstAccessor, typename SecondAccessor>
class [[nodiscard]] LockOrderedAccessorPair
{
private:
    using FirstLock = typename FirstAccessor::LockGuardType;
    using SecondLock = typename SecondAccessor::LockGuardType;
    using FirstObject = typename FirstAccessor::ObjectType;
    using SecondObject = typename SecondAccessor::ObjectType;

    static_assert(
        std::is_nothrow_move_constructible_v<FirstLock> && std::is_nothrow_move_constructible_v<SecondLock>,
        "LockOrderedAccessorPair requires nothrow-movable lock guards");

public:
    template <typename FirstProtected, typename SecondProtected>
    LockOrderedAccessorPair(
        detail::AccessRequest<FirstAccessor, FirstProtected> first,
        detail::AccessRequest<SecondAccessor, SecondProtected> second)
        : objects(first.protected_object.object, second.protected_object.object)
    {
        auto & first_protected = first.protected_object;
        auto & second_protected = second.protected_object;
        const void * first_address = std::addressof(first_protected);
        const void * second_address = std::addressof(second_protected);

        if (first_address == second_address)
            throw std::invalid_argument("Cannot acquire two accessors to the same MutexProtected object");

        if (std::less<const void *>{}(first_address, second_address))
        {
            std::get<0>(locks).emplace(first_protected.mutex);
            std::get<1>(locks).emplace(second_protected.mutex);
        }
        else
        {
            std::get<1>(locks).emplace(second_protected.mutex);
            std::get<0>(locks).emplace(first_protected.mutex);
        }
    }

    LockOrderedAccessorPair(const LockOrderedAccessorPair &) = delete;
    LockOrderedAccessorPair & operator=(const LockOrderedAccessorPair &) = delete;
    LockOrderedAccessorPair(LockOrderedAccessorPair &&) = delete;
    LockOrderedAccessorPair & operator=(LockOrderedAccessorPair &&) = delete;

    template <size_t index>
    [[nodiscard]] auto get() &&
    {
        static_assert(index < 2);
        if constexpr (index == 0)
        {
            auto lock = std::exchange(std::get<0>(locks), std::nullopt);
            if (!lock)
                throw std::logic_error("First accessor was already extracted");
            return FirstAccessor(std::move(*lock), std::get<0>(objects));
        }
        else
        {
            auto lock = std::exchange(std::get<1>(locks), std::nullopt);
            if (!lock)
                throw std::logic_error("Second accessor was already extracted");
            return SecondAccessor(std::move(*lock), std::get<1>(objects));
        }
    }

private:
    std::tuple<FirstObject &, SecondObject &> objects;
    std::tuple<std::optional<FirstLock>, std::optional<SecondLock>> locks;
};

template <typename FirstAccessor, typename FirstProtected, typename SecondAccessor, typename SecondProtected>
LockOrderedAccessorPair(
    detail::AccessRequest<FirstAccessor, FirstProtected>,
    detail::AccessRequest<SecondAccessor, SecondProtected>)
    -> LockOrderedAccessorPair<FirstAccessor, SecondAccessor>;

/// Protects an object with a mutex and provides scoped read-only or write-enabled access.
template <
    typename T,
    class Mutex = SharedMutex,
    template <class> class UniqueLock = std::unique_lock,
    template <class> class SharedLock = std::shared_lock>
class MutexProtected
{
public:
    using type = T;
    using ReadOnlyAccessor = MutexProtectedAccessor<const T, SharedLock<Mutex>>;
    using WriteEnabledAccessor = MutexProtectedAccessor<T, UniqueLock<Mutex>>;

    MutexProtected()
        : mutex()
        , object()
    {
    }

    explicit MutexProtected(T object_)
        : mutex()
        , object(std::move(object_))
    {
    }

    template <typename... Args>
    explicit MutexProtected(std::in_place_t, Args &&... args)
        : mutex()
        , object(std::forward<Args>(args)...)
    {
    }

    [[nodiscard]] auto getReadOnly() const &
        -> ReadOnlyAccessor
    {
        return {mutex, object};
    }

    auto getReadOnly() const &&
        -> ReadOnlyAccessor = delete;

    [[nodiscard]] auto getWriteEnabled() &
        -> WriteEnabledAccessor
    {
        return {mutex, object};
    }

    auto getWriteEnabled() &&
        -> WriteEnabledAccessor = delete;

private:
    template <typename, typename>
    friend class LockOrderedAccessorPair;

    mutable Mutex mutex;
    T object;
};

template <typename T>
MutexProtected(T) -> MutexProtected<T>;

namespace detail
{

template <typename>
inline constexpr bool is_mutex_protected = false;

template <
    typename T,
    class Mutex,
    template <class> class UniqueLock,
    template <class> class SharedLock>
inline constexpr bool is_mutex_protected<MutexProtected<T, Mutex, UniqueLock, SharedLock>> = true;

template <typename Protected>
concept MutexProtectedType = !std::is_volatile_v<Protected> && is_mutex_protected<std::remove_cv_t<Protected>>;

}

template <detail::MutexProtectedType Protected>
[[nodiscard]] auto readOnly(Protected & protected_)
{
    using Base = std::remove_const_t<Protected>;
    return detail::AccessRequest<typename Base::ReadOnlyAccessor, const Base>{protected_};
}

template <detail::MutexProtectedType Protected>
    requires (!std::is_const_v<Protected>)
[[nodiscard]] auto writeEnabled(Protected & protected_)
{
    return detail::AccessRequest<typename Protected::WriteEnabledAccessor, Protected>{protected_};
}

}

namespace std
{

template <typename FirstAccessor, typename SecondAccessor>
struct tuple_size<DB::LockOrderedAccessorPair<FirstAccessor, SecondAccessor>> : integral_constant<size_t, 2>
{
};

template <typename FirstAccessor, typename SecondAccessor>
struct tuple_element<0, DB::LockOrderedAccessorPair<FirstAccessor, SecondAccessor>>
{
    using type = FirstAccessor;
};

template <typename FirstAccessor, typename SecondAccessor>
struct tuple_element<1, DB::LockOrderedAccessorPair<FirstAccessor, SecondAccessor>>
{
    using type = SecondAccessor;
};

}
