#pragma once

#include <Common/SharedMutex.h>

#include <concepts>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{

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

    T * operator->()
    {
        return std::addressof(object);
    }

    const T * operator->() const
    {
        return std::addressof(object);
    }

    T & operator*()
    {
        return object;
    }

    const T & operator*() const
    {
        return object;
    }

private:
    LockGuard lock;
    T & object;
};

template <
    typename T,
    class Mutex = SharedMutex,
    template <class> class UniqueLock = std::unique_lock,
    template <class> class SharedLock = std::shared_lock>
class MutexProtected
{
public:
    using type = T;

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

    template <template <class> class ReadLock = SharedLock, typename Functor>
        requires std::invocable<Functor &&, const T *>
    decltype(auto) accessReadOnly(Functor && functor) const
    {
        ReadLock<Mutex> lock{mutex};
        return std::invoke(std::forward<Functor>(functor), std::addressof(object));
    }

    template <template <class> class WriteLock = UniqueLock, typename Functor>
        requires std::invocable<Functor &&, T *>
    decltype(auto) accessWriteEnabled(Functor && functor)
    {
        WriteLock<Mutex> lock{mutex};
        return std::invoke(std::forward<Functor>(functor), std::addressof(object));
    }

    template <template <class> class ReadLock = SharedLock>
    [[nodiscard]] auto getReadOnly() const
        -> MutexProtectedAccessor<const T, ReadLock<Mutex>>
    {
        return {mutex, object};
    }

    template <template <class> class WriteLock = UniqueLock>
    [[nodiscard]] auto getWriteEnabled()
        -> MutexProtectedAccessor<T, WriteLock<Mutex>>
    {
        return {mutex, object};
    }

private:
    mutable Mutex mutex;
    T object;
};

template <typename T>
MutexProtected(T) -> MutexProtected<T>;

}
