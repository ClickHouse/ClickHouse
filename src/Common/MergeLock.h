#pragma once

#include <base/defines.h>

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>

namespace DB
{

/// Locks the source shared and the destination exclusively in mutex address order.
template <class Mutex>
class TSA_SCOPED_LOCKABLE MergeLock
{
public:
    MergeLock(Mutex & source, Mutex & destination) TSA_ACQUIRE_SHARED(source) TSA_ACQUIRE(destination)
        : source_lock(source, std::defer_lock)
        , destination_lock(destination, std::defer_lock)
    {
        if (std::addressof(source) == std::addressof(destination))
            throw std::invalid_argument("Cannot merge-lock the same mutex twice");

        if (std::less<Mutex *>{}(std::addressof(source), std::addressof(destination)))
        {
            source_lock.lock();
            destination_lock.lock();
        }
        else
        {
            destination_lock.lock();
            source_lock.lock();
        }
    }

    ~MergeLock() TSA_RELEASE() = default;

    MergeLock(const MergeLock &) = delete;
    MergeLock & operator=(const MergeLock &) = delete;
    MergeLock(MergeLock &&) = delete;
    MergeLock & operator=(MergeLock &&) = delete;

private:
    std::shared_lock<Mutex> source_lock;
    std::unique_lock<Mutex> destination_lock;
};

template <class Mutex>
MergeLock(Mutex &, Mutex &) -> MergeLock<Mutex>;

}
