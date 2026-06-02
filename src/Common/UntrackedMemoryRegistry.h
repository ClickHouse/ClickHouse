#pragma once

#include <atomic>
#include <mutex>

#include <base/defines.h>
#include <base/types.h>
#include <boost/intrusive/list.hpp>


namespace DB
{

class ThreadLocalCounter
{
public:
    ThreadLocalCounter() = default;

    ThreadLocalCounter(const ThreadLocalCounter &) = delete;
    ThreadLocalCounter & operator=(const ThreadLocalCounter &) = delete;
    ThreadLocalCounter(ThreadLocalCounter &&) = delete;
    ThreadLocalCounter & operator=(ThreadLocalCounter &&) = delete;

    ALWAYS_INLINE inline Int64 load() const noexcept { return value.load(std::memory_order_relaxed); }
    ALWAYS_INLINE inline void store(Int64 v) noexcept { value.store(v, std::memory_order_relaxed); }

    ALWAYS_INLINE inline Int64 add(Int64 delta) noexcept
    {
        /// Not a real atomic RMW for performance considerations.
        /// All writes (add, store) come from a single thread, so this is fine.
        const Int64 new_value = value.load(std::memory_order_relaxed) + delta;
        value.store(new_value, std::memory_order_relaxed);
        return new_value;
    }

private:
    std::atomic<Int64> value{0};
};


class UntrackedMemoryCounter : public boost::intrusive::list_base_hook<>, public ThreadLocalCounter
{
public:
    UntrackedMemoryCounter();
    ~UntrackedMemoryCounter();
};


class UntrackedMemoryRegistry
{
public:
    static UntrackedMemoryRegistry & instance();

    void add(UntrackedMemoryCounter * counter);
    void remove(UntrackedMemoryCounter * counter);

    Int64 sum() const;

private:
    UntrackedMemoryRegistry() = default;

    mutable std::mutex mutex;
    boost::intrusive::list<UntrackedMemoryCounter> counters;
};

}
