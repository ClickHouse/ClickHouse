#pragma once

#include <atomic>
#include <mutex>
#include <unordered_set>

#include <base/types.h>


namespace DB
{

class UntrackedMemoryCounter
{
public:
    UntrackedMemoryCounter();
    ~UntrackedMemoryCounter();

    UntrackedMemoryCounter(const UntrackedMemoryCounter &) = delete;
    UntrackedMemoryCounter & operator=(const UntrackedMemoryCounter &) = delete;
    UntrackedMemoryCounter(UntrackedMemoryCounter &&) = delete;
    UntrackedMemoryCounter & operator=(UntrackedMemoryCounter &&) = delete;

    Int64 load() const noexcept { return value.load(std::memory_order_relaxed); }
    void store(Int64 v) noexcept { value.store(v, std::memory_order_relaxed); }

    void add(Int64 v) noexcept
    {
        value.store(value.load(std::memory_order_relaxed) + v, std::memory_order_relaxed);
    }

private:
    std::atomic<Int64> value{0};
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
    std::unordered_set<UntrackedMemoryCounter *> counters;
};

}
