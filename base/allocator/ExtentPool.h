#pragma once

/// A cache of `Extent` structures allocated via `Base::allocExtent` (as opposed to the extents they describe).
/// The contents of returned `Extent` objects are garbage and cannot be relied upon (except `struct_serial_number`).
/// jemalloc: `edata_cache.h`, `src/edata_cache.c`. The HPA-only `edata_cache_fast_t` is dropped.

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/Mutex.h>

#include <atomic>

namespace jemalloc
{

class Base;
class ThreadState;

/// jemalloc: edata_cache_t
class ExtentPool
{
public:
    constexpr ExtentPool() = default;

    ExtentPool(const ExtentPool &) = delete;
    ExtentPool & operator=(const ExtentPool &) = delete;

    /// Returns true on error.
    /// jemalloc: edata_cache_init
    bool init(Base * base_);

    /// The structure with the lowest (esn, address), or a new one from the base; nullptr on OOM.
    /// jemalloc: edata_cache_get
    Extent * get(ThreadState * thread_state);

    /// jemalloc: edata_cache_put
    void put(ThreadState * thread_state, Extent * extent);

    /// The number of cached structures (`stats.arenas.<i>.extent_avail`).
    size_t count() const { return count_.load(std::memory_order_relaxed); }

    /// jemalloc: edata_cache_prefork
    void prefork(ThreadState * thread_state) { mutex.prefork(thread_state); }
    /// jemalloc: edata_cache_postfork_parent
    void postforkParent(ThreadState * thread_state) { mutex.postforkParent(thread_state); }
    /// jemalloc: edata_cache_postfork_child
    void postforkChild(ThreadState * thread_state) { mutex.postforkChild(thread_state); }

    Mutex & getMutex() { return mutex; }

private:
    ExtentAvailableHeap available;
    std::atomic<size_t> count_{0};
    Mutex mutex;
    Base * base = nullptr;
};

}
