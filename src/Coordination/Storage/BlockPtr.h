#pragma once

#include <base/defines.h>

#include <atomic>
#include <thread>

namespace Coordination::Storage
{

/// Custom shared_ptr implementation for BlockData, to save 8 bytes per node on BlockWeakPtr.
/// We don't co-allocate the control block with BlockData because we don't want BlockWeakPtr to
/// prevent deallocating BlockData's memory.
struct BlockPtr;
struct BlockWeakPtr;
struct BlockWeakPtrWithSpinlock;

/// ===== Implementations below =====

struct BlockData;

struct BlockPtrControlBlock
{
    BlockData * ptr = nullptr;

    /// Number of BlockPtr-s.
    std::atomic<uint32_t> strong{1};
    /// Number of BlockWeakPtr-s, plus 1 while strong > 0 (i.e. while `ptr` is alive). The control
    /// block is freed when this reaches 0. (Same scheme as libstdc++/libc++ shared_ptr.)
    std::atomic<uint32_t> weak{1};
};

void destroyBlockData(BlockData * ptr) noexcept;

/// Drop one weak ref; free the control block if it was the last.
inline void releaseBlockWeakRef(BlockPtrControlBlock * control) noexcept
{
    if (control->weak.fetch_sub(1, std::memory_order_acq_rel) == 1)
        delete control;
}

struct BlockPtr
{
    BlockData * ptr = nullptr; // same as control->ptr, stored here for faster access
    BlockPtrControlBlock * control = nullptr;

    BlockPtr() = default;
    BlockPtr(std::nullptr_t) noexcept {} /// NOLINT(google-explicit-constructor)

    /// Adopts an existing strong ref (doesn't increment). Used by BlockData::create and weak lock.
    BlockPtr(BlockData * ptr_, BlockPtrControlBlock * control_) noexcept : ptr(ptr_), control(control_) {}

    BlockPtr(const BlockPtr & rhs) noexcept : ptr(rhs.ptr), control(rhs.control)
    {
        if (control)
            control->strong.fetch_add(1, std::memory_order_relaxed);
    }
    BlockPtr(BlockPtr && rhs) noexcept : ptr(rhs.ptr), control(rhs.control)
    {
        rhs.ptr = nullptr;
        rhs.control = nullptr;
    }
    BlockPtr & operator=(BlockPtr rhs) noexcept // by value: handles both copy and move
    {
        swap(rhs);
        return *this;
    }
    ~BlockPtr() { reset(); }

    void swap(BlockPtr & rhs) noexcept
    {
        std::swap(ptr, rhs.ptr);
        std::swap(control, rhs.control);
    }

    void reset() noexcept
    {
        if (control)
        {
            if (control->strong.fetch_sub(1, std::memory_order_acq_rel) == 1)
            {
                destroyBlockData(ptr);
                control->ptr = nullptr;
                releaseBlockWeakRef(control); // drop the strong refs' collective weak ref
            }
            ptr = nullptr;
            control = nullptr;
        }
    }

    BlockData * get() const { return ptr; }
    BlockData * operator->() const { return ptr; }
    BlockData & operator*() const { return *ptr; }
    explicit operator bool() const { return ptr != nullptr; }

    bool operator==(const BlockPtr & rhs) const { return control == rhs.control; }
    bool operator==(std::nullptr_t) const { return control == nullptr; }
};

/// Increment the strong count if the object is still alive, and return a BlockPtr to it (else null).
inline BlockPtr lockBlockControl(BlockPtrControlBlock * control) noexcept
{
    if (!control)
        return {};
    uint32_t s = control->strong.load(std::memory_order_relaxed);
    while (s != 0)
    {
        if (control->strong.compare_exchange_weak(s, s + 1, std::memory_order_acquire, std::memory_order_relaxed))
            return BlockPtr(control->ptr, control); // adopt the strong ref we just took
    }
    return {};
}

/// This is small (8 bytes) because we store it in NodeRefCache::Entry, where we don't want to spend
/// another 8 bytes on a BlockData * like std::weak_ptr would.
struct BlockWeakPtr
{
    BlockPtrControlBlock * control = nullptr;

    BlockWeakPtr() = default;
    BlockWeakPtr(std::nullptr_t) noexcept {} /// NOLINT(google-explicit-constructor)

    BlockWeakPtr(const BlockPtr & p) noexcept : control(p.control) /// NOLINT(google-explicit-constructor)
    {
        if (control)
            control->weak.fetch_add(1, std::memory_order_relaxed);
    }
    BlockWeakPtr(const BlockWeakPtr & rhs) noexcept : control(rhs.control)
    {
        if (control)
            control->weak.fetch_add(1, std::memory_order_relaxed);
    }
    BlockWeakPtr(BlockWeakPtr && rhs) noexcept : control(rhs.control) { rhs.control = nullptr; }
    BlockWeakPtr & operator=(BlockWeakPtr rhs) noexcept // by value: handles both copy and move
    {
        std::swap(control, rhs.control);
        return *this;
    }
    ~BlockWeakPtr() { reset(); }

    void reset() noexcept
    {
        if (control)
        {
            releaseBlockWeakRef(control);
            control = nullptr;
        }
    }

    /// weak -> strong.
    BlockPtr lock() const noexcept { return lockBlockControl(control); }
};

/// A BlockWeakPtr and a spinlock packed into 8 bytes.
struct BlockWeakPtrWithSpinlock
{
    /// Low bit is the spinlock (relying in 8-byte alignment).
    std::atomic<uintptr_t> packed{0};

    static constexpr uintptr_t LOCK_BIT = 1;
    static constexpr uintptr_t PTR_MASK = ~LOCK_BIT;

    BlockWeakPtrWithSpinlock() = default;
    BlockWeakPtrWithSpinlock(const BlockWeakPtrWithSpinlock &) = delete;
    BlockWeakPtrWithSpinlock & operator=(const BlockWeakPtrWithSpinlock &) = delete;

    /// Moving is not atomic.
    BlockWeakPtrWithSpinlock(BlockWeakPtrWithSpinlock && rhs) noexcept
    {
        uintptr_t v = rhs.packed.load(std::memory_order_relaxed);
        chassert(!(v & LOCK_BIT));
        packed.store(v, std::memory_order_relaxed);
        rhs.packed.store(0, std::memory_order_relaxed);
    }
    BlockWeakPtrWithSpinlock & operator=(BlockWeakPtrWithSpinlock && rhs) noexcept
    {
        if (this != &rhs)
        {
            uintptr_t v = packed.load(std::memory_order_relaxed);
            chassert(!(v & LOCK_BIT));
            if (auto * c = reinterpret_cast<BlockPtrControlBlock *>(v & PTR_MASK))
                releaseBlockWeakRef(c);

            uintptr_t rv = rhs.packed.load(std::memory_order_relaxed);
            chassert(!(rv & LOCK_BIT));
            packed.store(rv, std::memory_order_relaxed);
            rhs.packed.store(0, std::memory_order_relaxed);
        }
        return *this;
    }
    ~BlockWeakPtrWithSpinlock()
    {
        uintptr_t v = packed.load(std::memory_order_relaxed);
        chassert(!(v & LOCK_BIT));
        if (auto * c = reinterpret_cast<BlockPtrControlBlock *>(v & PTR_MASK))
            releaseBlockWeakRef(c);
    }

    /// Spinlock.
    void lock()
    {
        for (;;)
        {
            uintptr_t v = packed.load(std::memory_order_relaxed) & PTR_MASK;
            if (packed.compare_exchange_weak(v, v | LOCK_BIT, std::memory_order_acquire, std::memory_order_relaxed))
                return;
            std::this_thread::yield();
        }
    }
    void unlock()
    {
        uintptr_t v = packed.fetch_and(PTR_MASK, std::memory_order_release);
        chassert(v & LOCK_BIT);
    }

    /// weak -> strong. Caller must hold the spinlock.
    BlockPtr get() const
    {
        auto * c = reinterpret_cast<BlockPtrControlBlock *>(packed.load(std::memory_order_relaxed) & PTR_MASK);
        return lockBlockControl(c);
    }

    /// Replace the stored weak ref (adopting `weak`'s ref). Caller must hold the spinlock.
    void set(BlockWeakPtr weak)
    {
        uintptr_t v = packed.load(std::memory_order_relaxed);
        chassert(v & LOCK_BIT);
        auto * old = reinterpret_cast<BlockPtrControlBlock *>(v & PTR_MASK);
        packed.store(reinterpret_cast<uintptr_t>(weak.control) | LOCK_BIT, std::memory_order_relaxed);
        weak.control = nullptr; // adopt: prevent ~BlockWeakPtr from releasing the ref
        if (old)
            releaseBlockWeakRef(old);
    }

    BlockPtr lockAndGet()
    {
        for (;;)
        {
            uintptr_t v = packed.load(std::memory_order_relaxed) & PTR_MASK;
            if (packed.compare_exchange_weak(v, v | LOCK_BIT, std::memory_order_acquire, std::memory_order_relaxed))
                return lockBlockControl(reinterpret_cast<BlockPtrControlBlock *>(v & PTR_MASK));
            std::this_thread::yield();
        }
    }

    /// Convenience wrappers that take the spinlock themselves, for callers that don't need to hold
    /// it across other fields.
    BlockPtr load()
    {
        BlockPtr p = lockAndGet();
        unlock();
        return p;
    }
    void store(const BlockPtr & p)
    {
        lock();
        set(p);
        unlock();
    }
};

}
