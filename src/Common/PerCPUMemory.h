#pragma once

#include <Common/PerCPUMemoryThreadState.h>
#include <base/types.h>

#include <atomic>
#include <limits>

/// Supported only on Linux (needs sched_getcpu)
#if defined(OS_LINUX)
#include <unistd.h>

#include <Common/CacheLine.h>

#include <base/defines.h>

#include <sched.h>

#include <cstdlib>
#include <memory>

namespace DB
{

/// Per-CPU bound on the deferred (un-flushed) untracked memory the global MemoryTracker cannot yet
/// see, capping it in both directions: deferred allocations (-> OOM) and deferred frees (-> the
/// tracker over-reports usage).
///
/// Each CPU has two one-sided counters: `allocated` (>= 0) and `freed` (<= 0). A thread's
/// untracked_memory contributes to exactly one of them, by sign, batched so the shared counter is
/// touched only after drifting by `buffer`. When the occupied side leaves [-capacity, capacity]
/// the thread must flush, bounding the deferred bytes of every thread on the CPU, parked included.
///
/// Two separate counters, not one signed net, because a signed net would let a deferred free cancel
/// a deferred allocation: the allocation passes admission, then the free flushes on its own (its
/// thread exits or hits its per-thread cap) and the allocation is left untracked. Each thread can
/// leave up to max_untracked_memory untracked that way, i.e. nthreads * max_untracked_memory total —
/// the ~40 GiB overcommit this exists to remove.
///
/// Each counter is the sum of all threads' contributions to it (PerCPUMemoryThreadState::contributed):
/// every add is matched by a later subtract on flush, or a move to the other side/CPU on a sign
/// change or migration, so it equals the real deferred bytes on that side and the cap stays accurate.
///
/// The capacity check races with concurrent updates, so a counter may briefly exceed capacity —
/// harmless, since it only gates when bytes flush into the MemoryTracker, which enforces the real limit.
///
/// Bound per CPU: 0 <= allocated <= capacity, -capacity <= freed <= 0 (+ in-flight), so deferred
/// memory is ~ ncpu * max_per_cpu_untracked_memory + nthreads * per_cpu_untracked_memory_thread_buffer.
class PerCPUMemory
{
public:
    static constexpr Int64 DEFAULT_BUDGET = 8 * 1024 * 1024;
    static constexpr Int64 DEFAULT_THREAD_BUFFER = 32 * 1024;
    /// max_per_cpu_untracked_memory == 0 maps here: no side ever leaves the range, disabling the
    /// per-CPU bound (only the per-thread cap applies).
    static constexpr Int64 UNLIMITED_BUDGET = std::numeric_limits<Int64>::max() / 2;

    static int numberOfCPUs()
    {
        Int64 n = ::sysconf(_SC_NPROCESSORS_CONF);
        return n > 0 ? static_cast<int>(n) : 0;
    }

    PerCPUMemory(int cpu_count_, Int64 capacity_, Int64 buffer_)
        : cpu_count(cpu_count_)
        , slots(std::make_unique<Slot[]>(cpu_count_ > 0 ? static_cast<size_t>(cpu_count_) : 0))
        , capacity(capacity_ > 0 ? capacity_ : UNLIMITED_BUDGET)
        , buffer(buffer_ < 0 ? 0 : buffer_)
    {
    }

    /// Caller protocol (see CurrentMemoryTracker), per flush decision:
    ///   sync() == true  -> within budget, keep deferring, do nothing
    ///   sync() == false -> over budget (or CPU unavailable), release() this thread's contribution, then flush
    ///   flush threw         -> rollback() to the snapshot taken before sync()
    /// Sets this thread's contribution on its CPU's allocated/freed side to untracked_memory (the new
    /// absolute value, not a delta) and returns whether that side is still within budget. On false the
    /// contribution stays on the counter for the caller's release() to back out.
    [[nodiscard]] ALWAYS_INLINE bool sync(Int64 untracked_memory, PerCPUMemoryThreadState & state)
    {
        const Int64 buffer_now = buffer.load(std::memory_order_relaxed);
        if (likely(std::abs(untracked_memory - state.contributed) < buffer_now))
            return true;

        const int cpu = sched_getcpu();
        if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(cpu_count)))
            return false;

        Int64 occupied = 0;
        /// Fast path only for a genuine same-side delta. untracked_memory != 0 diverts a now-balanced
        /// thread to the slow branch: it holds nothing and must always be admitted, but the delta path
        /// would test the shared counter and could force a flush under other threads' pressure.
        const bool same_side = cpu == state.contributed_on_cpu
            && untracked_memory != 0
            && (untracked_memory > 0) == (state.contributed > 0);
        if (likely(same_side))
        {
            const Int64 delta = untracked_memory - state.contributed;
            std::atomic<Int64> & side = untracked_memory > 0 ? slots[cpu].allocated : slots[cpu].freed;
            occupied = side.fetch_add(delta, std::memory_order_relaxed) + delta;
            state.contributed = untracked_memory;
        }
        else
        {
            /// Migrated or changed sign: add to the new CPU/side before removing from the old, so
            /// the bytes are never charged to no CPU (a brief double-charge only tightens the bound).
            if (untracked_memory > 0)
                occupied = slots[cpu].allocated.fetch_add(untracked_memory, std::memory_order_relaxed) + untracked_memory;
            else if (untracked_memory < 0)
                occupied = slots[cpu].freed.fetch_add(untracked_memory, std::memory_order_relaxed) + untracked_memory;
            release(state);
            state.contributed = untracked_memory;
            state.contributed_on_cpu = cpu;
        }

        /// allocated >= 0 and freed <= 0, so one symmetric check covers both sides.
        const Int64 capacity_now = capacity.load(std::memory_order_relaxed);
        return -capacity_now <= occupied && occupied <= capacity_now;
    }

    /// Back this thread's contribution out of its CPU side (picked by sign). Idempotent.
    ALWAYS_INLINE void release(PerCPUMemoryThreadState & state)
    {
        if (state.contributed_on_cpu >= 0)
        {
            if (state.contributed > 0)
                slots[state.contributed_on_cpu].allocated.fetch_sub(state.contributed, std::memory_order_relaxed);
            else if (state.contributed < 0)
                slots[state.contributed_on_cpu].freed.fetch_sub(state.contributed, std::memory_order_relaxed);
        }
        state.contributed = 0;
    }

    /// Re-add the pre-sync() snapshot after a failed flush; unconditional (unlike the gated
    /// sync) because the bytes are still live. May transiently exceed capacity; the next sync
    /// corrects it.
    ALWAYS_INLINE void rollback(PerCPUMemoryThreadState & state, const PerCPUMemoryThreadState & saved)
    {
        release(state);
        if (saved.contributed_on_cpu >= 0)
        {
            if (saved.contributed > 0)
                slots[saved.contributed_on_cpu].allocated.fetch_add(saved.contributed, std::memory_order_relaxed);
            else if (saved.contributed < 0)
                slots[saved.contributed_on_cpu].freed.fetch_add(saved.contributed, std::memory_order_relaxed);
        }
        state = saved;
    }

    /// Config load/reload: change only the threshold / batch size, leaving the running counters.
    /// 0 buffer is valid: no per-thread slack, so every change hits the shared counter.
    void setThreadBuffer(Int64 bytes)
    {
        buffer.store(bytes < 0 ? 0 : bytes, std::memory_order_relaxed);
    }

    void setBudgetCapacity(Int64 bytes)
    {
        capacity.store(bytes > 0 ? bytes : UNLIMITED_BUDGET, std::memory_order_relaxed);
    }

    /// For system.server_settings; maps the UNLIMITED_BUDGET sentinel back to 0 (bound disabled).
    Int64 budgetCapacity() const
    {
        const Int64 c = capacity.load(std::memory_order_relaxed);
        return c == UNLIMITED_BUDGET ? 0 : c;
    }
    Int64 threadBuffer() const { return buffer.load(std::memory_order_relaxed); }

    /// Deferred bytes accounted on `cpu` (introspection and tests).
    Int64 allocatedOnCPU(int cpu) const { return slots[cpu].allocated.load(std::memory_order_relaxed); }
    Int64 freedOnCPU(int cpu) const { return slots[cpu].freed.load(std::memory_order_relaxed); }
    Int64 netOnCPU(int cpu) const { return allocatedOnCPU(cpu) + freedOnCPU(cpu); }

private:
    struct alignas(CH_CACHE_LINE_SIZE) Slot
    {
        /// Per-CPU sums of deferred allocations (>= 0) and frees (<= 0, kept signed so the budget
        /// check stays symmetric).
        std::atomic<Int64> allocated;
        std::atomic<Int64> freed;
    };

    const int cpu_count;
    const std::unique_ptr<Slot[]> slots;
    std::atomic<Int64> capacity;
    std::atomic<Int64> buffer;
};

/// Process-wide instance used by CurrentMemoryTracker. Constructed before main(); allocations
/// before its initialiser runs see cpu_count == 0, so sync() returns false without indexing the
/// (empty) array — a normal per-thread flush.
inline PerCPUMemory per_cpu_memory{PerCPUMemory::numberOfCPUs(), PerCPUMemory::DEFAULT_BUDGET, PerCPUMemory::DEFAULT_THREAD_BUFFER};

}

#else

namespace DB
{

/// No per-CPU accounting without sched_getcpu; behaviour is per-thread only. The settings are
/// still stored so system.server_settings reports what was configured.
class PerCPUMemory
{
public:
    static constexpr Int64 DEFAULT_BUDGET = 8 * 1024 * 1024;
    static constexpr Int64 DEFAULT_THREAD_BUFFER = 32 * 1024;
    static constexpr Int64 UNLIMITED_BUDGET = std::numeric_limits<Int64>::max() / 2;

    [[nodiscard]] bool sync(Int64 /*untracked_memory*/, PerCPUMemoryThreadState &) { return true; }
    void release(PerCPUMemoryThreadState &) {}
    void rollback(PerCPUMemoryThreadState &, const PerCPUMemoryThreadState &) {}
    void setThreadBuffer(Int64 bytes) { buffer.store(bytes < 0 ? 0 : bytes, std::memory_order_relaxed); }
    void setBudgetCapacity(Int64 bytes) { capacity.store(bytes > 0 ? bytes : UNLIMITED_BUDGET, std::memory_order_relaxed); }
    Int64 budgetCapacity() const
    {
        const Int64 c = capacity.load(std::memory_order_relaxed);
        return c == UNLIMITED_BUDGET ? 0 : c;
    }
    Int64 threadBuffer() const { return buffer.load(std::memory_order_relaxed); }

private:
    std::atomic<Int64> capacity{DEFAULT_BUDGET};
    std::atomic<Int64> buffer{DEFAULT_THREAD_BUFFER};
};

inline PerCPUMemory per_cpu_memory;

}

#endif
