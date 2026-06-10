#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>

namespace DB
{

class LiveConnectionLimit;

/// RAII lease on the global live-connection limit. Holding one means this reader
/// may keep a source connection open; it carries no per-connection metadata - just
/// the boolean "I hold a unit of the global limit". Releasing it (on destruction or
/// move-out) returns the unit to the counter. Move-only; a moved-from lease holds nothing.
class LiveConnectionSlot
{
public:
    LiveConnectionSlot() = default;
    ~LiveConnectionSlot();

    LiveConnectionSlot(const LiveConnectionSlot &) = delete;
    LiveConnectionSlot & operator=(const LiveConnectionSlot &) = delete;
    LiveConnectionSlot(LiveConnectionSlot && other) noexcept;
    /// Releases the currently-held unit (if any) BEFORE taking ownership of `other`'s,
    /// so reassigning never leaks a unit of capacity.
    LiveConnectionSlot & operator=(LiveConnectionSlot && other) noexcept;

    /// True iff this lease holds a unit of the global limit.
    explicit operator bool() const { return held; }

private:
    friend class LiveConnectionLimit;
    explicit LiveConnectionSlot(std::shared_ptr<LiveConnectionLimit> limit_) : limit(std::move(limit_)), held(true) {}

    void release();

    std::shared_ptr<LiveConnectionLimit> limit;
    bool held = false;
};

/// Global limit on the number of concurrently-open source connections, as a
/// lock-free counting semaphore: first-come acquirers take a unit and may keep a
/// connection open, the rest fall back to stateless reads. No per-connection
/// registry - just the count. Thread-safe via atomics. Created once in Context.
class LiveConnectionLimit
{
public:
    explicit LiveConnectionLimit(size_t max_slots_) : max_slots(max_slots_) {}

    /// Try to take a unit. Returns a held lease, or an empty one at capacity.
    /// `self` must be the `shared_ptr` owning this instance (kept alive by the lease).
    LiveConnectionSlot tryAcquire(std::shared_ptr<LiveConnectionLimit> self);

    /// Update capacity at runtime (called from config reload in Server.cpp). A lower
    /// limit is soft: existing leases beyond it are not forcibly released.
    void setCapacity(size_t new_max_slots) { max_slots.store(new_max_slots, std::memory_order_relaxed); }
    size_t getCapacity() const { return max_slots.load(std::memory_order_relaxed); }

    /// Live units currently held (observability).
    size_t getActiveCount() const { return count.load(std::memory_order_relaxed); }

private:
    friend class LiveConnectionSlot;
    void release();

    std::atomic<size_t> max_slots;
    std::atomic<size_t> count{0};
};

}
