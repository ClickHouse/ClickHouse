#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>

namespace DB
{

class LiveConnectionLimit;

/// RAII lease on the global live-connection limit: holding one means this
/// reader may keep a source connection open. Move-only; a moved-from lease
/// holds nothing.
class LiveConnectionSlot
{
public:
    LiveConnectionSlot() = default;
    ~LiveConnectionSlot();

    LiveConnectionSlot(const LiveConnectionSlot &) = delete;
    LiveConnectionSlot & operator=(const LiveConnectionSlot &) = delete;
    LiveConnectionSlot(LiveConnectionSlot && other) noexcept;
    /// Releases the currently-held unit before taking `other`'s, so
    /// reassigning never leaks capacity.
    LiveConnectionSlot & operator=(LiveConnectionSlot && other) noexcept;

    explicit operator bool() const { return held; }

private:
    friend class LiveConnectionLimit;

    explicit LiveConnectionSlot(std::shared_ptr<LiveConnectionLimit> limit_);

    void release();

    std::shared_ptr<LiveConnectionLimit> limit;
    bool held = false;
};

/// Global limit on concurrently-open source connections, as a lock-free
/// counting semaphore: first-come acquirers take a unit and may keep a
/// connection open, the rest fall back to stateless reads. Created once in
/// `Context`.
class LiveConnectionLimit
{
public:
    explicit LiveConnectionLimit(size_t max_slots_);

    /// Returns a held lease, or an empty one at capacity. `self` must be the
    /// `shared_ptr` owning this instance (kept alive by the lease).
    LiveConnectionSlot tryAcquire(std::shared_ptr<LiveConnectionLimit> self);

    /// A lower limit is soft: existing leases beyond it are not revoked.
    void setCapacity(size_t new_max_slots);

    size_t getCapacity() const { return max_slots.load(std::memory_order_relaxed); }
    size_t getActiveCount() const { return count.load(std::memory_order_relaxed); }

private:
    friend class LiveConnectionSlot;

    void release();

    std::atomic<size_t> max_slots;
    std::atomic<size_t> count{0};
};

}
