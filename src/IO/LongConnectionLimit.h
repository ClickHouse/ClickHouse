#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>

namespace DB
{

class LongConnectionLimit;

/// RAII lease on the global live-connection limit: holding one means this
/// reader may keep a source connection open. Move-only; a moved-from lease
/// holds nothing.
class LongConnectionSlot
{
public:
    LongConnectionSlot() = default;
    ~LongConnectionSlot();

    LongConnectionSlot(const LongConnectionSlot &) = delete;
    LongConnectionSlot & operator=(const LongConnectionSlot &) = delete;
    LongConnectionSlot(LongConnectionSlot && other) noexcept;
    /// Releases the currently-held unit before taking `other`'s, so
    /// reassigning never leaks capacity.
    LongConnectionSlot & operator=(LongConnectionSlot && other) noexcept;

    explicit operator bool() const { return held; }

private:
    friend class LongConnectionLimit;

    /// noexcept: does no allocation (a `shared_ptr` move and an atomic metric bump), so a
    /// slot can never throw after `tryAcquire` has already claimed the count -- no leak path.
    explicit LongConnectionSlot(std::shared_ptr<LongConnectionLimit> limit_) noexcept;

    void release();

    std::shared_ptr<LongConnectionLimit> limit;
    bool held = false;
};

/// Global limit on concurrently-open source connections, as a lock-free
/// counting semaphore: first-come acquirers take a unit and may keep a
/// connection open, the rest fall back to stateless reads. Created once in
/// `Context`.
class LongConnectionLimit
{
public:
    explicit LongConnectionLimit(size_t max_slots_);

    /// Returns a held lease, or an empty one at capacity. `self` must be the
    /// `shared_ptr` owning this instance (kept alive by the lease).
    LongConnectionSlot tryAcquire(std::shared_ptr<LongConnectionLimit> self);

    /// A lower limit is soft: existing leases beyond it are not revoked.
    void setCapacity(size_t new_max_slots);

    size_t getCapacity() const { return max_slots.load(std::memory_order_relaxed); }
    size_t getActiveCount() const { return count.load(std::memory_order_relaxed); }

private:
    friend class LongConnectionSlot;

    void release();

    std::atomic<size_t> max_slots;
    std::atomic<size_t> count{0};
};

}
