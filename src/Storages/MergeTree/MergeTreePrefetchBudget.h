#pragma once

#include <cstddef>
#include <memory>
#include <mutex>

namespace DB
{

class MergeTreePrefetchBudget;

/// RAII reservation of one prefetch buffer against a read step's budget: holding one means the
/// buffer it accounts for may stay allocated. Move-only; a moved-from slot holds nothing. Keep it
/// in the object that OWNS the buffer, declared so that the buffer is destroyed first.
class MergeTreePrefetchSlot
{
public:
    MergeTreePrefetchSlot() = default;
    ~MergeTreePrefetchSlot();

    MergeTreePrefetchSlot(const MergeTreePrefetchSlot &) = delete;
    MergeTreePrefetchSlot & operator=(const MergeTreePrefetchSlot &) = delete;
    MergeTreePrefetchSlot(MergeTreePrefetchSlot && other) noexcept;
    /// Releases the currently-held reservation before taking `other`'s, so reassigning never leaks.
    MergeTreePrefetchSlot & operator=(MergeTreePrefetchSlot && other) noexcept;

    explicit operator bool() const { return budget != nullptr; }

private:
    friend class MergeTreePrefetchBudget;

    /// noexcept: does no allocation (a `shared_ptr` move), so a slot can never throw after
    /// `tryReserve` has already claimed the count -- no leak path.
    MergeTreePrefetchSlot(std::shared_ptr<MergeTreePrefetchBudget> budget_, size_t bytes_) noexcept;

    void release();

    std::shared_ptr<MergeTreePrefetchBudget> budget;
    size_t bytes = 0;
};

/// Bounds the prefetch buffers alive at the same time within one read step: at most
/// `filesystem_prefetches_limit` of them (0 means no count bound), holding at most
/// `filesystem_prefetch_max_memory_usage` bytes. Shared by every reader of the step.
class MergeTreePrefetchBudget
{
public:
    MergeTreePrefetchBudget(size_t max_buffers_, size_t max_bytes_);

    /// Returns a held reservation, or an empty one when either bound is reached. `self` must be the
    /// `shared_ptr` owning this instance (kept alive by the reservation).
    MergeTreePrefetchSlot tryReserve(std::shared_ptr<MergeTreePrefetchBudget> self, size_t bytes);

    /// Whether any reservation could succeed. Advisory: used to skip opening a stream that would be
    /// refused anyway, so a stale answer costs at most one prefetch either way.
    bool hasCapacity();

private:
    friend class MergeTreePrefetchSlot;

    void release(size_t bytes);

    const size_t max_buffers;
    const size_t max_bytes;

    /// One mutex rather than two atomics: both bounds have to be tested and taken together.
    std::mutex mutex;
    size_t reserved_buffers = 0;
    size_t reserved_bytes = 0;
};

using MergeTreePrefetchBudgetPtr = std::shared_ptr<MergeTreePrefetchBudget>;

}
