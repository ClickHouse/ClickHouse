#pragma once

#include <Common/ProcessorMemoryStats.h>

#include <boost/core/noncopyable.hpp>

#include <atomic>
#include <cstddef>

namespace DB
{

struct MemoryReservation;
class TemporaryDataOnDiskScope;

/// Memory spilling interface of a processor.
/// Aggregation, join, sorting, and `DISTINCT` processors can be spillable.
///
/// Kept separate from IProcessor so that the spilling API can evolve without
/// recompiling every translation unit that uses processors.
///
/// If processes shares spilling/memory state, it should share ISpillable object.
class ISpillable : private boost::noncopyable
{
public:
    virtual ~ISpillable() = default;

    virtual ProcessorMemoryStats getMemoryStats() const = 0;

    /// Request to spill @at_least_bytes and return how many had been spilled
    /// May return less than requested; the scheduler rechecks memory before requesting more.
    virtual size_t spill(size_t at_least_bytes) = 0;

    /// Register each owning processor before execution starts.
    void registerProcessor()
    {
        active_processors.fetch_add(1, std::memory_order_relaxed);
    }

    /// Called once per processor on `Finished`; the last owner removes scheduler accounting.
    void unregisterProcessor();

    /// The scope retains cumulative spill statistics after its temporary files are deleted.
    /// Multiple processors can share a scope; count it once when reporting a plan step.
    virtual const TemporaryDataOnDiskScope * getSpillScope() const { return nullptr; }

private:
    friend struct MemoryReservation;

    /// Registered processors that have not reached `Finished`. Keep shared accounting until the last one finishes.
    /// Registration precedes execution; processors sharing this object may finish concurrently.
    std::atomic<size_t> active_processors{0};

    /// Accounting belongs to one query's `MemoryReservation`, whose mutex protects these fields.
    /// A shared spillable object must not be reused across reservations.
    struct SpillAccounting
    {
        /// Bound when reporting under the reservation mutex. Read by the last owner after the
        /// acquire decrement of `active_processors`, once all owners have stopped reporting.
        MemoryReservation * reservation = nullptr;
        Int64 reclaimable = 0;
        bool in_progress = false;
    };

    mutable SpillAccounting spill_accounting;
};

}
