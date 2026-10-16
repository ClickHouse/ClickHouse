#pragma once

#include <Common/ProcessorMemoryStats.h>

#include <boost/core/noncopyable.hpp>

#include <cstddef>
#include <mutex>

namespace DB
{

struct MemoryReservation;
class IProcessor;
class TemporaryDataOnDiskScope;

/// Memory spilling interface of a processor.
/// Aggregation, join, sorting, `DISTINCT`, and `IN` set processors can be spillable.
///
/// Processors or shared state own an implementation, exposed through `getSpillables`.
/// Processors may share the same spilling state. The executor excludes a spill from its selected
/// owner's `prepare` and `work`; shared implementations synchronize against the other owners themselves.
class ISpillable : private boost::noncopyable
{
public:
    virtual ~ISpillable() = default;

    virtual ProcessorMemoryStats getMemoryStats() const = 0;

    /// Request to spill @at_least_bytes and return how many had been spilled
    /// May return less than requested; the scheduler rechecks memory before requesting more.
    virtual size_t spill(size_t at_least_bytes) = 0;

    /// The scope retains cumulative spill statistics after its temporary files are deleted.
    /// Multiple processors and plan steps can share a scope; count it once per plan.
    virtual const TemporaryDataOnDiskScope * getSpillScope() const { return nullptr; }

private:
    friend struct MemoryReservation;

    /// Accounting belongs to one query's `MemoryReservation`, whose mutex protects these fields.
    /// A shared spillable object must not be reused across reservations.
    struct SpillAccounting
    {
        MemoryReservation * reservation = nullptr;
        size_t owners = 0;
        const IProcessor * scheduled_owner = nullptr;
        Int64 reclaimable = 0;
        bool in_progress = false;
    };

    mutable SpillAccounting spill_accounting;
    /// Order snapshots from different owners before publishing them to the reservation.
    mutable std::mutex report_mutex;
};

}
