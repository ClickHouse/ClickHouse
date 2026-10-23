#pragma once

#include <base/types.h>

#include <limits>

namespace DB
{

/// Identity of a plan step, unique within a query.
///
/// A query numbers its steps from 0 with a counter in its thread group, which keeps the numbers
/// small and stable for `EXPLAIN`. A step built outside of any query has no counter to draw from
/// and gets `NONE`, so that it stands out rather than taking a number another step may have.
class PlanStepIndex
{
public:
    /// Not the index of any step: marks what belongs to no step, and a step built outside of a query.
    static constexpr UInt64 NONE = std::numeric_limits<UInt64>::max();

    PlanStepIndex();

    /// A copy of a step is a different step -- `clone` is `make_unique<Step>(*this)` and both
    /// copies can end up in the same plan -- so copying takes a fresh index instead of
    /// duplicating the source's.
    PlanStepIndex(const PlanStepIndex &) : PlanStepIndex() {}
    PlanStepIndex & operator=(const PlanStepIndex &) { return *this; } // NOLINT(cert-oop54-cpp) - keeping our own index is self-assignment safe

    /// Moving is the same step changing hands, so the index travels with it.
    PlanStepIndex(PlanStepIndex &&) noexcept = default;
    PlanStepIndex & operator=(PlanStepIndex &&) noexcept = default;

    UInt64 get() const { return value; }

private:
    UInt64 value = 0;
};

}
