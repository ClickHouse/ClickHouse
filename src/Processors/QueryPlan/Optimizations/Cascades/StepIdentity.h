#pragma once

#include <base/extended_types.h>

#include <memory>

namespace DB
{

class IQueryPlanStep;

/// Content-based fingerprint of a step, over one of the digests written by
/// Processors/QueryPlan/StepIdentity.h. The same struct serves the full and the logical digest;
/// which one a given instance holds is fixed by its owning member.
struct StepFingerprint
{
    UInt128 value;
    /// The step the fingerprint was computed from; a mismatch means the cache is stale.
    std::shared_ptr<const IQueryPlanStep> source_step;
};

/// Streams the digest through SipHash without materializing the bytes.
UInt128 computeStepFullFingerprint(const IQueryPlanStep & step);

/// Materializes both digests and compares them byte for byte. Confirms a fingerprint match; never
/// a substitute for it.
bool stepFullDigestsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs);

/// The same pair over the logical digest (`writeStepLogicalDigest`): do the two steps compute the
/// same relation? Strictly coarser than the full pair - two steps differing only in a physical knob
/// are logically equal - so it keys group membership and must never drop an alternative inside a
/// group. Caller guarantees `hasLogicalDigest()` for every step passed.
UInt128 computeStepLogicalFingerprint(const IQueryPlanStep & step);
bool stepLogicalDigestsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs);

}
