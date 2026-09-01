#pragma once

#include <base/extended_types.h>

#include <memory>

namespace DB
{

class IQueryPlanStep;

/// Content-based cross-group fingerprint of a step, over the digest written by
/// `writeStepFullDigest` (Processors/QueryPlan/StepIdentity.h). Unlike
/// `GroupExpression::structurallyEqualTo`, which compares step name and description, this compares
/// the step's content.
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

}
