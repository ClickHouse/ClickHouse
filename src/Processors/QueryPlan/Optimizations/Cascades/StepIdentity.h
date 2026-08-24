#pragma once

#include <base/extended_types.h>

#include <atomic>
#include <memory>

namespace DB
{

class IQueryPlanStep;

/// Content-based cross-group identity of a step, over the encoding written by
/// `writeCascadesIdentityEncoding` (Processors/QueryPlan/StepIdentity.h). Unlike
/// `GroupExpression::structurallyEqualTo`, which compares step name and description, this compares
/// the step's content.
struct StepIdentity
{
    UInt128 hash;
    /// The step the hash was computed from; a mismatch means the cache is stale.
    std::shared_ptr<const IQueryPlanStep> step;
};

/// Streams the encoding through SipHash without materializing the bytes.
UInt128 computeCascadesIdentityHash(const IQueryPlanStep & step);

/// Materializes both encodings and compares them byte for byte. Confirms a hash match; never a
/// substitute for it.
bool cascadesIdentityEncodingsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs);

/// Cost of the identity machinery, for the performance gate on memo-wide deduplication.
struct CascadesIdentityMetrics
{
    /// Completed encoding passes, whether they produced a hash or bytes.
    static std::atomic<UInt64> encoded_steps;
    /// Total bytes produced by those passes.
    static std::atomic<UInt64> encoded_bytes;
    /// Encodings recomputed only to confirm a hash match byte-exactly.
    static std::atomic<UInt64> exact_reencodes;

    static void reset();
};

}
