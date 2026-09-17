#include <Databases/UDT/ResourceLimitAdapters.h>

#include <algorithm>

namespace DB::UDT
{

PhysicalizationTokenStoreLimits makePhysicalizationTokenStoreLimits(const EffectiveResourceLimits & limits)
{
    PhysicalizationTokenStoreLimits result;
    result.maximum_outstanding_tokens
        = std::min(result.maximum_outstanding_tokens, limits.get(ResourceLimit::PhysicalizationTokensPerDatabase));
    result.maximum_aggregate_record_bytes
        = std::min(result.maximum_aggregate_record_bytes, limits.get(ResourceLimit::PhysicalizationTokenBytesPerDatabase));
    result.maximum_tokens_per_principal
        = std::min(result.maximum_tokens_per_principal, limits.get(ResourceLimit::PhysicalizationTokensPerPrincipalDatabase));
    result.maximum_record_bytes_per_principal
        = std::min(result.maximum_record_bytes_per_principal, limits.get(ResourceLimit::PhysicalizationTokenBytesPerPrincipalDatabase));
    result.maximum_record_bytes = std::min(result.maximum_record_bytes, limits.get(ResourceLimit::PhysicalizationTokenBytesPerRecord));
    result.maximum_ttl_microseconds
        = std::min(result.maximum_ttl_microseconds, limits.get(ResourceLimit::PhysicalizationTokenTTLMicroseconds));
    return result;
}

AuthorityIntegrityVerifierLimits makeAuthorityIntegrityVerifierLimits(const EffectiveResourceLimits &)
{
    AuthorityIntegrityVerifierLimits result;
    /// Every field here is part of the immutable execution domain for one
    /// already-published target. Mutable verification quotas are checked while
    /// admitting new rooted artifacts and policies; narrowing this verifier
    /// after publication could make an existing target permanently
    /// unverifiable. OVER_QUOTA therefore blocks growth, not integrity work.
    return result;
}

AuthorityVerificationScheduleLimits makeAuthorityVerificationScheduleLimits(const EffectiveResourceLimits & limits)
{
    AuthorityVerificationScheduleLimits result;
    result.maximum_snapshot_targets = std::min(result.maximum_snapshot_targets, limits.get(ResourceLimit::VerificationTargetsPerDatabase));
    result.maximum_buckets = std::min(result.maximum_buckets, limits.get(ResourceLimit::VerificationBucketsPerDatabase));
    result.maximum_targets_per_batch = std::min(result.maximum_targets_per_batch, limits.get(ResourceLimit::VerificationTargetsPerBatch));
    result.maximum_canonical_bytes_per_batch
        = std::min(result.maximum_canonical_bytes_per_batch, limits.get(ResourceLimit::VerificationCanonicalBytesPerBatch));
    result.maximum_verification_work_units_per_batch
        = std::min(result.maximum_verification_work_units_per_batch, limits.get(ResourceLimit::VerificationWorkUnitsPerBatch));
    result.maximum_transient_bytes_per_batch
        = std::min(result.maximum_transient_bytes_per_batch, limits.get(ResourceLimit::VerificationTransientBytesPerBatch));
    result.maximum_io_bytes_per_batch = std::min(result.maximum_io_bytes_per_batch, limits.get(ResourceLimit::VerificationIOBytesPerBatch));
    result.maximum_rooted_target_canonical_bytes = result.maximum_canonical_bytes_per_batch;
    result.maximum_rooted_target_verification_work_units = result.maximum_verification_work_units_per_batch;
    result.maximum_rooted_target_transient_bytes = result.maximum_transient_bytes_per_batch;
    result.maximum_rooted_target_io_bytes = result.maximum_io_bytes_per_batch;
    result.maximum_planner_work_units
        = std::min(result.maximum_planner_work_units, limits.get(ResourceLimit::VerificationPlannerWorkUnitsPerBatch));
    result.maximum_planner_scratch_bytes
        = std::min(result.maximum_planner_scratch_bytes, limits.get(ResourceLimit::VerificationPlannerScratchBytesPerBatch));
    result.maximum_retained_canonical_bytes
        = std::min(result.maximum_retained_canonical_bytes, limits.get(ResourceLimit::VerificationRetainedBytesPerBatch));
    return result;
}

}
