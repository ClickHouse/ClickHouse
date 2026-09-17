#pragma once

#include <Databases/UDT/AuthorityQuarantinePlan.h>
#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <Core/Types.h>

#include <span>

namespace DB::UDT
{

enum class AuthorityQuarantineOperationKind : UInt8
{
    Read = 1,
    Write = 2,
    Mutation = 3,
    DDL = 4,
    Attach = 5,
};

enum class AuthorityQuarantineOperationTiming : UInt8
{
    New = 1,
    StartedBeforeQuarantine = 2,
};

/// Exact current image and dependency closure used by one already-running
/// read. The stamp pointer is borrowed for this decision only.
struct AuthorityReadContinuationObjectProofView
{
    AuthorityObjectImageIdentity current_object;
    const AuthorityVerificationStamp * last_verification_stamp = nullptr;
    std::span<const DefinitionIdentity> sorted_unique_current_required_definitions;
};

/// Trusted execution-boundary view. `touch_set_is_complete` may be set only
/// by the component that owns the operation's complete object/dependency
/// inventory; this pure API cannot infer omitted touches. Continuation proofs
/// are required only when an affected read started before quarantine.
struct AuthorityQuarantineOperationView
{
    AuthorityQuarantineOperationKind kind = AuthorityQuarantineOperationKind::Read;
    AuthorityQuarantineOperationTiming timing = AuthorityQuarantineOperationTiming::New;
    AuthorityRootGraphIdentity pinned_root;
    bool touch_set_is_complete = false;
    std::span<const SchemaObjectID> sorted_unique_touched_objects;
    bool continuation_proof_set_is_complete = false;
    std::span<const AuthorityReadContinuationObjectProofView> sorted_unique_continuation_proofs;
};

/// Every value is a positive, lowerable limit. The default values are also
/// implementation maxima; callers may only lower them through effective
/// server/database/query limits.
struct AuthorityQuarantineAdmissionLimits
{
    UInt64 maximum_touched_objects = 65'536;
    UInt64 maximum_continuation_proofs = 65'536;
    UInt64 maximum_required_definitions_per_proof = 65'536;
    /// Counts both the current closure and the borrowed stamped closure.
    UInt64 maximum_inspected_definition_items = 1'048'576;
    UInt64 maximum_work_units = 16'777'216;
    UInt64 maximum_evidence_canonical_bytes = 64ULL << 20;
};

struct AuthorityQuarantineAdmissionStatistics
{
    UInt64 quarantined_objects = 0;
    UInt64 touched_objects = 0;
    UInt64 continuation_proofs = 0;
    UInt64 current_definition_items = 0;
    UInt64 stamped_definition_items = 0;
    UInt64 quarantined_touched_objects = 0;
    UInt64 work_units = 0;
    UInt64 evidence_canonical_bytes = 0;

    bool operator==(const AuthorityQuarantineAdmissionStatistics &) const = default;
};

enum class AuthorityQuarantineAdmissionStatus : UInt8
{
    AllowedUnaffected,
    AllowedReadContinuation,
    /// The database-owned runtime snapshot cannot safely establish the exact
    /// quarantine image (shutdown, hazard exhaustion, or bounded closure
    /// construction failure). No operation may bypass this state.
    RuntimeFailClosed,
    InvalidConfiguration,
    ArithmeticOverflow,
    LimitExceeded,
    InvalidOperationKind,
    InvalidOperationTiming,
    InvalidQuarantineIdentity,
    OperationRootMismatch,
    IncompleteTouchSet,
    NonCanonicalTouchSet,
    NonCanonicalContinuationEvidence,
    NewOperationTouchesQuarantine,
    NonReadContinuationRejected,
    IncompleteContinuationEvidence,
    ContinuationRootMismatch,
    ContinuationStampMismatch,
    ContinuationDependencyMismatch,
};

struct AuthorityQuarantineAdmissionDecision
{
    AuthorityQuarantineAdmissionStatus status = AuthorityQuarantineAdmissionStatus::InvalidConfiguration;
    AuthorityQuarantineAdmissionStatistics statistics;

    bool isAllowed() const noexcept
    {
        return status == AuthorityQuarantineAdmissionStatus::AllowedUnaffected
            || status == AuthorityQuarantineAdmissionStatus::AllowedReadContinuation;
    }
};

/// Pure deterministic quarantine gate. It allocates, retains, publishes and
/// mutates nothing. All input cardinalities, canonical bytes and worst-case
/// work are admitted prospectively before semantic evidence inspection.
[[nodiscard]] AuthorityQuarantineAdmissionDecision decideAuthorityQuarantineAdmission(
    const AuthorityQuarantinePlan & quarantine,
    const AuthorityQuarantineOperationView & operation,
    const AuthorityQuarantineAdmissionLimits & limits = {}) noexcept;

}
