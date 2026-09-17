#pragma once

#include <Databases/UDT/AuthorityRepairAudit.h>
#include <Databases/UDT/AuthorityRepairCoordinator.h>

#include <Core/Types.h>

#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <vector>

namespace DB
{

class DatabaseAtomic;

namespace UDT
{

class AuthorityAutomaticRepairContinuation final
{
public:
    class Impl;

    AuthorityAutomaticRepairContinuation();
    AuthorityAutomaticRepairContinuation(const AuthorityAutomaticRepairContinuation &) = delete;
    AuthorityAutomaticRepairContinuation & operator=(const AuthorityAutomaticRepairContinuation &) = delete;
    ~AuthorityAutomaticRepairContinuation();

private:
    friend class AuthorityAutomaticRepair;
    std::unique_ptr<Impl> impl;
};

struct AuthorityExactRepairSourceLimits
{
    UInt64 maximum_candidates = 100'000;
    UInt64 maximum_total_candidate_bytes = 512ULL << 20;
    UInt64 maximum_source_reference_bytes = 4ULL << 10;
    /// Aggregate non-payload control storage for the returned owned vector.
    /// Implementations must admit it before allocating result elements.
    UInt64 maximum_control_bytes = 64ULL << 20;
    std::stop_token cancellation;
    std::optional<std::chrono::steady_clock::time_point> monotonic_deadline;
    std::optional<UInt64> thread_cpu_deadline_nanoseconds;
};

struct AuthorityExactRepairSourceCandidate
{
    AuthorityRepairArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 object_revision = 0;
    Digest physical_schema_fingerprint{};
    String canonical_bytes;
    String source_reference;
};

/// Database-owned seam for an already-authenticated exact authority source.
/// Implementations own replica/backup trust and freshness checks and return
/// bounded owned bytes only; the pure repair planner independently decodes and
/// hashes every candidate against the anchored audit target. `collect` is one
/// bounded cooperative work item: implementations must prospectively enforce
/// all count/byte/control-memory limits and cancellation/deadlines while doing
/// any cluster or backup I/O. It runs under the database schema-mutation fence
/// and must not re-enter DatabaseAtomic, invoke schema mutations, or call back
/// into this repair continuation. A cancellation/deadline before the requested chunk has a final
/// source answer must throw AuthorityRepairAuditError(ExecutionBudgetExceeded),
/// never return a partial result that would advance the monotonic chunk cursor.
/// A successful result is strictly ordered by the requested target order and
/// contains at most one candidate for each requested target.
/// This adapter boundary does not provide cluster/backup orchestration or
/// an implicit trust policy.
class IExactAuthorityRepairSource
{
public:
    virtual ~IExactAuthorityRepairSource() = default;
    virtual AuthorityRepairSource getSource() const noexcept = 0;
    virtual std::vector<AuthorityExactRepairSourceCandidate> collect(
        const AuthorityRootGraphIdentity & root,
        std::span<const AuthorityRepairTarget> missing_targets,
        const AuthorityExactRepairSourceLimits & limits) const = 0;
};

struct AuthorityAutomaticRepairLimits
{
    AuthorityRepairAuditLimits audit;
    AuthorityRepairPlanLimits plan;
    AuthorityExactRepairLimits execution;
    UInt64 maximum_local_wal_transactions = 200'000;
    /// Maximum bounded directory/merge/validation work items performed by one
    /// resumable storage discovery call before the orchestration rechecks its
    /// shared wall/CPU/cancellation budget.
    UInt64 maximum_local_wal_discovery_work_items_per_pass = 256;
    UInt64 maximum_local_wal_artifacts_examined = 1'200'000;
    UInt64 maximum_local_wal_bytes_examined = 1ULL << 30;
    UInt64 maximum_audit_input_retained_bytes = 256ULL << 20;
    /// Peak logical non-payload memory of source discovery. This includes all
    /// retained target/candidate/index/transaction-ID controls plus one
    /// in-flight WAL decode or external adapter result. Canonical candidate
    /// bytes and provenance remain independently bounded by the plan limits.
    UInt64 maximum_source_control_bytes = 256ULL << 20;
    UInt64 maximum_external_source_targets_per_call = 1;
    UInt64 maximum_external_source_bytes_per_call = 256ULL << 20;
    std::shared_ptr<const IExactAuthorityRepairSource> replicated_authority_source;
    std::shared_ptr<const IExactAuthorityRepairSource> verified_backup_source;
    std::stop_token cancellation;
    /// One scheduler-run budget shared by audit, local WAL, external source
    /// selection, planning, and execution. Individual storage/source calls are
    /// atomic cooperative work items and are checked immediately around them.
    std::optional<std::chrono::steady_clock::time_point> monotonic_deadline;
    std::optional<UInt64> thread_cpu_deadline_nanoseconds;
    std::shared_ptr<AuthorityAutomaticRepairContinuation> continuation;
};

enum class AuthorityAutomaticRepairStatus : UInt8
{
    NoQuarantine = 1,
    ReverifiedAndReleased = 2,
    AuditUnrepairable = 3,
    ExactSourceUnavailable = 4,
    RepairedAndReleased = 5,
    RepairedQuarantineRetained = 6,
};

struct AuthorityAutomaticRepairResult
{
    AuthorityAutomaticRepairStatus status = AuthorityAutomaticRepairStatus::NoQuarantine;
    UInt64 audited_artifacts = 0;
    UInt64 damaged_artifacts = 0;
    UInt64 local_wal_transactions_examined = 0;
    UInt64 local_wal_artifacts_examined = 0;
    UInt64 local_wal_bytes_examined = 0;
    std::optional<AuthorityExactRepairResult> execution;
};

/// Database-owned automatic exact-repair orchestration.  A complete trusted
/// audit first re-anchors/expands the runtime quarantine without releasing any
/// member. Exact bytes are selected in strict local-schema-WAL, replicated-
/// authority, then verified-backup priority through authenticated bounded
/// adapters and are independently validated at the pure AuthorityRepairPlan
/// boundary.
/// The coordinator commits the content-neutral successor and releases only
/// after its sealed complete-root verification succeeds.
class AuthorityAutomaticRepair final
{
public:
    [[nodiscard]] static AuthorityAutomaticRepairResult
    attempt(DB::DatabaseAtomic & database, const AuthorityAutomaticRepairLimits & limits = {});

    /// Compatibility alias retained for existing callers. The implementation
    /// now runs the complete configured WAL -> replica -> verified-backup
    /// hierarchy rather than stopping after the local WAL.
    [[nodiscard]] static AuthorityAutomaticRepairResult
    attemptLocalSchemaWAL(DB::DatabaseAtomic & database, const AuthorityAutomaticRepairLimits & limits = {});

private:
    AuthorityAutomaticRepair() = delete;
};

}
}
