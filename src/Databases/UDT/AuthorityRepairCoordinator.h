#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRepairPlan.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Core/Types.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>

namespace DB
{

class DatabaseAtomic;

namespace UDT
{

class AtomicDatabaseSchemaMutationStorage;
class AuthorityVerificationRuntimeState;

/// Database-scheduler-owned bounded process-local progress for complete
/// quarantine re-verification. It carries no authority capability and is
/// discarded automatically if the exact root or quarantine object changes.
class AuthorityRepairReverificationContinuation final
{
public:
    AuthorityRepairReverificationContinuation();
    AuthorityRepairReverificationContinuation(const AuthorityRepairReverificationContinuation &) = delete;
    AuthorityRepairReverificationContinuation & operator=(const AuthorityRepairReverificationContinuation &) = delete;
    ~AuthorityRepairReverificationContinuation();

    [[nodiscard]] bool isActiveFor(const AuthorityQuarantinePlan & quarantine) const noexcept;

private:
    friend class AuthorityRepairCoordinator;
    class Impl;
    std::unique_ptr<Impl> impl;
};

struct AuthorityExactRepairLimits
{
    DatabaseSchemaWALLimits wal;
    AuthorityStateLimits authority_state;
    AuthorityVerificationScheduleLimits verification_schedule;
    AuthorityVerificationBatchExecutorLimits verification_executor;
    UInt64 maximum_reverification_batches = 200'000;
    /// Cumulative snapshot, batch-planning, and terminal target work items in
    /// one coordinator invocation. Sealed continuations carry the remainder of
    /// the complete-root pass into the next scheduler run.
    UInt64 maximum_reverification_work_items_per_pass = 256;
    /// Process-local bytes retained by the resumable exact-root target
    /// snapshot. This is independent of canonical/input byte budgets: each
    /// fixed-size scheduling record survives between cooperative passes.
    UInt64 maximum_reverification_retained_bytes = 64ULL << 20;
    std::shared_ptr<AuthorityRepairReverificationContinuation> reverification_continuation;
};

enum class AuthorityExactRepairStatus : UInt8
{
    RepairedAndReleased = 1,
    RepairedQuarantineRetained = 2,
    ReverifiedAndReleased = 3,
    ReverificationFailed = 4,
};

struct AuthorityExactRepairResult
{
    AuthorityExactRepairStatus status = AuthorityExactRepairStatus::ReverificationFailed;
    UInt64 transaction_id = 0;
    AuthorityRootIdentity previous_root;
    AuthorityRootIdentity repaired_root;
    UInt64 repaired_artifacts = 0;
    Digest damaged_artifact_manifest_digest{};
    UInt64 local_wal_sources = 0;
    UInt64 replicated_authority_sources = 0;
    UInt64 verified_backup_sources = 0;
    UInt64 reverification_batches = 0;
    UInt64 reverified_inventory_targets = 0;
    UInt64 released_quarantined_objects = 0;
};

class AuthorityRepairCoordinatorError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidPlan,
        InvalidQuarantine,
        InvalidRoot,
        TransactionIDExhausted,
        ReverificationLimitExceeded,
    };

    AuthorityRepairCoordinatorError(Code code_, std::string_view message);

    const Code code;
};

/// Concrete Atomic repair boundary. It consumes an exact-source plan while
/// holding database schema serialization, commits a content-neutral epoch
/// transition through the ordinary schema WAL, publishes once, then performs
/// one complete linear inventory/live-object verification using only sealed
/// executor receipts. Any failure after publication leaves the old exact
/// quarantine in place. A later call to resumeReverificationAndRelease can
/// safely finish that last phase without writing another repair transaction.
/// Every entry point requires a non-null database-owned re-verification
/// continuation in `limits`; the caller must retain and reuse it until the
/// quarantine is released. Invalid limits are rejected before any schema-WAL
/// mutation, so bounded progress can never be stranded in ephemeral state.
class AuthorityRepairCoordinator final
{
public:
    [[nodiscard]] static AuthorityExactRepairResult
    executeAndRelease(DB::DatabaseAtomic & database, const AuthorityRepairPlan & plan, const AuthorityExactRepairLimits & limits);

    /// Same trusted boundary for an orchestrator which already owns the exact
    /// database schema lock while auditing and selecting sources. This avoids
    /// a stale-root window between audit and repair commit.
    [[nodiscard]] static AuthorityExactRepairResult executeAndRelease(
        DB::DatabaseAtomic & database,
        const AuthorityRepairPlan & plan,
        std::unique_lock<std::mutex> schema_lock,
        const AuthorityExactRepairLimits & limits);

    [[nodiscard]] static AuthorityExactRepairResult
    resumeReverificationAndRelease(DB::DatabaseAtomic & database, const AuthorityExactRepairLimits & limits);
    [[nodiscard]] static AuthorityExactRepairResult resumeReverificationAndRelease(
        DB::DatabaseAtomic & database, std::unique_lock<std::mutex> schema_lock, const AuthorityExactRepairLimits & limits);

private:
    static bool reverifyAndRelease(
        DB::DatabaseAtomic & database,
        AtomicAuthority & authority,
        AtomicDatabaseSchemaMutationStorage & storage,
        AuthorityVerificationRuntimeState & runtime,
        AtomicAuthority::RootSnapshot & repaired_root,
        const AuthorityQuarantinePlan::Ptr & quarantine,
        std::unique_lock<std::mutex> schema_lock,
        const AuthorityExactRepairLimits & limits,
        AuthorityExactRepairResult & result);

    AuthorityRepairCoordinator() = delete;
};
}
}
