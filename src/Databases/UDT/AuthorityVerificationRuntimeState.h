#pragma once

#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityQuarantineAdmission.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace DB
{
class DatabaseAtomic;
}

namespace DB::UDT
{

class AuthorityRoot;
class AuthorityRepairCoordinator;
class AuthorityAutomaticRepair;
class AuthorityStorageNewOperationCommitGuard;

struct AuthorityVerificationRuntimeStateLimits
{
    std::size_t hazard_slot_count = 256;
    /// One extra slot lets a writer retire the current snapshot even when
    /// every reader hazard protects a different older snapshot.
    UInt64 maximum_retired_snapshot_count = 257;
    AuthorityQuarantinePlanLimits quarantine;
};

enum class AuthorityVerificationRuntimeConsumeStatus : UInt8
{
    CursorAdvanced,
    NoWork,
    DamagedQuarantined,
    DamagedFailClosed,
    RetryRootChanged,
    RetryIncompleteRotation,
};

struct AuthorityVerificationRuntimeConsumeResult
{
    AuthorityVerificationRuntimeConsumeStatus status = AuthorityVerificationRuntimeConsumeStatus::RetryIncompleteRotation;
    AuthorityVerificationCursorDecision cursor_decision;
    UInt64 damaged_target_count = 0;
    UInt64 failing_seed_count = 0;
    UInt64 quarantined_object_count = 0;
    UInt64 published_revision = 0;
};

/// Stable, bounded runtime diagnostic carried by the same immutable snapshot
/// as the cursor/quarantine admission state. It deliberately contains no
/// object names, canonical SQL, or exception text.
enum class AuthorityVerificationRuntimeLastErrorKind : UInt8
{
    None,
    IntegrityDamageQuarantined,
    QuarantineConstructionFailed,
};

class AuthorityVerificationRuntimeStateError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidCursor,
        InvalidRoot,
        HazardSlotsExhausted,
        Shutdown,
    };

    AuthorityVerificationRuntimeStateError(Code code_, std::string_view message);

    const Code code;
};

/// Database-owned lock-free runtime publication for the durable verification
/// cursor and its exact-root quarantine closure. Writers must already own the
/// database schema-mutation serialization boundary. Readers claim one bounded
/// hazard slot and observe both values from the same immutable snapshot.
class AuthorityVerificationRuntimeState final : public IAtomicAuthorityPublicationObserver
{
    struct PublishedState;
    class Impl;
    class PreparedAuthorityPublication;

public:
    class Snapshot final
    {
    public:
        Snapshot(const Snapshot &) = delete;
        Snapshot & operator=(const Snapshot &) = delete;
        Snapshot(Snapshot && other) noexcept;
        Snapshot & operator=(Snapshot &&) = delete;
        ~Snapshot();

        const AuthorityVerificationScheduleCursor & getCursor() const;
        const AuthorityQuarantinePlan::Ptr & getQuarantine() const;
        UInt64 getRevision() const noexcept;
        bool isFailClosed() const noexcept;
        AuthorityVerificationRuntimeLastErrorKind getLastErrorKind() const noexcept;

    private:
        Snapshot(Impl * owner_, std::size_t hazard_slot_, const PublishedState * state_) noexcept;

        friend class AuthorityVerificationRuntimeState;
        friend class Impl;
        Impl * owner;
        std::size_t hazard_slot;
        const PublishedState * state;
    };

    AuthorityVerificationRuntimeState(
        UUID database_uuid_,
        AuthorityVerificationScheduleCursor initial_cursor,
        const AuthorityVerificationRuntimeStateLimits & limits = {});
    AuthorityVerificationRuntimeState(const AuthorityVerificationRuntimeState &) = delete;
    AuthorityVerificationRuntimeState & operator=(const AuthorityVerificationRuntimeState &) = delete;
    ~AuthorityVerificationRuntimeState() override;

    [[nodiscard]] Snapshot acquireSnapshot() const;
    [[nodiscard]] AuthorityVerificationScheduleCursor getCursor() const;

    /// Consumes a sealed executor receipt against the same exact root while
    /// the caller still owns schema serialization. Any valid damaged prefix
    /// publishes its complete reverse-dependency closure without advancing
    /// the cursor. An unrepresentable/corrupt damage identity publishes a
    /// stricter fail-closed state instead of leaving operations admitted.
    [[nodiscard]] AuthorityVerificationRuntimeConsumeResult consume(
        const AuthorityRoot & exact_current_root,
        const AuthorityVerificationBatchPlan & plan,
        const AuthorityVerificationBatchReceipt & receipt,
        const std::function<void(const AuthorityVerificationScheduleCursor &)> & persist_advanced_cursor = {});

    /// Lock-free operation boundary. Hazard exhaustion, shutdown, or a prior
    /// quarantine-construction failure are rejected rather than bypassed.
    [[nodiscard]] AuthorityQuarantineAdmissionDecision decideOperation(
        const AuthorityQuarantineOperationView & operation, const AuthorityQuarantineAdmissionLimits & limits = {}) const noexcept;

    void scanRetired();
    void shutdownAndDrain() noexcept;
    bool isShutdown() const noexcept;

private:
    friend class DB::DatabaseAtomic;
    friend class AuthorityRepairCoordinator;
    friend class AuthorityAutomaticRepair;
    friend class AuthorityStorageNewOperationCommitGuard;

    /// Serializes the exact final publication point of an admitted non-read
    /// storage operation with quarantine/fail-closed publication. The caller
    /// must run its complete new-operation admission while retaining this
    /// counted fence; runtime publications wait for all such fences to drain.
    /// Acquisition is reentrant only on the acquiring thread. The outermost
    /// acquisition owns the counted fence; nested guards retain that fence and
    /// must be destroyed on the same thread. Nesting fences from independent
    /// database runtimes is rejected rather than risking cross-runtime ABBA.
    void acquireNewOperationCommitFence() const;
    void releaseNewOperationCommitFence() const noexcept;

    /// Prepares the exact quarantine re-anchor for an authority mutation. An
    /// unrelated mutation may advance the root only when it preserves every
    /// quarantined inventory image and both sides of its graph adjacency, and
    /// rebuilding the same failing seeds yields the identical closure.
    std::unique_ptr<IAtomicAuthorityPublicationObserver::PreparedTransition>
    prepareAuthorityPublication(const AuthorityRoot & before, const AuthorityRoot & after) override;

    /// Re-anchors (and, if the complete audit found more damage, expands) an
    /// already-published quarantine to one freshly audited exact root.  The
    /// old closure must be a subset, so this operation can never release an
    /// object.  Only the full trusted automatic-repair audit may call it.
    void publishAutomaticRepairAuditQuarantine(
        const AuthorityRoot & exact_audited_root, const AuthorityQuarantinePlan::Ptr & audited_quarantine);

    /// Trusted release seam. The coordinator calls this only after sealed
    /// executor receipts cover the complete current-root inventory while
    /// schema serialization is still held.  The root may be the quarantined
    /// root itself when a complete audit finds that a transient mismatch has
    /// already converged, or a later exact-repair successor.
    void releaseQuarantineAfterCompleteVerification(
        const AuthorityRoot & exact_repaired_root, const AuthorityQuarantinePlan::Ptr & expected_quarantine);

    const UUID database_uuid;
    std::unique_ptr<Impl> impl;
};

}
