#pragma once

#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <DataTypes/UDT/AuthorityInventory.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;
class AuthorityVerificationBatchExecutor;
class AuthorityVerificationBatchReceipt;
class AuthorityVerificationBatchReceiptFactory;
class AuthorityVerificationPlanningContinuation;
struct AuthorityVerificationPlanningResult;
struct AuthorityRepairReverificationBatchPlanningResult;

inline constexpr UInt16 authority_verification_schedule_contract_abi = 1;
/// Frozen accounting contract used by prospective schedule declarations and
/// trusted execution receipts. Additive dimensions accumulate across
/// sequential targets; transient_bytes is the maximum simultaneously retained
/// charge, not the sum of per-target peaks. Any change to source-read,
/// decode/validation, or verifier attribution must bump this ABI.
inline constexpr UInt16 authority_verification_charge_abi = 2;

/// Domain-separated identity of the complete immutable inventory. The Merkle
/// root already commits to every canonical leaf; hashing the count and root
/// avoids materializing the persistent radix tree during each execution.
[[nodiscard]] Digest computeAuthorityVerificationTargetSetDigest(const AuthorityInventorySummary & inventory);

/// Prospective upper bounds for verifying one inventory target. The executor
/// must account actual work independently and stop before any bound is
/// exceeded; these declarations only make a batch schedulable in advance.
struct AuthorityVerificationTargetCost
{
    UInt64 canonical_bytes = 0;
    UInt64 work_units = 0;
    UInt64 transient_bytes = 0;
    UInt64 io_bytes = 0;

    bool operator==(const AuthorityVerificationTargetCost &) const = default;
};

/// Immutable scheduling view of one exact inventory leaf. Priority metadata
/// never establishes integrity and is deliberately excluded from target-set
/// identity. A zero epoch/sequence means that the corresponding observation
/// is unavailable, not that the target is verified.
struct AuthorityVerificationTarget
{
    AuthorityInventoryLeaf leaf;
    UInt64 last_changed_catalog_epoch = 0;
    UInt64 last_periodic_verification_sequence = 0;
    UInt64 reverse_dependency_count = 0;
    AuthorityVerificationTargetCost cost;

    bool operator==(const AuthorityVerificationTarget &) const = default;
};

/// Bounded scheduler-owned history for one exact inventory leaf. Matching the
/// complete leaf, rather than only its stable key, prevents a replacement at
/// the same UUID from inheriting an earlier verification observation.
struct AuthorityVerificationTargetHistory
{
    AuthorityInventoryLeaf leaf;
    UInt64 last_changed_catalog_epoch = 0;
    UInt64 last_periodic_verification_sequence = 0;

    bool operator==(const AuthorityVerificationTargetHistory &) const = default;
};

/// Administrator-selected deterministic scheduling policy. Bucket placement
/// is stable across authority-root changes for the same database and policy.
struct AuthorityVerificationSchedulePolicy
{
    UInt32 bucket_count = 256;
    UInt64 bucket_seed = 0;
    UInt64 recent_catalog_epoch_window = 64;
    UInt64 high_dependency_threshold = 128;
    UInt64 maximum_recent_targets_per_batch = 32;
    UInt64 maximum_high_dependency_targets_per_batch = 32;
    UInt64 random_targets_per_batch = 8;

    bool operator==(const AuthorityVerificationSchedulePolicy &) const = default;
};

/// Independent finite ceilings for one pure scheduling decision and its
/// resulting execution batch. No zero or unlimited sentinel is accepted.
struct AuthorityVerificationScheduleLimits
{
    /// Immutable execution-domain guard for one already-published inventory.
    /// Mutable database admission quotas must not lower this below existing
    /// pinned usage; OVER_QUOTA blocks growth but does not stop verification.
    UInt64 maximum_snapshot_targets = 200'000;
    UInt64 maximum_targets_per_batch = 1'024;
    UInt64 maximum_buckets = 4'096;
    UInt64 maximum_reverse_dependency_count = 200'000;
    /// These four execution ceilings are the exact durable admission
    /// verifier domain. Source reads, parsing, and canonical input validation
    /// are bounded independently by I/O/canonical/transient and scheduler
    /// wall/CPU limits; they must not be re-attributed as byte-proportional
    /// verifier work on top of a stamp that already consumed this domain.
    UInt64 maximum_canonical_bytes_per_batch = 256ULL << 20;
    UInt64 maximum_verification_work_units_per_batch = 8'388'608;
    UInt64 maximum_transient_bytes_per_batch = 64ULL << 20;
    UInt64 maximum_io_bytes_per_batch = 256ULL << 20;
    /// Exact immutable-root escape hatch for one indivisible target. These
    /// derived values equal the effective aggregate caps for a newly admitted
    /// root and may be widened only to the rooted usage already admitted by a
    /// published root. A target using the escape hatch is the whole batch.
    UInt64 maximum_rooted_target_canonical_bytes = 256ULL << 20;
    UInt64 maximum_rooted_target_verification_work_units = 8'388'608;
    UInt64 maximum_rooted_target_transient_bytes = 64ULL << 20;
    UInt64 maximum_rooted_target_io_bytes = 256ULL << 20;
    UInt64 maximum_planner_work_units = 67'108'864;
    UInt64 maximum_planner_scratch_bytes = 16ULL << 20;
    UInt64 maximum_retained_canonical_bytes = 16ULL << 20;

    bool operator==(const AuthorityVerificationScheduleLimits &) const = default;
};

/// Deterministic prospective domain of one complete planning decision for an
/// exact immutable target set and policy. It is independent of target bytes
/// and local allocator capacity. The work value is a conservative bound over
/// every cursor/history arrangement for that exact target count.
struct AuthorityVerificationPlanningRequirements
{
    UInt64 planner_work_units = 0;
    UInt64 planner_scratch_bytes = 0;
    UInt64 retained_canonical_bytes = 0;

    bool operator==(const AuthorityVerificationPlanningRequirements &) const = default;
};

[[nodiscard]] AuthorityVerificationPlanningRequirements computeAuthorityVerificationPlanningRequirements(
    UInt64 snapshot_targets, const AuthorityVerificationSchedulePolicy & policy, UInt64 maximum_targets_per_batch);

/// Cooperative run boundary shared by snapshot construction and resumable
/// planning. The limit is checked before each bounded work item. Individual
/// synchronous file reads and one-object parsers remain atomic work items.
struct AuthorityVerificationPassBudget
{
    std::stop_token cancellation;
    std::optional<std::chrono::steady_clock::time_point> monotonic_deadline;
    std::optional<UInt64> thread_cpu_deadline_nanoseconds;
    UInt64 maximum_work_items = std::numeric_limits<UInt64>::max();
};

/// Bounded resumable liveness cursor. It never proves that an object was
/// verified and therefore may cross authority-root changes. A stable key
/// inserted behind the cursor is visited during the next complete rotation.
struct AuthorityVerificationScheduleCursor
{
    UInt16 contract_abi = authority_verification_schedule_contract_abi;
    UUID database_uuid = UUIDHelpers::Nil;
    UInt32 bucket_count = 0;
    UInt64 bucket_seed = 0;
    UInt32 current_bucket = 0;
    std::optional<AuthorityInventoryKey> resume_after;
    UInt64 completed_rotations = 0;
    UInt64 planned_batches = 0;

    bool operator==(const AuthorityVerificationScheduleCursor &) const = default;
};

enum class AuthorityVerificationSelectionReason : UInt8
{
    Rotation = UInt8{1} << 0,
    RecentlyChanged = UInt8{1} << 1,
    HighDependency = UInt8{1} << 2,
    OutsideBucketSample = UInt8{1} << 3,
};

using AuthorityVerificationSelectionReasons = UInt8;

[[nodiscard]] constexpr AuthorityVerificationSelectionReasons
authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason reason) noexcept
{
    return static_cast<AuthorityVerificationSelectionReasons>(reason);
}

struct ScheduledAuthorityVerificationTarget
{
    AuthorityInventoryLeaf leaf;
    AuthorityVerificationTargetCost cost;
    AuthorityVerificationSelectionReasons reasons = 0;

    bool operator==(const ScheduledAuthorityVerificationTarget &) const = default;
};

struct AuthorityVerificationScheduleStatistics
{
    UInt64 snapshot_targets = 0;
    /// With a fixed nonempty snapshot, one mandatory rotation target per
    /// successfully committed batch makes this a conservative full-coverage
    /// bound. Preparing a batch never advances the durable cursor.
    UInt64 maximum_batches_per_full_rotation = 0;
    UInt64 buckets_examined = 0;
    UInt64 targets_examined = 0;
    UInt64 rotation_targets = 0;
    UInt64 recent_targets = 0;
    UInt64 high_dependency_targets = 0;
    UInt64 random_targets = 0;
    UInt64 verification_canonical_bytes = 0;
    UInt64 verification_work_units = 0;
    UInt64 verification_transient_bytes = 0;
    UInt64 verification_io_bytes = 0;
    UInt64 planner_work_units = 0;
    UInt64 planner_scratch_bytes = 0;
    UInt64 retained_canonical_bytes = 0;

    bool operator==(const AuthorityVerificationScheduleStatistics &) const = default;
};

enum class AuthorityVerificationScheduleStatus : UInt8
{
    Scheduled,
    EmptySnapshot,
};

enum class AuthorityVerificationCursorDecisionStatus : UInt8
{
    Advanced,
    RetryRootChanged,
    RetryIncompleteRotation,
    NoWork,
};

struct AuthorityVerificationCursorDecision
{
    AuthorityVerificationCursorDecisionStatus status = AuthorityVerificationCursorDecisionStatus::RetryIncompleteRotation;
    AuthorityVerificationScheduleCursor cursor;

    bool operator==(const AuthorityVerificationCursorDecision &) const = default;
};

class AuthorityVerificationScheduleError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ArithmeticOverflow,
        InvalidRootIdentity,
        InvalidCursor,
        InvalidTarget,
        NonCanonicalTargetSet,
        UnschedulableRotationTarget,
        InvalidReceipt,
    };

    AuthorityVerificationScheduleError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable exact-root execution plan. Selected leaves and declared costs are
/// copied; authority roots, inventory trees, records and object handles are
/// never retained.
class AuthorityVerificationBatchPlan final
{
public:
    using Ptr = std::shared_ptr<const AuthorityVerificationBatchPlan>;

    const AuthorityRootIdentity & getRoot() const noexcept { return root; }
    const Digest & getTargetSetDigest() const noexcept { return target_set_digest; }
    UInt16 getChargeABI() const noexcept { return charge_abi; }
    AuthorityVerificationScheduleStatus getStatus() const noexcept { return status; }
    /// The only cursor callers may persist before successful completion.
    const AuthorityVerificationScheduleCursor & getRetryCursor() const noexcept { return retry_cursor; }
    std::span<const ScheduledAuthorityVerificationTarget> getTargets() const noexcept { return targets; }
    const AuthorityVerificationScheduleStatistics & getStatistics() const noexcept { return statistics; }

private:
    friend AuthorityVerificationBatchPlan::Ptr planPeriodicAuthorityVerification(
        const AuthorityRoot &,
        std::span<const AuthorityVerificationTarget>,
        const AuthorityVerificationScheduleCursor &,
        const AuthorityVerificationSchedulePolicy &,
        const AuthorityVerificationScheduleLimits &);
    friend AuthorityVerificationCursorDecision
    finalizePeriodicAuthorityVerificationBatch(const AuthorityVerificationBatchPlan &, const AuthorityVerificationBatchReceipt &);
    friend AuthorityVerificationPlanningResult resumePeriodicAuthorityVerificationPlanning(
        const AuthorityRoot &,
        std::span<const AuthorityVerificationTarget>,
        const AuthorityVerificationScheduleCursor &,
        const AuthorityVerificationSchedulePolicy &,
        const AuthorityVerificationScheduleLimits &,
        std::unique_ptr<AuthorityVerificationPlanningContinuation> &,
        const AuthorityVerificationPassBudget &);
    friend std::vector<AuthorityVerificationBatchPlan::Ptr> planCompleteAuthorityRepairReverification(
        const AuthorityRoot &, std::span<const AuthorityVerificationTarget>, const AuthorityVerificationScheduleLimits &);
    friend AuthorityRepairReverificationBatchPlanningResult planNextAuthorityRepairReverificationBatch(
        const AuthorityRoot &,
        std::span<const AuthorityVerificationTarget>,
        UInt64,
        const AuthorityVerificationScheduleCursor &,
        const AuthorityVerificationScheduleLimits &,
        const AuthorityVerificationPassBudget &);

    AuthorityVerificationBatchPlan(
        AuthorityRootIdentity root_,
        Digest target_set_digest_,
        AuthorityVerificationScheduleStatus status_,
        AuthorityVerificationScheduleCursor retry_cursor_,
        AuthorityVerificationScheduleCursor next_cursor_,
        std::vector<ScheduledAuthorityVerificationTarget> targets_,
        AuthorityVerificationScheduleStatistics statistics_) noexcept;

    const AuthorityRootIdentity root;
    const Digest target_set_digest;
    const UInt16 charge_abi = authority_verification_charge_abi;
    const AuthorityVerificationScheduleStatus status;
    const AuthorityVerificationScheduleCursor retry_cursor;
    const AuthorityVerificationScheduleCursor next_cursor;
    const std::vector<ScheduledAuthorityVerificationTarget> targets;
    const AuthorityVerificationScheduleStatistics statistics;
};

/// A terminal result for one target. Work that fails or is cancelled before a
/// terminal disposition is reached has no entry; consequently an incomplete
/// execution is represented by a strict prefix of the plan's ordered targets.
enum class AuthorityVerificationTargetDisposition : UInt8
{
    Verified = 1,
    Damaged = 2,
};

struct AuthorityVerificationTargetCompletion
{
    AuthorityInventoryLeaf leaf;
    AuthorityVerificationTargetDisposition disposition = AuthorityVerificationTargetDisposition::Damaged;
    AuthorityVerificationTargetCost actual_charged_cost;

    bool operator==(const AuthorityVerificationTargetCompletion &) const = default;
};

/// Immutable completion evidence issued only by the bounded exact-root
/// executor boundary below. A later execution may adopt a previously sealed
/// clean prefix and append newly verified suffix targets; failed-attempt work
/// is not evidence and is deliberately not carried into the cumulative cost.
/// The receipt binds the pinned root, complete target-set digest, plan cursor,
/// ordered terminal results and actual evidence costs. Callers cannot mint it.
class AuthorityVerificationBatchReceipt final
{
public:
    using Ptr = std::shared_ptr<const AuthorityVerificationBatchReceipt>;

    AuthorityVerificationBatchReceipt(const AuthorityVerificationBatchReceipt &) = delete;
    AuthorityVerificationBatchReceipt & operator=(const AuthorityVerificationBatchReceipt &) = delete;

    const AuthorityRootIdentity & getRoot() const noexcept { return root; }
    const Digest & getTargetSetDigest() const noexcept { return target_set_digest; }
    UInt16 getChargeABI() const noexcept { return charge_abi; }
    std::span<const AuthorityVerificationTargetCompletion> getTerminalCompletions() const noexcept { return terminal_completions; }
    const AuthorityVerificationTargetCost & getActualChargedCost() const noexcept { return actual_charged_cost; }

private:
    friend class AuthorityVerificationBatchReceiptFactory;
    friend AuthorityVerificationCursorDecision
    finalizePeriodicAuthorityVerificationBatch(const AuthorityVerificationBatchPlan &, const AuthorityVerificationBatchReceipt &);

    AuthorityVerificationBatchReceipt(
        AuthorityRootIdentity root_,
        Digest target_set_digest_,
        AuthorityVerificationScheduleCursor retry_cursor_,
        UInt64 planned_target_count_,
        std::vector<AuthorityVerificationTargetCompletion> terminal_completions_,
        AuthorityVerificationTargetCost actual_charged_cost_) noexcept;

    const AuthorityRootIdentity root;
    const Digest target_set_digest;
    const UInt16 charge_abi = authority_verification_charge_abi;
    const AuthorityVerificationScheduleCursor retry_cursor;
    const UInt64 planned_target_count;
    const std::vector<AuthorityVerificationTargetCompletion> terminal_completions;
    const AuthorityVerificationTargetCost actual_charged_cost;
};

/// Sealed minting boundary for the concrete bounded exact-root executor. No
/// caller or subclass can issue a receipt from self-asserted completion data.
/// The executor must independently pin the supplied root, derive the complete
/// target-set digest, and prospectively enforce every declared cost bound. The
/// factory validates and adopts the executor-owned bounded completion vector;
/// it never duplicates caller-controlled completion storage.
class AuthorityVerificationBatchReceiptFactory final
{
private:
    friend class AuthorityVerificationBatchExecutor;

    AuthorityVerificationBatchReceiptFactory() = delete;

    [[nodiscard]] static AuthorityVerificationBatchReceipt::Ptr issue(
        const AuthorityVerificationBatchPlan & plan,
        const AuthorityRootIdentity & executed_root,
        const Digest & executed_target_set_digest,
        std::vector<AuthorityVerificationTargetCompletion> && ordered_terminal_completions,
        const AuthorityVerificationTargetCost & total_charged_cost);

    static void adoptVerifiedPrefix(
        const AuthorityVerificationBatchPlan & plan,
        const AuthorityVerificationBatchReceipt & prefix,
        std::vector<AuthorityVerificationTargetCompletion> & terminal_completions,
        AuthorityVerificationTargetCost & completed_cost);
};

enum class AuthorityVerificationPlanningStatus : UInt8
{
    InProgress,
    Complete,
};

/// Process-local resumable state for one immutable exact-root scheduling
/// decision. It retains only bounded indices/candidates and is invalidated if
/// the root, cursor, policy, limits, or backing snapshot changes.
class AuthorityVerificationPlanningContinuation final
{
public:
    using Ptr = std::unique_ptr<AuthorityVerificationPlanningContinuation>;

    AuthorityVerificationPlanningContinuation(const AuthorityVerificationPlanningContinuation &) = delete;
    AuthorityVerificationPlanningContinuation & operator=(const AuthorityVerificationPlanningContinuation &) = delete;
    ~AuthorityVerificationPlanningContinuation();

private:
    friend AuthorityVerificationPlanningResult resumePeriodicAuthorityVerificationPlanning(
        const AuthorityRoot &,
        std::span<const AuthorityVerificationTarget>,
        const AuthorityVerificationScheduleCursor &,
        const AuthorityVerificationSchedulePolicy &,
        const AuthorityVerificationScheduleLimits &,
        std::unique_ptr<AuthorityVerificationPlanningContinuation> &,
        const AuthorityVerificationPassBudget &);

    class Impl;
    explicit AuthorityVerificationPlanningContinuation(std::unique_ptr<Impl> impl_) noexcept;
    std::unique_ptr<Impl> impl;
};

struct AuthorityVerificationPlanningResult
{
    AuthorityVerificationPlanningStatus status = AuthorityVerificationPlanningStatus::InProgress;
    AuthorityVerificationBatchPlan::Ptr plan;
    /// Exact cooperative work items consumed by this resumable invocation.
    /// This lets a caller share one pass quantum with snapshot construction.
    UInt64 consumed_work_items = 0;
};

enum class AuthorityRepairReverificationBatchPlanningStatus : UInt8
{
    InProgress,
    Scheduled,
    Complete,
};

struct AuthorityRepairReverificationBatchPlanningResult
{
    AuthorityRepairReverificationBatchPlanningStatus status = AuthorityRepairReverificationBatchPlanningStatus::InProgress;
    AuthorityVerificationBatchPlan::Ptr plan;
    AuthorityVerificationScheduleCursor next_cursor;
    UInt64 next_target = 0;
    /// Exact cooperative work items consumed by setup and target selection in
    /// this invocation. The repair coordinator shares one quantum with
    /// snapshot construction and terminal execution.
    UInt64 consumed_work_items = 0;
};

/// Creates the only canonical initial cursor for a database and policy. A
/// bucket-count or seed change requires an explicit reset through this
/// function; a cursor from the old policy is never migrated implicitly.
[[nodiscard]] AuthorityVerificationScheduleCursor makeAuthorityVerificationScheduleCursor(
    const UUID & database_uuid,
    const AuthorityVerificationSchedulePolicy & policy = {},
    const AuthorityVerificationScheduleLimits & limits = {});

/// Builds one deterministic, bounded periodic batch. At least one rotation
/// target is selected whenever the snapshot is nonempty; optional priority
/// work can never consume that progress guarantee. The result performs no I/O,
/// verification, quarantine, repair, publication, sleeping or thread work.
[[nodiscard]] AuthorityVerificationBatchPlan::Ptr planPeriodicAuthorityVerification(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleCursor & cursor,
    const AuthorityVerificationSchedulePolicy & policy = {},
    const AuthorityVerificationScheduleLimits & limits = {});

/// Resumes one pure scheduling decision for at most the supplied cooperative
/// pass budget. A completed plan is returned exactly once and the continuation
/// is cleared; a normal budget yield preserves all validated progress.
[[nodiscard]] AuthorityVerificationPlanningResult resumePeriodicAuthorityVerificationPlanning(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleCursor & cursor,
    const AuthorityVerificationSchedulePolicy & policy,
    const AuthorityVerificationScheduleLimits & limits,
    std::unique_ptr<AuthorityVerificationPlanningContinuation> & continuation,
    const AuthorityVerificationPassBudget & pass_budget = {});

/// Partitions the complete exact inventory into deterministic, sequential
/// repair-release batches after validating the anchored target set once. No
/// priority sampling or rotating rescans are performed, so planning and
/// retained state remain O(number of inventory leaves), including at 100k.
/// Only sealed executor receipts for every returned target can authorize the
/// trusted release coordinator.
[[nodiscard]] std::vector<AuthorityVerificationBatchPlan::Ptr> planCompleteAuthorityRepairReverification(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleLimits & limits = {});

/// Plans the next contiguous batch of a complete repair re-verification.
/// The caller retains `sorted_unique_targets`, `next_cursor`, and
/// `next_target` across bounded runs. Every position is validated against the
/// pinned inventory exactly once before it can enter a sealed executor plan.
[[nodiscard]] AuthorityRepairReverificationBatchPlanningResult planNextAuthorityRepairReverificationBatch(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    UInt64 begin_target,
    const AuthorityVerificationScheduleCursor & retry_cursor,
    const AuthorityVerificationScheduleLimits & limits,
    const AuthorityVerificationPassBudget & pass_budget = {});

/// Produces the only cursor that may be persisted after executing a prepared
/// batch. Only a complete, ordered, clean receipt for the exact plan root and
/// target-set digest advances the cursor. A stale, partial or damaged receipt
/// returns the unadvanced retry cursor. This is pure validation: it performs
/// no verification, I/O, cursor persistence, or root publication.
[[nodiscard]] AuthorityVerificationCursorDecision
finalizePeriodicAuthorityVerificationBatch(const AuthorityVerificationBatchPlan & plan, const AuthorityVerificationBatchReceipt & receipt);

}
