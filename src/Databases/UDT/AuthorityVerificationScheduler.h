#pragma once

#include <Databases/UDT/AuthorityAutomaticRepair.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>

#include <Common/Logger.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string_view>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class DatabaseAtomic;

namespace UDT
{

struct AuthorityVerificationSchedulerLoadSnapshot
{
    UInt64 foreground_queries = 0;
    UInt64 competing_background_tasks = 0;
};

using AuthorityVerificationSchedulerLoadProbe = std::function<AuthorityVerificationSchedulerLoadSnapshot()>;

struct AuthorityVerificationSchedulerLimits
{
    AuthorityVerificationSchedulePolicy policy;
    AuthorityVerificationScheduleLimits schedule;
    AuthorityVerificationBatchExecutorLimits executor;
    AuthorityAutomaticRepairLimits automatic_repair;
    std::chrono::milliseconds successful_batch_interval{60'000};
    std::chrono::milliseconds retry_interval{5'000};
    std::chrono::milliseconds empty_root_probe_interval{300'000};
    std::chrono::milliseconds failure_backoff_interval{30'000};
    std::chrono::milliseconds snapshot_build_interval{100};
    std::chrono::milliseconds load_throttle_retry_interval{1'000};
    std::chrono::milliseconds maximum_run_wall_time{500};
    std::chrono::milliseconds maximum_run_cpu_time{250};
    /// Shared cooperative quantum for snapshot targets and resumable planner
    /// work items. Kept in the existing V2 override field for compatibility.
    UInt64 maximum_snapshot_targets_per_pass = 256;
    /// Zero means that any foreground query gets priority over verification.
    UInt64 maximum_foreground_queries_for_admission = 0;
    UInt64 maximum_competing_background_tasks_for_admission = 64;
    /// Positive nice values make the verifier best-effort lower priority on
    /// platforms where per-thread nice changes are supported.
    Int32 os_thread_nice_value = 10;
    /// Empty selects the production CurrentMetrics probe. Injection is closed
    /// to this immutable snapshot contract and cannot bypass the hard budgets.
    AuthorityVerificationSchedulerLoadProbe load_probe;
};

inline constexpr UInt16 authority_verification_scheduler_override_format_version = 2;
inline constexpr std::string_view authority_verification_scheduler_override_hash_domain
    = "ClickHouse UDT Atomic authority verification scheduler override V2";

/// Process-global admin limits plus an optional UUID-selected, canonical V2
/// per-database record. The encoded form contains no process-local probe,
/// cancellation token, or run deadline.
struct ResolvedAuthorityVerificationSchedulerConfiguration
{
    AuthorityVerificationSchedulerLimits global_limits;
    std::optional<String> encoded_database_override;
};

[[nodiscard]] ResolvedAuthorityVerificationSchedulerConfiguration
resolveAuthorityVerificationSchedulerConfigurationFromConfig(const Poco::Util::AbstractConfiguration & config, UUID database_uuid);
[[nodiscard]] String
encodeAuthorityVerificationSchedulerOverrideV2(UUID database_uuid, const AuthorityVerificationSchedulerLimits & override_limits);
[[nodiscard]] AuthorityVerificationSchedulerLimits
decodeAuthorityVerificationSchedulerOverrideV2(std::string_view bytes, UUID expected_database_uuid);
[[nodiscard]] AuthorityVerificationSchedulerLimits mergeAuthorityVerificationSchedulerLimits(
    const AuthorityVerificationSchedulerLimits & global_limits, const AuthorityVerificationSchedulerLimits & persisted_database_override);

enum class AuthorityVerificationSchedulerState : UInt8
{
    Dormant,
    Scheduled,
    BuildingSnapshot,
    Executing,
    EmptyRoot,
    Throttled,
    Backoff,
    Shutdown,
};

enum class AuthorityVerificationSchedulerThrottleReason : UInt8
{
    None,
    ForegroundLoad,
    BackgroundLoad,
    WallTimeBudget,
    CPUTimeBudget,
};

/// Stable, bounded and deliberately payload-free diagnostics. Raw exception
/// text can contain canonical SQL or normalized literals and must not enter
/// operator surfaces without the corresponding object-level SHOW checks.
enum class AuthorityVerificationSchedulerLastErrorKind : UInt8
{
    None,
    VerificationFailure,
    IntegrityDamageQuarantined,
    ExactRepairUnavailable,
    RuntimeFailClosed,
    RuntimeQuarantineConstructionFailed,
    StartupInvalid,
    StartupIncomplete,
    StartupConflicted,
};

struct AuthorityVerificationSchedulerStatus
{
    bool scheduler_status_available = false;
    bool verification_scheduler_override_configured = false;
    bool verification_scheduler_override_effective = false;
    bool verification_scheduler_override_persisted = false;
    bool database_resource_quota_override_configured = false;
    bool database_resource_quota_override_effective = false;
    bool database_resource_quota_override_persisted = false;
    AuthorityVerificationSchedulerState state = AuthorityVerificationSchedulerState::Dormant;
    UInt64 runs = 0;
    UInt64 cached_targets = 0;
    UInt64 planned_batches = 0;
    UInt64 planned_targets = 0;
    UInt64 terminal_targets = 0;
    UInt64 verified_targets = 0;
    UInt64 damaged_targets = 0;
    UInt64 cursor_advances = 0;
    UInt64 incomplete_batches = 0;
    UInt64 failures = 0;
    UInt64 repair_attempts = 0;
    UInt64 repair_successes = 0;
    UInt64 repair_unavailable = 0;
    UInt64 last_repair_transaction_id = 0;
    UInt64 last_repair_local_wal_sources = 0;
    UInt64 last_repair_replicated_authority_sources = 0;
    UInt64 last_repair_verified_backup_sources = 0;
    bool last_repair_provenance_available = false;
    UInt64 last_repair_damaged_artifacts = 0;
    Digest last_repair_damaged_artifact_manifest_digest{};
    UInt64 last_repair_previous_catalog_epoch = 0;
    Digest last_repair_previous_authority_anchor{};
    UInt64 last_repair_repaired_catalog_epoch = 0;
    Digest last_repair_repaired_authority_anchor{};
    UInt64 throttles = 0;
    UInt64 foreground_load_throttles = 0;
    UInt64 background_load_throttles = 0;
    UInt64 wall_time_budget_yields = 0;
    UInt64 cpu_time_budget_yields = 0;
    UInt64 last_observed_foreground_queries = 0;
    UInt64 last_observed_competing_background_tasks = 0;
    UInt64 last_root_catalog_epoch = 0;
    Digest last_root_authority_anchor{};
    UInt64 last_successful_root_catalog_epoch = 0;
    Digest last_successful_root_authority_anchor{};
    UInt64 last_completed_rotations = 0;
    UInt64 last_planned_batch_sequence = 0;
    UInt64 effective_maximum_snapshot_targets = 0;
    UInt64 effective_maximum_targets_per_batch = 0;
    UInt64 effective_maximum_buckets = 0;
    UInt64 effective_maximum_reverse_dependency_count = 0;
    UInt64 effective_maximum_canonical_bytes_per_batch = 0;
    UInt64 effective_maximum_work_units_per_batch = 0;
    UInt64 effective_maximum_transient_bytes_per_batch = 0;
    UInt64 effective_maximum_io_bytes_per_batch = 0;
    UInt64 effective_maximum_rooted_target_canonical_bytes = 0;
    UInt64 effective_maximum_rooted_target_work_units = 0;
    UInt64 effective_maximum_rooted_target_transient_bytes = 0;
    UInt64 effective_maximum_rooted_target_io_bytes = 0;
    UInt64 effective_maximum_planner_work_units = 0;
    UInt64 effective_maximum_planner_scratch_bytes = 0;
    UInt64 effective_maximum_planner_retained_bytes = 0;
    UInt64 effective_maximum_cooperative_work_items_per_pass = 0;
    UInt64 effective_maximum_run_wall_time_ms = 0;
    UInt64 effective_maximum_run_cpu_time_ms = 0;
    UInt64 effective_successful_batch_interval_ms = 0;
    UInt64 effective_load_throttle_retry_interval_ms = 0;
    UInt64 effective_maximum_foreground_queries_for_admission = 0;
    UInt64 effective_maximum_competing_background_tasks_for_admission = 0;
    Int32 effective_os_thread_nice_value = 0;
    UInt64 runtime_revision = 0;
    UInt64 quarantine_failing_seeds = 0;
    UInt64 quarantined_objects = 0;
    bool runtime_status_available = false;
    bool runtime_fail_closed = false;
    bool root_quota_status_available = false;
    bool root_quota_over_quota = false;
    UInt64 root_quota_revision = 0;
    UInt64 root_quota_definitions = 0;
    UInt64 root_quota_deterministic_catalog_bytes = 0;
    UInt64 root_quota_verification_targets = 0;
    UInt64 root_quota_verification_buckets = 0;
    UInt64 root_quota_verification_canonical_bytes = 0;
    UInt64 root_quota_verification_work_units = 0;
    UInt64 root_quota_verification_transient_bytes = 0;
    UInt64 root_quota_verification_io_bytes = 0;
    UInt64 root_quota_verification_planner_work_units = 0;
    UInt64 root_quota_verification_planner_scratch_bytes = 0;
    UInt64 root_quota_verification_retained_bytes = 0;
    UInt64 root_quota_durable_dependent_object_bytes = 0;
    UInt64 root_quota_limit_definitions = 0;
    UInt64 root_quota_limit_deterministic_catalog_bytes = 0;
    UInt64 root_quota_limit_verification_targets = 0;
    UInt64 root_quota_limit_verification_buckets = 0;
    UInt64 root_quota_limit_verification_canonical_bytes = 0;
    UInt64 root_quota_limit_verification_work_units = 0;
    UInt64 root_quota_limit_verification_transient_bytes = 0;
    UInt64 root_quota_limit_verification_io_bytes = 0;
    UInt64 root_quota_limit_verification_planner_work_units = 0;
    UInt64 root_quota_limit_verification_planner_scratch_bytes = 0;
    UInt64 root_quota_limit_verification_retained_bytes = 0;
    UInt64 root_quota_limit_durable_dependent_object_bytes = 0;
    UInt64 root_quota_limit_occurrence_paths_per_object = 0;
    UInt64 root_quota_limit_persisted_specializations_per_template = 0;
    UInt64 root_quota_limit_sidecar_bytes_per_object = 0;
    UInt64 root_quota_maximum_occurrence_paths_per_object = 0;
    UInt64 root_quota_maximum_persisted_specializations_per_template = 0;
    UInt64 root_quota_maximum_sidecar_bytes_per_object = 0;
    UInt64 root_usage_dependent_objects = 0;
    UInt64 root_usage_total_occurrence_paths = 0;
    UInt64 root_usage_unique_persisted_specializations = 0;
    AuthorityVerificationSchedulerThrottleReason last_throttle_reason = AuthorityVerificationSchedulerThrottleReason::None;
    AuthorityVerificationSchedulerLastErrorKind last_error_kind = AuthorityVerificationSchedulerLastErrorKind::None;
    Int32 last_error_code = 0;
};

/// One database-owned periodic verification consumer. The background task is
/// constructed unscheduled and starts only after DatabaseAtomic metadata
/// startup has completed. Snapshot, planning continuation, immutable plan and
/// sealed clean receipt prefix are all bound to one exact root. Normal budget
/// yields retain them; root/cursor changes or invalid evidence discard them.
class AuthorityVerificationScheduler final
{
public:
    explicit AuthorityVerificationScheduler(DB::DatabaseAtomic & database_, const AuthorityVerificationSchedulerLimits & limits_ = {});
    AuthorityVerificationScheduler(const AuthorityVerificationScheduler &) = delete;
    AuthorityVerificationScheduler & operator=(const AuthorityVerificationScheduler &) = delete;
    ~AuthorityVerificationScheduler();

    void activateAfterDatabaseStartup();
    void requestStop() noexcept;
    void shutdownAndDrain() noexcept;
    [[nodiscard]] AuthorityVerificationSchedulerStatus getStatus() const noexcept;
    [[nodiscard]] static AuthorityVerificationSchedulerLimits validateEffectiveLimits(AuthorityVerificationSchedulerLimits candidate);

private:
    void run() noexcept;
    void scheduleAfter(std::chrono::milliseconds delay) noexcept;
    void setState(AuthorityVerificationSchedulerState state_) noexcept;
    void recordLastError(AuthorityVerificationSchedulerLastErrorKind kind, Int32 code = 0) noexcept;
    void throttle(AuthorityVerificationSchedulerThrottleReason reason, std::chrono::milliseconds delay) noexcept;
    void invalidateCachedSnapshot() noexcept;

    DB::DatabaseAtomic & database;
    std::stop_source stop_source;
    const AuthorityVerificationSchedulerLimits limits;
    LoggerPtr log;
    const std::shared_ptr<AuthorityRepairReverificationContinuation> repair_reverification_continuation
        = std::make_shared<AuthorityRepairReverificationContinuation>();
    const std::shared_ptr<AuthorityAutomaticRepairContinuation> automatic_repair_continuation
        = std::make_shared<AuthorityAutomaticRepairContinuation>();

    std::mutex cache_mutex;
    std::optional<AuthorityRootIdentity> cached_root;
    std::vector<AuthorityVerificationTarget> cached_targets;
    std::vector<AuthorityVerificationTargetHistory> scheduling_history;
    std::vector<AuthorityVerificationTargetHistory> cached_history;
    UInt64 cached_build_offset = 0;
    bool cached_history_published = false;
    AuthorityVerificationPlanningContinuation::Ptr cached_planning;
    std::optional<AuthorityVerificationScheduleCursor> cached_planning_cursor;
    AuthorityVerificationBatchPlan::Ptr cached_plan;
    AuthorityVerificationBatchReceipt::Ptr cached_receipt;

    std::atomic<AuthorityVerificationSchedulerState> state{AuthorityVerificationSchedulerState::Dormant};
    std::atomic<UInt64> runs{0};
    std::atomic<UInt64> cached_target_count{0};
    std::atomic<UInt64> planned_batches{0};
    std::atomic<UInt64> planned_targets{0};
    std::atomic<UInt64> terminal_targets{0};
    std::atomic<UInt64> verified_targets{0};
    std::atomic<UInt64> damaged_targets{0};
    std::atomic<UInt64> cursor_advances{0};
    std::atomic<UInt64> incomplete_batches{0};
    std::atomic<UInt64> failures{0};
    std::atomic<UInt64> repair_attempts{0};
    std::atomic<UInt64> repair_successes{0};
    std::atomic<UInt64> repair_unavailable{0};
    std::atomic<UInt64> last_repair_transaction_id{0};
    std::atomic<UInt64> last_repair_local_wal_sources{0};
    std::atomic<UInt64> last_repair_replicated_authority_sources{0};
    std::atomic<UInt64> last_repair_verified_backup_sources{0};
    std::atomic<UInt64> throttles{0};
    std::atomic<UInt64> foreground_load_throttles{0};
    std::atomic<UInt64> background_load_throttles{0};
    std::atomic<UInt64> wall_time_budget_yields{0};
    std::atomic<UInt64> cpu_time_budget_yields{0};
    std::atomic<UInt64> last_observed_foreground_queries{0};
    std::atomic<UInt64> last_observed_competing_background_tasks{0};
    std::atomic<AuthorityVerificationSchedulerThrottleReason> last_throttle_reason{AuthorityVerificationSchedulerThrottleReason::None};
    /// Kind and numeric code form one exact lock-free diagnostic snapshot.
    /// Low 8 bits contain AuthorityVerificationSchedulerLastErrorKind; the
    /// following 32 bits contain the payload-free ClickHouse error code.
    std::atomic<UInt64> last_error{0};
    /// DatabaseAtomic may hold udt_authority_mutex while taking this mutex for
    /// a status snapshot. Scheduler code must therefore never retain it while
    /// entering a database-owned authority boundary.
    mutable std::mutex diagnostics_mutex;
    std::optional<AuthorityRootIdentity> last_observed_root;
    std::optional<AuthorityRootIdentity> last_successful_root;
    std::atomic<UInt64> last_completed_rotations{0};
    std::atomic<UInt64> last_planned_batch_sequence{0};
    std::atomic<bool> shutdown{false};
    /// Activation and draining are one cold lifecycle boundary. Serializing
    /// them prevents an activation which observed the old shutdown value from
    /// reactivating the task after its concurrent drain completed.
    std::mutex lifecycle_mutex;
    /// Destroy the holder before every callback-visible field, even if a future
    /// lifecycle regression were to leave a task queued at destruction.
    BackgroundSchedulePoolTaskHolder task;
};

}
}
