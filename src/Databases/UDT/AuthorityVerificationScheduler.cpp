#include <Databases/UDT/AuthorityVerificationScheduler.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <Interpreters/Context.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/OSThreadNiceValue.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <array>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <string>
#include <utility>

namespace CurrentMetrics
{
extern const Metric QueryNonInternal;
extern const Metric BackgroundMergesAndMutationsPoolTask;
extern const Metric BackgroundFetchesPoolTask;
extern const Metric BackgroundCommonPoolTask;
extern const Metric BackgroundMovePoolTask;
extern const Metric BackgroundSchedulePoolTask;
extern const Metric UDTAuthorityVerificationTask;
}

namespace ProfileEvents
{
extern const Event UDTAuthorityVerificationRuns;
extern const Event UDTAuthorityVerificationTargetsVerified;
extern const Event UDTAuthorityVerificationTargetsDamaged;
extern const Event UDTAuthorityVerificationFailures;
extern const Event UDTAuthorityVerificationForegroundLoadThrottles;
extern const Event UDTAuthorityVerificationBackgroundLoadThrottles;
extern const Event UDTAuthorityVerificationWallTimeYields;
extern const Event UDTAuthorityVerificationCPUTimeYields;
extern const Event UDTAuthorityAutomaticRepairAttempts;
extern const Event UDTAuthorityAutomaticRepairSuccesses;
extern const Event UDTAuthorityAutomaticRepairUnavailable;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char udt_authority_verification_pause_before_cache_invalidation[];
}
}

namespace DB::UDT
{
namespace
{

constexpr std::string_view scheduler_override_magic = "CHUDTVO2";
constexpr size_t scheduler_override_u64_fields = 33;
constexpr size_t scheduler_override_encoded_bytes = scheduler_override_magic.size() + sizeof(UInt16) + sizeof(CanonicalUUID)
    + scheduler_override_u64_fields * sizeof(UInt64) + sizeof(Digest);

void logVerificationFailureNoThrow(const LoggerPtr & log, UUID database_uuid, Int32 error_code) noexcept
{
    try
    {
        LOG_ERROR(
            log,
            "Atomic UDT verification failed for database UUID {} with error code {}; exception payload omitted",
            database_uuid,
            error_code);
    }
    catch (...)
    {
    }
}

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUUID(String & output, UUID value)
{
    const auto canonical = uuidToCanonicalBytes(value);
    output.append(reinterpret_cast<const char *>(canonical.data()), canonical.size());
}

void appendDigest(String & output, const Digest & digest)
{
    output.append(reinterpret_cast<const char *>(digest.data()), digest.size());
}

class SchedulerOverrideReader final
{
public:
    explicit SchedulerOverrideReader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    std::string_view readBytes(size_t size)
    {
        if (size > bytes.size() - position)
            throw AuthorityVerificationScheduleError(
                AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                "persisted authority verification scheduler override is truncated");
        const auto result = bytes.substr(position, size);
        position += size;
        return result;
    }

    UInt16 readUInt16LE()
    {
        const auto value = readBytes(sizeof(UInt16));
        return static_cast<UInt16>(static_cast<UInt8>(value[0]) | (static_cast<UInt16>(static_cast<UInt8>(value[1])) << 8));
    }

    UInt64 readUInt64LE()
    {
        const auto value = readBytes(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(value[index])) << (8 * index);
        return result;
    }

    UUID readUUID()
    {
        const auto value = readBytes(sizeof(CanonicalUUID));
        CanonicalUUID canonical{};
        std::copy(value.begin(), value.end(), reinterpret_cast<char *>(canonical.data()));
        return uuidFromCanonicalBytes(canonical);
    }

    Digest readDigest()
    {
        const auto value = readBytes(sizeof(Digest));
        Digest digest{};
        std::copy(value.begin(), value.end(), reinterpret_cast<char *>(digest.data()));
        return digest;
    }

    bool atEnd() const noexcept { return position == bytes.size(); }

private:
    std::string_view bytes;
    size_t position = 0;
};

AuthorityRootIdentity identifyRoot(const AuthorityRoot & root)
{
    const auto & state = root.getAuthorityState();
    return {
        .database_uuid = state.database_uuid,
        .database_catalog_epoch = state.database_catalog_epoch,
        .authority_anchor = state.anchor_hash,
    };
}

bool cursorMatchesPolicy(
    const AuthorityVerificationScheduleCursor & cursor, const AuthorityVerificationSchedulePolicy & policy, UUID database_uuid) noexcept
{
    return cursor.contract_abi == authority_verification_schedule_contract_abi && cursor.database_uuid == database_uuid
        && cursor.bucket_count == policy.bucket_count && cursor.bucket_seed == policy.bucket_seed;
}

UInt64 nonnegativeMetric(CurrentMetrics::Metric metric) noexcept
{
    return static_cast<UInt64>(std::max<Int64>(0, CurrentMetrics::get(metric)));
}

UInt64 saturatingAdd(UInt64 lhs, UInt64 rhs) noexcept
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        return std::numeric_limits<UInt64>::max();
    return lhs + rhs;
}

AuthorityVerificationSchedulerLoadSnapshot probeProductionLoad() noexcept
{
    UInt64 background = nonnegativeMetric(CurrentMetrics::BackgroundMergesAndMutationsPoolTask);
    background = saturatingAdd(background, nonnegativeMetric(CurrentMetrics::BackgroundFetchesPoolTask));
    background = saturatingAdd(background, nonnegativeMetric(CurrentMetrics::BackgroundCommonPoolTask));
    background = saturatingAdd(background, nonnegativeMetric(CurrentMetrics::BackgroundMovePoolTask));
    const UInt64 schedule_pool = nonnegativeMetric(CurrentMetrics::BackgroundSchedulePoolTask);
    /// The current verifier task itself is included in this metric.
    background = saturatingAdd(background, schedule_pool == 0 ? 0 : schedule_pool - 1);
    return {
        .foreground_queries = nonnegativeMetric(CurrentMetrics::QueryNonInternal),
        .competing_background_tasks = background,
    };
}

struct SchedulerRunBudget
{
    explicit SchedulerRunBudget(const AuthorityVerificationSchedulerLimits & limits)
        : wall_deadline(std::chrono::steady_clock::now() + limits.maximum_run_wall_time)
        , cpu_deadline_nanoseconds(
              clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) + static_cast<UInt64>(limits.maximum_run_cpu_time.count()) * 1'000'000ULL)
    {
    }

    bool wallExpired() const noexcept { return std::chrono::steady_clock::now() >= wall_deadline; }
    bool cpuExpired() const { return clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= cpu_deadline_nanoseconds; }

    std::chrono::steady_clock::time_point wall_deadline;
    UInt64 cpu_deadline_nanoseconds;
};

void validateSchedulerLimits(AuthorityVerificationSchedulerLimits & limits)
{
    static constexpr auto maximum_implementation_run_time = std::chrono::hours(1);
    static constexpr UInt64 required_object_verifier_canonical_bytes = 256ULL << 20;
    static constexpr UInt64 required_object_verifier_work_units = 8'388'608;
    static constexpr UInt64 required_object_verifier_transient_bytes = 64ULL << 20;
    if (limits.successful_batch_interval.count() <= 0 || limits.retry_interval.count() <= 0 || limits.empty_root_probe_interval.count() <= 0
        || limits.failure_backoff_interval.count() <= 0 || limits.snapshot_build_interval.count() <= 0
        || limits.load_throttle_retry_interval.count() <= 0 || limits.maximum_run_wall_time.count() <= 0
        || limits.maximum_run_cpu_time.count() <= 0)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration, "authority verification scheduler intervals must be positive");
    if (limits.maximum_snapshot_targets_per_pass == 0 || limits.maximum_snapshot_targets_per_pass > limits.schedule.maximum_snapshot_targets
        || !std::in_range<size_t>(limits.maximum_snapshot_targets_per_pass))
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification scheduler snapshot pass limit is outside the configured target domain");
    if (limits.maximum_competing_background_tasks_for_admission == 0 || limits.os_thread_nice_value <= 0 || limits.os_thread_nice_value > 19
        || limits.maximum_run_wall_time > maximum_implementation_run_time || limits.maximum_run_cpu_time > maximum_implementation_run_time
        || !std::in_range<size_t>(limits.successful_batch_interval.count()) || !std::in_range<size_t>(limits.retry_interval.count())
        || !std::in_range<size_t>(limits.empty_root_probe_interval.count())
        || !std::in_range<size_t>(limits.failure_backoff_interval.count()) || !std::in_range<size_t>(limits.snapshot_build_interval.count())
        || !std::in_range<size_t>(limits.load_throttle_retry_interval.count())
        || static_cast<UInt64>(limits.maximum_run_cpu_time.count()) > std::numeric_limits<UInt64>::max() / 1'000'000ULL)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification scheduler throttle limits are outside the implementation domain");
    /// Admission/startup mints every live verification stamp through the
    /// default integrity verifier before durable publication. Periodic
    /// execution must use exactly that 256 MiB / 8,388,608 / 64 MiB object
    /// domain; the database's larger deterministic-catalog quota is aggregate
    /// capacity, not permission to admit a larger single verification closure.
    if (limits.executor.object_verifier.maximum_canonical_bytes_hashed != required_object_verifier_canonical_bytes
        || limits.executor.object_verifier.maximum_work_units != required_object_verifier_work_units
        || limits.executor.object_verifier.maximum_transient_bytes != required_object_verifier_transient_bytes)
    {
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification per-object limits must exactly match the durable admission-verifier domain");
    }
    if (limits.executor.maximum_terminal_targets == 0 || limits.executor.maximum_terminal_targets > 1'024)
    {
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification executor terminal-target limit is outside the implementation domain");
    }
    const auto prospective_planning = computeAuthorityVerificationPlanningRequirements(
        limits.schedule.maximum_snapshot_targets, limits.policy, limits.schedule.maximum_targets_per_batch);
    if (prospective_planning.planner_work_units > limits.schedule.maximum_planner_work_units
        || prospective_planning.planner_scratch_bytes > limits.schedule.maximum_planner_scratch_bytes
        || prospective_planning.retained_canonical_bytes > limits.schedule.maximum_retained_canonical_bytes)
    {
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification policy cannot plan its complete admitted target domain within the configured limits");
    }
    if (!limits.load_probe)
        limits.load_probe = probeProductionLoad;

    /// Validate the complete pure scheduling domain without materializing a
    /// root. The canonical cursor constructor covers policy/limit coupling.
    static_cast<void>(makeAuthorityVerificationScheduleCursor(UUIDHelpers::generateV4(), limits.policy, limits.schedule));
    validateAuthorityIntegrityVerifierLimits(limits.executor.object_verifier);
}

}

AuthorityVerificationScheduler::AuthorityVerificationScheduler(
    DB::DatabaseAtomic & database_, const AuthorityVerificationSchedulerLimits & limits_)
    : database(database_)
    , limits(
          [&]
          {
              auto validated = limits_;
              validated.executor.cancellation = stop_source.get_token();
              validated.automatic_repair.cancellation = stop_source.get_token();
              validated.automatic_repair.execution.verification_executor.cancellation = stop_source.get_token();
              return validateEffectiveLimits(std::move(validated));
          }())
    , log(getLogger("AtomicUDTVerification"))
    , task(database_.getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "AtomicUDTVerification", [this] { run(); }))
{
    /// A newly created task is active but unscheduled and therefore cannot
    /// execute until database startup explicitly activates this scheduler.
}

ResolvedAuthorityVerificationSchedulerConfiguration
resolveAuthorityVerificationSchedulerConfigurationFromConfig(const Poco::Util::AbstractConfiguration & config, UUID database_uuid)
{
    if (database_uuid == UUIDHelpers::Nil)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "authority verification scheduler database override selector UUID is nil");

    const auto validate_group = [&](const String & prefix, std::initializer_list<std::string_view> allowed)
    {
        if (!config.has(prefix))
            return;
        std::vector<String> keys;
        config.keys(prefix, keys);
        for (const auto & key : keys)
        {
            if (std::none_of(allowed.begin(), allowed.end(), [&](std::string_view candidate) { return key == candidate; }))
            {
                throw AuthorityVerificationScheduleError(
                    AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                    "authority verification scheduler configuration contains unknown key " + key + " under " + prefix);
            }
        }
    };
    const auto validate_limits_tree = [&](const String & prefix, bool allow_database_overrides)
    {
        validate_group(
            prefix,
            allow_database_overrides
                ? std::initializer_list<std::string_view>{"policy", "limits", "schedule", "throttle", "repair", "database_overrides"}
                : std::initializer_list<std::string_view>{"policy", "limits", "schedule", "throttle", "repair"});
        validate_group(
            prefix + ".policy",
            {"bucket_count",
             "bucket_seed",
             "recent_catalog_epoch_window",
             "high_dependency_threshold",
             "maximum_recent_targets_per_batch",
             "maximum_high_dependency_targets_per_batch",
             "random_targets_per_batch"});
        validate_group(
            prefix + ".limits",
            {"maximum_snapshot_targets",
             "maximum_targets_per_batch",
             "maximum_buckets",
             "maximum_reverse_dependency_count",
             "maximum_canonical_bytes_per_batch",
             "maximum_verification_work_units_per_batch",
             "maximum_transient_bytes_per_batch",
             "maximum_io_bytes_per_batch",
             "maximum_planner_work_units",
             "maximum_planner_scratch_bytes",
             "maximum_retained_canonical_bytes",
             "maximum_run_wall_time_ms",
             "maximum_run_cpu_time_ms",
             "maximum_snapshot_targets_per_pass"});
        validate_group(
            prefix + ".schedule",
            {"successful_batch_interval_ms",
             "retry_interval_ms",
             "empty_root_probe_interval_ms",
             "failure_backoff_interval_ms",
             "snapshot_build_interval_ms",
             "load_throttle_retry_interval_ms"});
        validate_group(prefix + ".throttle", {"maximum_foreground_queries", "maximum_competing_background_tasks", "os_thread_nice_value"});
        validate_group(
            prefix + ".repair",
            {"maximum_local_wal_transactions", "maximum_local_wal_artifacts_examined", "maximum_local_wal_bytes_examined"});
    };

    const auto read_limits = [&](const String & prefix, AuthorityVerificationSchedulerLimits limits)
    {
        const auto key = [&](std::string_view suffix) { return prefix + "." + String(suffix); };
        const auto get_u64 = [&](std::string_view suffix, UInt64 fallback) { return config.getUInt64(key(suffix), fallback); };
        const auto get_ms = [&](std::string_view suffix, std::chrono::milliseconds fallback)
        {
            const UInt64 value = get_u64(suffix, static_cast<UInt64>(fallback.count()));
            if (!std::in_range<Int64>(value))
                throw AuthorityVerificationScheduleError(
                    AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                    "authority verification scheduler millisecond interval exceeds Int64");
            return std::chrono::milliseconds(static_cast<Int64>(value));
        };

        const UInt64 bucket_count = get_u64("policy.bucket_count", limits.policy.bucket_count);
        if (!std::in_range<UInt32>(bucket_count))
            throw AuthorityVerificationScheduleError(
                AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                "authority verification scheduler bucket count exceeds UInt32");
        limits.policy.bucket_count = static_cast<UInt32>(bucket_count);
        limits.policy.bucket_seed = get_u64("policy.bucket_seed", limits.policy.bucket_seed);
        limits.policy.recent_catalog_epoch_window
            = get_u64("policy.recent_catalog_epoch_window", limits.policy.recent_catalog_epoch_window);
        limits.policy.high_dependency_threshold = get_u64("policy.high_dependency_threshold", limits.policy.high_dependency_threshold);
        limits.policy.maximum_recent_targets_per_batch
            = get_u64("policy.maximum_recent_targets_per_batch", limits.policy.maximum_recent_targets_per_batch);
        limits.policy.maximum_high_dependency_targets_per_batch
            = get_u64("policy.maximum_high_dependency_targets_per_batch", limits.policy.maximum_high_dependency_targets_per_batch);
        limits.policy.random_targets_per_batch = get_u64("policy.random_targets_per_batch", limits.policy.random_targets_per_batch);

        limits.schedule.maximum_snapshot_targets = get_u64("limits.maximum_snapshot_targets", limits.schedule.maximum_snapshot_targets);
        limits.schedule.maximum_targets_per_batch = get_u64("limits.maximum_targets_per_batch", limits.schedule.maximum_targets_per_batch);
        limits.schedule.maximum_buckets = get_u64("limits.maximum_buckets", limits.schedule.maximum_buckets);
        limits.schedule.maximum_reverse_dependency_count
            = get_u64("limits.maximum_reverse_dependency_count", limits.schedule.maximum_reverse_dependency_count);
        limits.schedule.maximum_canonical_bytes_per_batch
            = get_u64("limits.maximum_canonical_bytes_per_batch", limits.schedule.maximum_canonical_bytes_per_batch);
        limits.schedule.maximum_verification_work_units_per_batch
            = get_u64("limits.maximum_verification_work_units_per_batch", limits.schedule.maximum_verification_work_units_per_batch);
        limits.schedule.maximum_transient_bytes_per_batch
            = get_u64("limits.maximum_transient_bytes_per_batch", limits.schedule.maximum_transient_bytes_per_batch);
        limits.schedule.maximum_io_bytes_per_batch
            = get_u64("limits.maximum_io_bytes_per_batch", limits.schedule.maximum_io_bytes_per_batch);
        limits.schedule.maximum_planner_work_units
            = get_u64("limits.maximum_planner_work_units", limits.schedule.maximum_planner_work_units);
        limits.schedule.maximum_planner_scratch_bytes
            = get_u64("limits.maximum_planner_scratch_bytes", limits.schedule.maximum_planner_scratch_bytes);
        limits.schedule.maximum_retained_canonical_bytes
            = get_u64("limits.maximum_retained_canonical_bytes", limits.schedule.maximum_retained_canonical_bytes);

        limits.successful_batch_interval = get_ms("schedule.successful_batch_interval_ms", limits.successful_batch_interval);
        limits.retry_interval = get_ms("schedule.retry_interval_ms", limits.retry_interval);
        limits.empty_root_probe_interval = get_ms("schedule.empty_root_probe_interval_ms", limits.empty_root_probe_interval);
        limits.failure_backoff_interval = get_ms("schedule.failure_backoff_interval_ms", limits.failure_backoff_interval);
        limits.snapshot_build_interval = get_ms("schedule.snapshot_build_interval_ms", limits.snapshot_build_interval);
        limits.load_throttle_retry_interval = get_ms("schedule.load_throttle_retry_interval_ms", limits.load_throttle_retry_interval);
        limits.maximum_run_wall_time = get_ms("limits.maximum_run_wall_time_ms", limits.maximum_run_wall_time);
        limits.maximum_run_cpu_time = get_ms("limits.maximum_run_cpu_time_ms", limits.maximum_run_cpu_time);
        limits.maximum_snapshot_targets_per_pass
            = get_u64("limits.maximum_snapshot_targets_per_pass", limits.maximum_snapshot_targets_per_pass);
        limits.maximum_foreground_queries_for_admission
            = get_u64("throttle.maximum_foreground_queries", limits.maximum_foreground_queries_for_admission);
        limits.maximum_competing_background_tasks_for_admission
            = get_u64("throttle.maximum_competing_background_tasks", limits.maximum_competing_background_tasks_for_admission);
        limits.os_thread_nice_value = config.getInt(key("throttle.os_thread_nice_value"), limits.os_thread_nice_value);

        limits.automatic_repair.maximum_local_wal_transactions
            = get_u64("repair.maximum_local_wal_transactions", limits.automatic_repair.maximum_local_wal_transactions);
        limits.automatic_repair.maximum_local_wal_artifacts_examined
            = get_u64("repair.maximum_local_wal_artifacts_examined", limits.automatic_repair.maximum_local_wal_artifacts_examined);
        limits.automatic_repair.maximum_local_wal_bytes_examined
            = get_u64("repair.maximum_local_wal_bytes_examined", limits.automatic_repair.maximum_local_wal_bytes_examined);

        return AuthorityVerificationScheduler::validateEffectiveLimits(std::move(limits));
    };

    static const String global_prefix = "user_defined_types.authority_verification";
    validate_limits_tree(global_prefix, true);
    auto global = read_limits(global_prefix, {});
    std::optional<String> encoded_override;
    const String overrides_root = global_prefix + ".database_overrides";
    if (config.has(overrides_root))
    {
        std::vector<String> selectors;
        config.keys(overrides_root, selectors);
        for (const auto & selector : selectors)
        {
            UUID parsed = UUIDHelpers::Nil;
            if (!tryParseUUID({reinterpret_cast<const UInt8 *>(selector.data()), selector.size()}, parsed) || toString(parsed) != selector)
            {
                throw AuthorityVerificationScheduleError(
                    AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                    "authority verification database override selector must be one canonical database UUID");
            }
            const String override_prefix = overrides_root + "." + selector;
            validate_limits_tree(override_prefix, false);
            /// Persist the database layer independently from the current
            /// process-global policy. Otherwise every unspecified field would
            /// capture today's global value, making a later global-only
            /// tightening/relaxation either a false byte mismatch or a frozen
            /// implicit database override. Effective limits are merged below
            /// only after this complete canonical database layer is decoded.
            auto requested = read_limits(override_prefix, {});
            if (selector == toString(database_uuid))
                encoded_override = encodeAuthorityVerificationSchedulerOverrideV2(database_uuid, requested);
        }
    }
    return {.global_limits = std::move(global), .encoded_database_override = std::move(encoded_override)};
}

String encodeAuthorityVerificationSchedulerOverrideV2(UUID database_uuid, const AuthorityVerificationSchedulerLimits & override_limits)
{
    if (database_uuid == UUIDHelpers::Nil)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler override database UUID is nil");

    auto limits = AuthorityVerificationScheduler::validateEffectiveLimits(override_limits);
    String result;
    result.reserve(scheduler_override_encoded_bytes);
    result.append(scheduler_override_magic);
    appendUInt16LE(result, authority_verification_scheduler_override_format_version);
    appendUUID(result, database_uuid);

    appendUInt64LE(result, limits.policy.bucket_count);
    appendUInt64LE(result, limits.policy.bucket_seed);
    appendUInt64LE(result, limits.policy.recent_catalog_epoch_window);
    appendUInt64LE(result, limits.policy.high_dependency_threshold);
    appendUInt64LE(result, limits.policy.maximum_recent_targets_per_batch);
    appendUInt64LE(result, limits.policy.maximum_high_dependency_targets_per_batch);
    appendUInt64LE(result, limits.policy.random_targets_per_batch);

    appendUInt64LE(result, limits.schedule.maximum_snapshot_targets);
    appendUInt64LE(result, limits.schedule.maximum_targets_per_batch);
    appendUInt64LE(result, limits.schedule.maximum_buckets);
    appendUInt64LE(result, limits.schedule.maximum_reverse_dependency_count);
    appendUInt64LE(result, limits.schedule.maximum_canonical_bytes_per_batch);
    appendUInt64LE(result, limits.schedule.maximum_verification_work_units_per_batch);
    appendUInt64LE(result, limits.schedule.maximum_transient_bytes_per_batch);
    appendUInt64LE(result, limits.schedule.maximum_io_bytes_per_batch);
    appendUInt64LE(result, limits.schedule.maximum_planner_work_units);
    appendUInt64LE(result, limits.schedule.maximum_planner_scratch_bytes);
    appendUInt64LE(result, limits.schedule.maximum_retained_canonical_bytes);

    appendUInt64LE(result, static_cast<UInt64>(limits.successful_batch_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.retry_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.empty_root_probe_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.failure_backoff_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.snapshot_build_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.load_throttle_retry_interval.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.maximum_run_wall_time.count()));
    appendUInt64LE(result, static_cast<UInt64>(limits.maximum_run_cpu_time.count()));
    appendUInt64LE(result, limits.maximum_snapshot_targets_per_pass);
    appendUInt64LE(result, limits.maximum_foreground_queries_for_admission);
    appendUInt64LE(result, limits.maximum_competing_background_tasks_for_admission);
    appendUInt64LE(result, static_cast<UInt64>(limits.os_thread_nice_value));

    appendUInt64LE(result, limits.automatic_repair.maximum_local_wal_transactions);
    appendUInt64LE(result, limits.automatic_repair.maximum_local_wal_artifacts_examined);
    appendUInt64LE(result, limits.automatic_repair.maximum_local_wal_bytes_examined);

    if (result.size() + sizeof(Digest) != scheduler_override_encoded_bytes)
        std::terminate();
    appendDigest(result, hashDomainSeparated(authority_verification_scheduler_override_hash_domain, result));
    return result;
}

AuthorityVerificationSchedulerLimits decodeAuthorityVerificationSchedulerOverrideV2(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil || bytes.size() != scheduler_override_encoded_bytes)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler override has an invalid identity or size");

    SchedulerOverrideReader reader(bytes);
    if (reader.readBytes(scheduler_override_magic.size()) != scheduler_override_magic
        || reader.readUInt16LE() != authority_verification_scheduler_override_format_version || reader.readUUID() != expected_database_uuid)
    {
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler override has an incompatible format or database identity");
    }

    AuthorityVerificationSchedulerLimits limits;
    const UInt64 bucket_count = reader.readUInt64LE();
    if (!std::in_range<UInt32>(bucket_count))
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler bucket count exceeds UInt32");
    limits.policy.bucket_count = static_cast<UInt32>(bucket_count);
    limits.policy.bucket_seed = reader.readUInt64LE();
    limits.policy.recent_catalog_epoch_window = reader.readUInt64LE();
    limits.policy.high_dependency_threshold = reader.readUInt64LE();
    limits.policy.maximum_recent_targets_per_batch = reader.readUInt64LE();
    limits.policy.maximum_high_dependency_targets_per_batch = reader.readUInt64LE();
    limits.policy.random_targets_per_batch = reader.readUInt64LE();

    limits.schedule.maximum_snapshot_targets = reader.readUInt64LE();
    limits.schedule.maximum_targets_per_batch = reader.readUInt64LE();
    limits.schedule.maximum_buckets = reader.readUInt64LE();
    limits.schedule.maximum_reverse_dependency_count = reader.readUInt64LE();
    limits.schedule.maximum_canonical_bytes_per_batch = reader.readUInt64LE();
    limits.schedule.maximum_verification_work_units_per_batch = reader.readUInt64LE();
    limits.schedule.maximum_transient_bytes_per_batch = reader.readUInt64LE();
    limits.schedule.maximum_io_bytes_per_batch = reader.readUInt64LE();
    limits.schedule.maximum_planner_work_units = reader.readUInt64LE();
    limits.schedule.maximum_planner_scratch_bytes = reader.readUInt64LE();
    limits.schedule.maximum_retained_canonical_bytes = reader.readUInt64LE();

    const auto read_milliseconds = [&]()
    {
        const UInt64 value = reader.readUInt64LE();
        if (!std::in_range<Int64>(value))
            throw AuthorityVerificationScheduleError(
                AuthorityVerificationScheduleError::Code::InvalidConfiguration,
                "persisted authority verification scheduler interval exceeds Int64");
        return std::chrono::milliseconds(static_cast<Int64>(value));
    };
    limits.successful_batch_interval = read_milliseconds();
    limits.retry_interval = read_milliseconds();
    limits.empty_root_probe_interval = read_milliseconds();
    limits.failure_backoff_interval = read_milliseconds();
    limits.snapshot_build_interval = read_milliseconds();
    limits.load_throttle_retry_interval = read_milliseconds();
    limits.maximum_run_wall_time = read_milliseconds();
    limits.maximum_run_cpu_time = read_milliseconds();
    limits.maximum_snapshot_targets_per_pass = reader.readUInt64LE();
    limits.maximum_foreground_queries_for_admission = reader.readUInt64LE();
    limits.maximum_competing_background_tasks_for_admission = reader.readUInt64LE();
    const UInt64 nice_value = reader.readUInt64LE();
    if (!std::in_range<Int32>(nice_value))
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler nice value exceeds Int32");
    limits.os_thread_nice_value = static_cast<Int32>(nice_value);

    limits.automatic_repair.maximum_local_wal_transactions = reader.readUInt64LE();
    limits.automatic_repair.maximum_local_wal_artifacts_examined = reader.readUInt64LE();
    limits.automatic_repair.maximum_local_wal_bytes_examined = reader.readUInt64LE();

    const Digest persisted_hash = reader.readDigest();
    if (!reader.atEnd()
        || persisted_hash
            != hashDomainSeparated(authority_verification_scheduler_override_hash_domain, bytes.substr(0, bytes.size() - sizeof(Digest))))
    {
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler override checksum is invalid");
    }

    limits = AuthorityVerificationScheduler::validateEffectiveLimits(std::move(limits));
    if (encodeAuthorityVerificationSchedulerOverrideV2(expected_database_uuid, limits) != bytes)
        throw AuthorityVerificationScheduleError(
            AuthorityVerificationScheduleError::Code::InvalidConfiguration,
            "persisted authority verification scheduler override is not canonical");
    return limits;
}

AuthorityVerificationSchedulerLimits mergeAuthorityVerificationSchedulerLimits(
    const AuthorityVerificationSchedulerLimits & global_limits, const AuthorityVerificationSchedulerLimits & persisted_database_override)
{
    const auto global = AuthorityVerificationScheduler::validateEffectiveLimits(global_limits);
    const auto persisted = AuthorityVerificationScheduler::validateEffectiveLimits(persisted_database_override);
    auto result = global;

    /// Bucket topology is durable policy identity. All remaining choices use
    /// the lower-load/minimum-resource side of both admin layers.
    result.policy.bucket_count = persisted.policy.bucket_count;
    result.policy.bucket_seed = persisted.policy.bucket_seed;
    result.policy.recent_catalog_epoch_window
        = std::min(global.policy.recent_catalog_epoch_window, persisted.policy.recent_catalog_epoch_window);
    result.policy.high_dependency_threshold = std::max(global.policy.high_dependency_threshold, persisted.policy.high_dependency_threshold);
    result.policy.maximum_recent_targets_per_batch
        = std::min(global.policy.maximum_recent_targets_per_batch, persisted.policy.maximum_recent_targets_per_batch);
    result.policy.maximum_high_dependency_targets_per_batch
        = std::min(global.policy.maximum_high_dependency_targets_per_batch, persisted.policy.maximum_high_dependency_targets_per_batch);
    result.policy.random_targets_per_batch = std::min(global.policy.random_targets_per_batch, persisted.policy.random_targets_per_batch);

    result.schedule.maximum_snapshot_targets
        = std::min(global.schedule.maximum_snapshot_targets, persisted.schedule.maximum_snapshot_targets);
    result.schedule.maximum_targets_per_batch
        = std::min(global.schedule.maximum_targets_per_batch, persisted.schedule.maximum_targets_per_batch);
    result.schedule.maximum_buckets = std::min(global.schedule.maximum_buckets, persisted.schedule.maximum_buckets);
    result.schedule.maximum_reverse_dependency_count
        = std::min(global.schedule.maximum_reverse_dependency_count, persisted.schedule.maximum_reverse_dependency_count);
    result.schedule.maximum_canonical_bytes_per_batch
        = std::min(global.schedule.maximum_canonical_bytes_per_batch, persisted.schedule.maximum_canonical_bytes_per_batch);
    result.schedule.maximum_verification_work_units_per_batch
        = std::min(global.schedule.maximum_verification_work_units_per_batch, persisted.schedule.maximum_verification_work_units_per_batch);
    result.schedule.maximum_transient_bytes_per_batch
        = std::min(global.schedule.maximum_transient_bytes_per_batch, persisted.schedule.maximum_transient_bytes_per_batch);
    result.schedule.maximum_io_bytes_per_batch
        = std::min(global.schedule.maximum_io_bytes_per_batch, persisted.schedule.maximum_io_bytes_per_batch);
    result.schedule.maximum_planner_work_units
        = std::min(global.schedule.maximum_planner_work_units, persisted.schedule.maximum_planner_work_units);
    result.schedule.maximum_planner_scratch_bytes
        = std::min(global.schedule.maximum_planner_scratch_bytes, persisted.schedule.maximum_planner_scratch_bytes);
    result.schedule.maximum_retained_canonical_bytes
        = std::min(global.schedule.maximum_retained_canonical_bytes, persisted.schedule.maximum_retained_canonical_bytes);

    result.successful_batch_interval = std::max(global.successful_batch_interval, persisted.successful_batch_interval);
    result.retry_interval = std::max(global.retry_interval, persisted.retry_interval);
    result.empty_root_probe_interval = std::max(global.empty_root_probe_interval, persisted.empty_root_probe_interval);
    result.failure_backoff_interval = std::max(global.failure_backoff_interval, persisted.failure_backoff_interval);
    result.snapshot_build_interval = std::max(global.snapshot_build_interval, persisted.snapshot_build_interval);
    result.load_throttle_retry_interval = std::max(global.load_throttle_retry_interval, persisted.load_throttle_retry_interval);
    result.maximum_run_wall_time = std::min(global.maximum_run_wall_time, persisted.maximum_run_wall_time);
    result.maximum_run_cpu_time = std::min(global.maximum_run_cpu_time, persisted.maximum_run_cpu_time);
    result.maximum_snapshot_targets_per_pass
        = std::min(global.maximum_snapshot_targets_per_pass, persisted.maximum_snapshot_targets_per_pass);
    result.maximum_foreground_queries_for_admission
        = std::min(global.maximum_foreground_queries_for_admission, persisted.maximum_foreground_queries_for_admission);
    result.maximum_competing_background_tasks_for_admission
        = std::min(global.maximum_competing_background_tasks_for_admission, persisted.maximum_competing_background_tasks_for_admission);
    result.os_thread_nice_value = std::max(global.os_thread_nice_value, persisted.os_thread_nice_value);

    result.automatic_repair.maximum_local_wal_transactions
        = std::min(global.automatic_repair.maximum_local_wal_transactions, persisted.automatic_repair.maximum_local_wal_transactions);
    result.automatic_repair.maximum_local_wal_artifacts_examined = std::min(
        global.automatic_repair.maximum_local_wal_artifacts_examined, persisted.automatic_repair.maximum_local_wal_artifacts_examined);
    result.automatic_repair.maximum_local_wal_bytes_examined
        = std::min(global.automatic_repair.maximum_local_wal_bytes_examined, persisted.automatic_repair.maximum_local_wal_bytes_examined);
    return AuthorityVerificationScheduler::validateEffectiveLimits(std::move(result));
}

AuthorityVerificationSchedulerLimits AuthorityVerificationScheduler::validateEffectiveLimits(AuthorityVerificationSchedulerLimits candidate)
{
    validateSchedulerLimits(candidate);
    return candidate;
}

AuthorityVerificationScheduler::~AuthorityVerificationScheduler()
{
    shutdownAndDrain();
}

void AuthorityVerificationScheduler::activateAfterDatabaseStartup()
{
    std::lock_guard lock(lifecycle_mutex);
    if (shutdown.load(std::memory_order_acquire) || stop_source.stop_requested())
        return;
    setState(AuthorityVerificationSchedulerState::Scheduled);
    task->activateAndSchedule();
}

void AuthorityVerificationScheduler::requestStop() noexcept
{
    static_cast<void>(stop_source.request_stop());
}

void AuthorityVerificationScheduler::shutdownAndDrain() noexcept
{
    {
        std::lock_guard lock(lifecycle_mutex);
        if (!shutdown.exchange(true, std::memory_order_acq_rel))
            requestStop();
    }

    /// BackgroundSchedulePool keeps its execution mutex while invoking run(),
    /// which enters the database schema boundary. Do not retain the lifecycle
    /// mutex while waiting for that callback: schema mutations may activate
    /// the scheduler while holding the same schema boundary. Every concurrent
    /// shutdown caller still deactivates the task, so returning from this
    /// method remains a callback-drain barrier.
    if (task)
        task->deactivate();
    invalidateCachedSnapshot();
    setState(AuthorityVerificationSchedulerState::Shutdown);
}

AuthorityVerificationSchedulerStatus AuthorityVerificationScheduler::getStatus() const noexcept
{
    const UInt64 last_error_snapshot = last_error.load(std::memory_order_acquire);
    AuthorityVerificationSchedulerStatus result;
    result.scheduler_status_available = true;
    result.state = state.load(std::memory_order_acquire);
    result.runs = runs.load(std::memory_order_relaxed);
    result.cached_targets = cached_target_count.load(std::memory_order_relaxed);
    result.planned_batches = planned_batches.load(std::memory_order_relaxed);
    result.planned_targets = planned_targets.load(std::memory_order_relaxed);
    result.terminal_targets = terminal_targets.load(std::memory_order_relaxed);
    result.verified_targets = verified_targets.load(std::memory_order_relaxed);
    result.damaged_targets = damaged_targets.load(std::memory_order_relaxed);
    result.cursor_advances = cursor_advances.load(std::memory_order_relaxed);
    result.incomplete_batches = incomplete_batches.load(std::memory_order_relaxed);
    result.failures = failures.load(std::memory_order_relaxed);
    result.repair_attempts = repair_attempts.load(std::memory_order_relaxed);
    result.repair_successes = repair_successes.load(std::memory_order_relaxed);
    result.repair_unavailable = repair_unavailable.load(std::memory_order_relaxed);
    result.last_repair_transaction_id = last_repair_transaction_id.load(std::memory_order_relaxed);
    result.last_repair_local_wal_sources = last_repair_local_wal_sources.load(std::memory_order_relaxed);
    result.last_repair_replicated_authority_sources = last_repair_replicated_authority_sources.load(std::memory_order_relaxed);
    result.last_repair_verified_backup_sources = last_repair_verified_backup_sources.load(std::memory_order_relaxed);
    result.throttles = throttles.load(std::memory_order_relaxed);
    result.foreground_load_throttles = foreground_load_throttles.load(std::memory_order_relaxed);
    result.background_load_throttles = background_load_throttles.load(std::memory_order_relaxed);
    result.wall_time_budget_yields = wall_time_budget_yields.load(std::memory_order_relaxed);
    result.cpu_time_budget_yields = cpu_time_budget_yields.load(std::memory_order_relaxed);
    result.last_observed_foreground_queries = last_observed_foreground_queries.load(std::memory_order_relaxed);
    result.last_observed_competing_background_tasks = last_observed_competing_background_tasks.load(std::memory_order_relaxed);
    result.last_completed_rotations = last_completed_rotations.load(std::memory_order_relaxed);
    result.last_planned_batch_sequence = last_planned_batch_sequence.load(std::memory_order_relaxed);
    result.effective_maximum_snapshot_targets = limits.schedule.maximum_snapshot_targets;
    result.effective_maximum_targets_per_batch = limits.schedule.maximum_targets_per_batch;
    result.effective_maximum_buckets = limits.schedule.maximum_buckets;
    result.effective_maximum_reverse_dependency_count = limits.schedule.maximum_reverse_dependency_count;
    result.effective_maximum_canonical_bytes_per_batch = limits.schedule.maximum_canonical_bytes_per_batch;
    result.effective_maximum_work_units_per_batch = limits.schedule.maximum_verification_work_units_per_batch;
    result.effective_maximum_transient_bytes_per_batch = limits.schedule.maximum_transient_bytes_per_batch;
    result.effective_maximum_io_bytes_per_batch = limits.schedule.maximum_io_bytes_per_batch;
    result.effective_maximum_rooted_target_canonical_bytes = limits.schedule.maximum_rooted_target_canonical_bytes;
    result.effective_maximum_rooted_target_work_units = limits.schedule.maximum_rooted_target_verification_work_units;
    result.effective_maximum_rooted_target_transient_bytes = limits.schedule.maximum_rooted_target_transient_bytes;
    result.effective_maximum_rooted_target_io_bytes = limits.schedule.maximum_rooted_target_io_bytes;
    result.effective_maximum_planner_work_units = limits.schedule.maximum_planner_work_units;
    result.effective_maximum_planner_scratch_bytes = limits.schedule.maximum_planner_scratch_bytes;
    result.effective_maximum_planner_retained_bytes = limits.schedule.maximum_retained_canonical_bytes;
    result.effective_maximum_cooperative_work_items_per_pass = limits.maximum_snapshot_targets_per_pass;
    result.effective_maximum_run_wall_time_ms = static_cast<UInt64>(limits.maximum_run_wall_time.count());
    result.effective_maximum_run_cpu_time_ms = static_cast<UInt64>(limits.maximum_run_cpu_time.count());
    result.effective_successful_batch_interval_ms = static_cast<UInt64>(limits.successful_batch_interval.count());
    result.effective_load_throttle_retry_interval_ms = static_cast<UInt64>(limits.load_throttle_retry_interval.count());
    result.effective_maximum_foreground_queries_for_admission = limits.maximum_foreground_queries_for_admission;
    result.effective_maximum_competing_background_tasks_for_admission = limits.maximum_competing_background_tasks_for_admission;
    result.effective_os_thread_nice_value = limits.os_thread_nice_value;
    result.last_throttle_reason = last_throttle_reason.load(std::memory_order_relaxed);
    result.last_error_kind = static_cast<AuthorityVerificationSchedulerLastErrorKind>(last_error_snapshot & 0xff);
    result.last_error_code = static_cast<Int32>(static_cast<UInt32>(last_error_snapshot >> 8));
    {
        std::lock_guard lock(diagnostics_mutex);
        if (last_observed_root)
        {
            result.last_root_catalog_epoch = last_observed_root->database_catalog_epoch;
            result.last_root_authority_anchor = last_observed_root->authority_anchor;
        }
        if (last_successful_root)
        {
            result.last_successful_root_catalog_epoch = last_successful_root->database_catalog_epoch;
            result.last_successful_root_authority_anchor = last_successful_root->authority_anchor;
        }
    }
    return result;
}

void AuthorityVerificationScheduler::scheduleAfter(std::chrono::milliseconds delay) noexcept
{
    if (shutdown.load(std::memory_order_acquire) || stop_source.stop_requested() || !task)
        return;
    static_cast<void>(task->scheduleAfter(static_cast<size_t>(delay.count())));
}

void AuthorityVerificationScheduler::setState(AuthorityVerificationSchedulerState state_) noexcept
{
    state.store(state_, std::memory_order_release);
}

void AuthorityVerificationScheduler::recordLastError(AuthorityVerificationSchedulerLastErrorKind kind, Int32 code) noexcept
{
    const UInt64 snapshot = (static_cast<UInt64>(static_cast<UInt32>(code)) << 8) | static_cast<UInt8>(kind);
    last_error.store(snapshot, std::memory_order_release);
}

void AuthorityVerificationScheduler::throttle(AuthorityVerificationSchedulerThrottleReason reason, std::chrono::milliseconds delay) noexcept
{
    throttles.fetch_add(1, std::memory_order_relaxed);
    last_throttle_reason.store(reason, std::memory_order_relaxed);
    switch (reason)
    {
        case AuthorityVerificationSchedulerThrottleReason::ForegroundLoad:
            foreground_load_throttles.fetch_add(1, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationForegroundLoadThrottles);
            break;
        case AuthorityVerificationSchedulerThrottleReason::BackgroundLoad:
            background_load_throttles.fetch_add(1, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationBackgroundLoadThrottles);
            break;
        case AuthorityVerificationSchedulerThrottleReason::WallTimeBudget:
            wall_time_budget_yields.fetch_add(1, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationWallTimeYields);
            break;
        case AuthorityVerificationSchedulerThrottleReason::CPUTimeBudget:
            cpu_time_budget_yields.fetch_add(1, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationCPUTimeYields);
            break;
        case AuthorityVerificationSchedulerThrottleReason::None: break;
    }
    setState(AuthorityVerificationSchedulerState::Throttled);
    scheduleAfter(delay);
}

void AuthorityVerificationScheduler::invalidateCachedSnapshot() noexcept
{
    FailPointInjection::pauseFailPoint(DB::FailPoints::udt_authority_verification_pause_before_cache_invalidation);
    std::lock_guard lock(cache_mutex);
    cached_root.reset();
    cached_targets.clear();
    cached_history.clear();
    cached_build_offset = 0;
    cached_history_published = false;
    cached_planning.reset();
    cached_planning_cursor.reset();
    cached_plan.reset();
    cached_receipt.reset();
    cached_target_count.store(0, std::memory_order_relaxed);
}

void AuthorityVerificationScheduler::run() noexcept
{
    if (shutdown.load(std::memory_order_acquire) || stop_source.stop_requested())
        return;
    ++runs;
    ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationRuns);

    try
    {
        CurrentMetrics::Increment active_task(CurrentMetrics::UDTAuthorityVerificationTask);
        auto nice_guard = OSThreadNiceValue::scoped(limits.os_thread_nice_value);
        const SchedulerRunBudget budget(limits);
        const auto observed_load = limits.load_probe();
        last_observed_foreground_queries.store(observed_load.foreground_queries, std::memory_order_relaxed);
        last_observed_competing_background_tasks.store(observed_load.competing_background_tasks, std::memory_order_relaxed);
        const auto throttle_if_budget_expired = [&]()
        {
            if (stop_source.stop_requested())
                return true;
            if (budget.cpuExpired())
            {
                throttle(AuthorityVerificationSchedulerThrottleReason::CPUTimeBudget, limits.load_throttle_retry_interval);
                return true;
            }
            if (budget.wallExpired())
            {
                throttle(AuthorityVerificationSchedulerThrottleReason::WallTimeBudget, limits.load_throttle_retry_interval);
                return true;
            }
            return false;
        };
        auto executor_limits = limits.executor;
        executor_limits.monotonic_deadline = budget.wall_deadline;
        executor_limits.thread_cpu_deadline_nanoseconds = budget.cpu_deadline_nanoseconds;
        const AuthorityVerificationPassBudget pass_budget{
            .cancellation = stop_source.get_token(),
            .monotonic_deadline = budget.wall_deadline,
            .thread_cpu_deadline_nanoseconds = budget.cpu_deadline_nanoseconds,
            .maximum_work_items = limits.maximum_snapshot_targets_per_pass,
        };
        UInt64 remaining_pass_work_items = pass_budget.maximum_work_items;
        const auto consume_pass_work_items = [&](UInt64 consumed)
        {
            if (consumed > remaining_pass_work_items)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification phase exceeded its cooperative pass budget");
            remaining_pass_work_items -= consumed;
        };

        AtomicDatabaseSchemaMutationStorage * storage = nullptr;
        AuthorityVerificationRuntimeState * runtime = nullptr;
        std::optional<AtomicAuthority::RootSnapshot> root;
        {
            std::unique_lock schema_lock(database.udt_schema_mutation_mutex);
            std::lock_guard authority_lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown || database.udt_table_startup_state || !database.udt_authority
                || !database.udt_mutation_storage || !database.udt_verification_runtime
                || database.active_udt_authority.load(std::memory_order_acquire) != database.udt_authority.get()
                || database.active_udt_verification_runtime.load(std::memory_order_acquire) != database.udt_verification_runtime.get())
            {
                setState(AuthorityVerificationSchedulerState::Dormant);
                return;
            }
            storage = database.udt_mutation_storage.get();
            runtime = database.udt_verification_runtime.get();
            root.emplace(database.udt_authority->acquireCurrentRoot());
        }

        if (!*root)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "active Atomic UDT verification scheduler has no authority root");
        if (throttle_if_budget_expired())
            return;
        const AuthorityRoot & exact_root = root->get();
        const auto root_identity = identifyRoot(exact_root);
        {
            std::lock_guard lock(diagnostics_mutex);
            last_observed_root = root_identity;
        }
        const auto attempt_automatic_repair = [&](AuthorityVerificationSchedulerLastErrorKind trigger)
        {
            if (throttle_if_budget_expired())
                return;
            recordLastError(trigger);
            repair_attempts.fetch_add(1, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::UDTAuthorityAutomaticRepairAttempts);
            auto repair_limits = limits.automatic_repair;
            repair_limits.cancellation = stop_source.get_token();
            repair_limits.monotonic_deadline = budget.wall_deadline;
            repair_limits.thread_cpu_deadline_nanoseconds = budget.cpu_deadline_nanoseconds;
            repair_limits.continuation = automatic_repair_continuation;
            repair_limits.execution.maximum_reverification_work_items_per_pass = limits.maximum_snapshot_targets_per_pass;
            repair_limits.execution.reverification_continuation = repair_reverification_continuation;
            repair_limits.execution.verification_executor.monotonic_deadline = budget.wall_deadline;
            repair_limits.execution.verification_executor.thread_cpu_deadline_nanoseconds = budget.cpu_deadline_nanoseconds;
            std::optional<AuthorityAutomaticRepairResult> repair_result;
            try
            {
                repair_result.emplace(AuthorityAutomaticRepair::attempt(database, repair_limits));
            }
            catch (const AuthorityRepairAuditError & error)
            {
                if (error.code != AuthorityRepairAuditError::Code::ExecutionBudgetExceeded)
                    throw;
                invalidateCachedSnapshot();
                if (!throttle_if_budget_expired())
                    throw;
                return;
            }
            const auto & repair = *repair_result;
            if (repair.execution)
            {
                if (repair.execution->transaction_id != 0)
                {
                    last_repair_transaction_id.store(repair.execution->transaction_id, std::memory_order_relaxed);
                    last_repair_local_wal_sources.store(repair.execution->local_wal_sources, std::memory_order_relaxed);
                    last_repair_replicated_authority_sources.store(
                        repair.execution->replicated_authority_sources, std::memory_order_relaxed);
                    last_repair_verified_backup_sources.store(repair.execution->verified_backup_sources, std::memory_order_relaxed);
                }
                std::lock_guard lock(diagnostics_mutex);
                last_observed_root = repair.execution->repaired_root;
            }
            const bool released = repair.status == AuthorityAutomaticRepairStatus::RepairedAndReleased
                || repair.status == AuthorityAutomaticRepairStatus::ReverifiedAndReleased;
            const bool no_longer_quarantined = released || repair.status == AuthorityAutomaticRepairStatus::NoQuarantine;
            if (!no_longer_quarantined && (stop_source.stop_requested() || budget.cpuExpired() || budget.wallExpired()))
            {
                invalidateCachedSnapshot();
                static_cast<void>(throttle_if_budget_expired());
                return;
            }
            if (no_longer_quarantined)
            {
                recordLastError(AuthorityVerificationSchedulerLastErrorKind::None);
                if (released)
                {
                    if (repair.execution)
                    {
                        std::lock_guard lock(diagnostics_mutex);
                        last_successful_root = repair.execution->repaired_root;
                    }
                    repair_successes.fetch_add(1, std::memory_order_relaxed);
                    ProfileEvents::increment(ProfileEvents::UDTAuthorityAutomaticRepairSuccesses);
                }
            }
            else
            {
                recordLastError(AuthorityVerificationSchedulerLastErrorKind::ExactRepairUnavailable);
                repair_unavailable.fetch_add(1, std::memory_order_relaxed);
                ProfileEvents::increment(ProfileEvents::UDTAuthorityAutomaticRepairUnavailable);
            }
            invalidateCachedSnapshot();
            if (throttle_if_budget_expired())
                return;
            setState(no_longer_quarantined ? AuthorityVerificationSchedulerState::Scheduled : AuthorityVerificationSchedulerState::Backoff);
            scheduleAfter(no_longer_quarantined ? limits.successful_batch_interval : limits.failure_backoff_interval);
        };

        bool runtime_fail_closed = false;
        bool runtime_quarantined = false;
        {
            auto runtime_snapshot = runtime->acquireSnapshot();
            runtime_fail_closed = runtime_snapshot.isFailClosed();
            runtime_quarantined = static_cast<bool>(runtime_snapshot.getQuarantine());
        }
        if (runtime_fail_closed)
        {
            root.reset();
            recordLastError(AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed);
            setState(AuthorityVerificationSchedulerState::Backoff);
            scheduleAfter(limits.failure_backoff_interval);
            return;
        }
        if (runtime_quarantined)
        {
            root.reset();
            attempt_automatic_repair(AuthorityVerificationSchedulerLastErrorKind::IntegrityDamageQuarantined);
            return;
        }
        if (observed_load.foreground_queries > limits.maximum_foreground_queries_for_admission)
        {
            throttle(AuthorityVerificationSchedulerThrottleReason::ForegroundLoad, limits.load_throttle_retry_interval);
            return;
        }
        if (observed_load.competing_background_tasks > limits.maximum_competing_background_tasks_for_admission)
        {
            throttle(AuthorityVerificationSchedulerThrottleReason::BackgroundLoad, limits.load_throttle_retry_interval);
            return;
        }
        if (exact_root.getInventorySummary().leaf_count == 0)
        {
            invalidateCachedSnapshot();
            recordLastError(AuthorityVerificationSchedulerLastErrorKind::None);
            setState(AuthorityVerificationSchedulerState::EmptyRoot);
            scheduleAfter(limits.empty_root_probe_interval);
            return;
        }

        const auto cursor = runtime->getCursor();
        if (!cursorMatchesPolicy(cursor, limits.policy, database.db_uuid))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "durable Atomic UDT verification cursor does not match the effective scheduler policy");
        AuthorityVerificationBatchPlan::Ptr plan;
        AuthorityVerificationBatchReceipt::Ptr prefix_receipt;
        {
            std::lock_guard cache_lock(cache_mutex);
            if (!cached_root || *cached_root != root_identity)
            {
                setState(AuthorityVerificationSchedulerState::BuildingSnapshot);
                cached_root = root_identity;
                cached_targets.clear();
                cached_history.clear();
                cached_build_offset = 0;
                cached_history_published = false;
                cached_planning.reset();
                cached_planning_cursor.reset();
                cached_plan.reset();
                cached_receipt.reset();
                cached_target_count.store(0, std::memory_order_relaxed);
            }
            const UInt64 leaf_count = exact_root.getInventorySummary().leaf_count;
            if (cached_build_offset < leaf_count)
            {
                setState(AuthorityVerificationSchedulerState::BuildingSnapshot);
                auto snapshot_pass_budget = pass_budget;
                snapshot_pass_budget.maximum_work_items = remaining_pass_work_items;
                UInt64 consumed_snapshot_work_items = 0;
                auto additions = storage->snapshotAuthorityVerificationTargets(
                    exact_root,
                    limits.schedule,
                    executor_limits,
                    cached_build_offset,
                    limits.maximum_snapshot_targets_per_pass,
                    scheduling_history,
                    snapshot_pass_budget,
                    &consumed_snapshot_work_items);
                consume_pass_work_items(consumed_snapshot_work_items);
                if (additions.empty())
                {
                    if (throttle_if_budget_expired())
                        return;
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification snapshot builder made no progress");
                }
                if (additions.size() > leaf_count - cached_build_offset)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification snapshot builder made invalid progress");
                cached_build_offset += static_cast<UInt64>(additions.size());
                for (const auto & target : additions)
                {
                    cached_history.push_back({
                        .leaf = target.leaf,
                        .last_changed_catalog_epoch = target.last_changed_catalog_epoch,
                        .last_periodic_verification_sequence = target.last_periodic_verification_sequence,
                    });
                }
                cached_targets.insert(
                    cached_targets.end(), std::make_move_iterator(additions.begin()), std::make_move_iterator(additions.end()));
                cached_target_count.store(static_cast<UInt64>(cached_targets.size()), std::memory_order_relaxed);
                if (throttle_if_budget_expired())
                    return;
                if (cached_build_offset < leaf_count)
                {
                    scheduleAfter(limits.snapshot_build_interval);
                    return;
                }
            }
            if (cached_targets.size() != leaf_count)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification snapshot does not cover the exact root inventory");
            if (!cached_history_published)
            {
                if (cached_history.size() != leaf_count)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification scheduling history is incomplete");
                scheduling_history.swap(cached_history);
                cached_history.clear();
                cached_history_published = true;
            }

            if (cached_plan && cached_plan->getRetryCursor() != cursor)
            {
                cached_plan.reset();
                cached_receipt.reset();
            }
            if (!cached_plan && remaining_pass_work_items == 0)
            {
                scheduleAfter(limits.snapshot_build_interval);
                return;
            }
            if (!cached_plan)
            {
                if (!cached_planning_cursor || *cached_planning_cursor != cursor)
                {
                    cached_planning.reset();
                    cached_planning_cursor = cursor;
                }
                auto planning_pass_budget = pass_budget;
                planning_pass_budget.maximum_work_items = remaining_pass_work_items;
                auto planning = resumePeriodicAuthorityVerificationPlanning(
                    exact_root, cached_targets, cursor, limits.policy, limits.schedule, cached_planning, planning_pass_budget);
                consume_pass_work_items(planning.consumed_work_items);
                if (planning.status == AuthorityVerificationPlanningStatus::InProgress)
                {
                    if (throttle_if_budget_expired())
                        return;
                    if (planning.consumed_work_items == 0)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification planner made no cooperative progress");
                    scheduleAfter(limits.snapshot_build_interval);
                    return;
                }
                cached_plan = std::move(planning.plan);
                cached_receipt.reset();
                planned_batches.fetch_add(1, std::memory_order_relaxed);
                planned_targets.fetch_add(static_cast<UInt64>(cached_plan->getTargets().size()), std::memory_order_relaxed);
                last_planned_batch_sequence.store(cursor.planned_batches + 1, std::memory_order_relaxed);
            }
            plan = cached_plan;
            prefix_receipt = cached_receipt;
        }
        root.reset();

        if (throttle_if_budget_expired())
            return;
        if (remaining_pass_work_items == 0)
        {
            setState(AuthorityVerificationSchedulerState::Scheduled);
            scheduleAfter(limits.snapshot_build_interval);
            return;
        }

        setState(AuthorityVerificationSchedulerState::Executing);
        const size_t prefix_completion_count = prefix_receipt ? prefix_receipt->getTerminalCompletions().size() : 0;
        /// Establish the cooperative charge before prefix validation/copy and
        /// trusted execution setup, both of which scale with the sealed plan.
        /// The first newly terminal target shares this setup item.
        const UInt64 execution_work_item_budget = remaining_pass_work_items;
        consume_pass_work_items(1);
        executor_limits.maximum_terminal_targets = std::min(executor_limits.maximum_terminal_targets, execution_work_item_budget);
        const auto receipt = database.executeUDTAuthorityVerificationBatch(*plan, executor_limits, false, prefix_receipt.get());
        const auto completions = receipt->getTerminalCompletions();
        if (completions.size() < prefix_completion_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification cumulative receipt lost its sealed prefix");
        const UInt64 new_completion_count = static_cast<UInt64>(completions.size() - prefix_completion_count);
        if (new_completion_count > executor_limits.maximum_terminal_targets)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification executor exceeded its cooperative terminal-target cap");
        if (new_completion_count > 1)
            consume_pass_work_items(new_completion_count - 1);
        terminal_targets.fetch_add(new_completion_count, std::memory_order_relaxed);
        UInt64 batch_verified = 0;
        UInt64 batch_damaged = 0;
        for (size_t index = prefix_completion_count; index < completions.size(); ++index)
        {
            const auto & completion = completions[index];
            if (completion.disposition == AuthorityVerificationTargetDisposition::Verified)
                ++batch_verified;
            else if (completion.disposition == AuthorityVerificationTargetDisposition::Damaged)
                ++batch_damaged;
        }
        {
            std::lock_guard cache_lock(cache_mutex);
            if (cached_plan != plan || !cached_root || *cached_root != plan->getRoot())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification cache changed during one scheduler run");
            cached_receipt = receipt;
            const UInt64 verification_sequence = plan->getRetryCursor().planned_batches + 1;
            for (size_t index = prefix_completion_count; index < completions.size(); ++index)
            {
                const auto & completion = completions[index];
                if (completion.disposition != AuthorityVerificationTargetDisposition::Verified)
                    continue;
                const auto history_it = std::lower_bound(
                    scheduling_history.begin(),
                    scheduling_history.end(),
                    completion.leaf.key,
                    [](const AuthorityVerificationTargetHistory & candidate, const AuthorityInventoryKey & key)
                    { return authorityInventoryKeyLess(candidate.leaf.key, key); });
                const auto target_it = std::lower_bound(
                    cached_targets.begin(),
                    cached_targets.end(),
                    completion.leaf.key,
                    [](const AuthorityVerificationTarget & candidate, const AuthorityInventoryKey & key)
                    { return authorityInventoryKeyLess(candidate.leaf.key, key); });
                if (history_it == scheduling_history.end() || history_it->leaf != completion.leaf || target_it == cached_targets.end()
                    || target_it->leaf != completion.leaf)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification history lost a completed target");
                history_it->last_periodic_verification_sequence = verification_sequence;
                target_it->last_periodic_verification_sequence = verification_sequence;
            }
        }
        verified_targets.fetch_add(batch_verified, std::memory_order_relaxed);
        damaged_targets.fetch_add(batch_damaged, std::memory_order_relaxed);
        ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationTargetsVerified, batch_verified);
        ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationTargetsDamaged, batch_damaged);

        if (throttle_if_budget_expired())
            return;

        runtime_fail_closed = false;
        runtime_quarantined = false;
        {
            auto runtime_snapshot = runtime->acquireSnapshot();
            runtime_fail_closed = runtime_snapshot.isFailClosed();
            runtime_quarantined = static_cast<bool>(runtime_snapshot.getQuarantine());
        }
        if (runtime_fail_closed)
        {
            invalidateCachedSnapshot();
            recordLastError(AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed);
            setState(AuthorityVerificationSchedulerState::Backoff);
            scheduleAfter(limits.failure_backoff_interval);
            return;
        }
        if (runtime_quarantined)
        {
            attempt_automatic_repair(AuthorityVerificationSchedulerLastErrorKind::IntegrityDamageQuarantined);
            return;
        }

        const auto next_cursor = runtime->getCursor();
        last_completed_rotations.store(next_cursor.completed_rotations, std::memory_order_relaxed);
        if (next_cursor != cursor)
        {
            {
                std::lock_guard cache_lock(cache_mutex);
                cached_plan.reset();
                cached_receipt.reset();
                cached_planning.reset();
                cached_planning_cursor.reset();
            }
            recordLastError(AuthorityVerificationSchedulerLastErrorKind::None);
            {
                std::lock_guard lock(diagnostics_mutex);
                last_successful_root = plan->getRoot();
            }
            cursor_advances.fetch_add(1, std::memory_order_relaxed);
            setState(AuthorityVerificationSchedulerState::Scheduled);
            scheduleAfter(limits.successful_batch_interval);
            return;
        }

        if (completions.size() == plan->getTargets().size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "complete clean Atomic UDT verification receipt did not advance its cursor");

        incomplete_batches.fetch_add(1, std::memory_order_relaxed);
        setState(AuthorityVerificationSchedulerState::Scheduled);
        scheduleAfter(limits.retry_interval);
    }
    catch (...)
    {
        const auto error_code = static_cast<Int32>(getCurrentExceptionCode());
        recordLastError(AuthorityVerificationSchedulerLastErrorKind::VerificationFailure, error_code);
        failures.fetch_add(1, std::memory_order_relaxed);
        ProfileEvents::increment(ProfileEvents::UDTAuthorityVerificationFailures);
        invalidateCachedSnapshot();
        setState(AuthorityVerificationSchedulerState::Backoff);
        logVerificationFailureNoThrow(log, database.db_uuid, error_code);
        scheduleAfter(limits.failure_backoff_interval);
    }
}

}
