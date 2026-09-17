#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/logger_useful.h>

#include <base/hex.h>

#include <algorithm>
#include <string>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_DATABASE;
}

namespace
{

String verificationStateName(const UDT::AuthorityVerificationSchedulerStatus & status)
{
    if (status.runtime_fail_closed)
        return "FailClosed";
    if (status.quarantined_objects != 0)
        return "Quarantined";
    if (!status.scheduler_status_available)
        return "Unavailable";

    switch (status.state)
    {
        case UDT::AuthorityVerificationSchedulerState::Dormant: return "Dormant";
        case UDT::AuthorityVerificationSchedulerState::Scheduled: return "Scheduled";
        case UDT::AuthorityVerificationSchedulerState::BuildingSnapshot: return "BuildingSnapshot";
        case UDT::AuthorityVerificationSchedulerState::Executing: return "Executing";
        case UDT::AuthorityVerificationSchedulerState::EmptyRoot: return "EmptyRoot";
        case UDT::AuthorityVerificationSchedulerState::Throttled: return "Throttled";
        case UDT::AuthorityVerificationSchedulerState::Backoff: return "Backoff";
        case UDT::AuthorityVerificationSchedulerState::Shutdown: return "Shutdown";
    }
    return "Unavailable";
}

String verificationThrottleReasonName(UDT::AuthorityVerificationSchedulerThrottleReason reason)
{
    switch (reason)
    {
        case UDT::AuthorityVerificationSchedulerThrottleReason::None: return "None";
        case UDT::AuthorityVerificationSchedulerThrottleReason::ForegroundLoad: return "ForegroundLoad";
        case UDT::AuthorityVerificationSchedulerThrottleReason::BackgroundLoad: return "BackgroundLoad";
        case UDT::AuthorityVerificationSchedulerThrottleReason::WallTimeBudget: return "WallTimeBudget";
        case UDT::AuthorityVerificationSchedulerThrottleReason::CPUTimeBudget: return "CPUTimeBudget";
    }
    return "None";
}

String verificationLastError(const UDT::AuthorityVerificationSchedulerStatus & status)
{
    switch (status.last_error_kind)
    {
        case UDT::AuthorityVerificationSchedulerLastErrorKind::None: return {};
        case UDT::AuthorityVerificationSchedulerLastErrorKind::VerificationFailure:
            return status.last_error_code == 0 ? "VerificationFailure"
                                               : "VerificationFailure:ErrorCode=" + std::to_string(status.last_error_code);
        case UDT::AuthorityVerificationSchedulerLastErrorKind::IntegrityDamageQuarantined: return "IntegrityDamageQuarantined";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::ExactRepairUnavailable: return "ExactRepairUnavailable";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed: return "RuntimeFailClosed";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeQuarantineConstructionFailed:
            return "RuntimeQuarantineConstructionFailed";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::StartupInvalid: return "StartupInvalid";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::StartupIncomplete: return "StartupIncomplete";
        case UDT::AuthorityVerificationSchedulerLastErrorKind::StartupConflicted: return "StartupConflicted";
    }
    return "RuntimeFailClosed";
}

String rootQuotaStateName(const UDT::AuthorityVerificationSchedulerStatus & status)
{
    if (!status.root_quota_status_available)
        return "UNAVAILABLE";
    return status.root_quota_over_quota ? "OVER_QUOTA" : "ACTIVE";
}

String overrideStateName(bool configured, bool effective, bool persisted)
{
    if (persisted)
        return "Persisted";
    if (configured)
        return "ConfiguredPendingActivation";
    if (effective)
        return "Effective";
    return "Default";
}

String digestToLowerHex(const UDT::Digest & digest)
{
    String result(digest.size() * 2, '\0');
    for (size_t index = 0; index < digest.size(); ++index)
    {
        const auto byte = static_cast<unsigned char>(digest[index]);
        result[2 * index] = hexDigitLowercase(byte >> 4);
        result[2 * index + 1] = hexDigitLowercase(byte & 0x0f);
    }
    return result;
}

}

ColumnsDescription StorageSystemDatabases::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto description = ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(), "Database name."},
        {"engine", std::make_shared<DataTypeString>(), "Database engine."},
        {"data_path", std::make_shared<DataTypeString>(), "Data path."},
        {"metadata_path", std::make_shared<DataTypeString>(), "Metadata path."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Database UUID."},
        {"engine_full", std::make_shared<DataTypeString>(), "Parameters of the database engine."},
        {"comment", std::make_shared<DataTypeString>(), "Database comment."},
        {"is_external", std::make_shared<DataTypeUInt8>(), "Database is external (i.e. PostgreSQL/DataLakeCatalog)."},
        {"udt_verification_state", low_cardinality_string, "Stable database-owned UDT verifier/quarantine state."},
        {"udt_verification_last_throttle_reason", low_cardinality_string, "Stable last bounded UDT verifier throttle reason."},
        {"udt_verification_last_error",
         std::make_shared<DataTypeString>(),
         "Bounded payload-free UDT integrity diagnostic; raw metadata and normalized literals are never exposed."},
        {"udt_verification_scheduler_override_state",
         low_cardinality_string,
         "Verification-scheduler override state: Default, ConfiguredPendingActivation, Effective, or Persisted."},
        {"udt_resource_quota_override_state",
         low_cardinality_string,
         "Database resource-quota override state: Default, ConfiguredPendingActivation, Effective, or Persisted."},
        {"udt_verification_runs", std::make_shared<DataTypeUInt64>(), "Periodic UDT verifier runs."},
        {"udt_verification_cached_targets",
         std::make_shared<DataTypeUInt64>(),
         "Targets retained in the current exact-root verifier snapshot."},
        {"udt_verification_planned_batches", std::make_shared<DataTypeUInt64>(), "Verification batches planned by the scheduler."},
        {"udt_verification_planned_targets", std::make_shared<DataTypeUInt64>(), "Targets sealed into verification batch plans."},
        {"udt_verification_terminal_targets", std::make_shared<DataTypeUInt64>(), "Targets with a terminal verification disposition."},
        {"udt_verification_verified_targets", std::make_shared<DataTypeUInt64>(), "Targets completed as verified."},
        {"udt_verification_damaged_targets", std::make_shared<DataTypeUInt64>(), "Targets completed as damaged."},
        {"udt_verification_cursor_advances", std::make_shared<DataTypeUInt64>(), "Durable scheduler cursor advances after clean batches."},
        {"udt_verification_incomplete_batches",
         std::make_shared<DataTypeUInt64>(),
         "Verification executions retained for cooperative continuation."},
        {"udt_verification_completed_rotations", std::make_shared<DataTypeUInt64>(), "Completed deterministic inventory rotations."},
        {"udt_verification_last_root_catalog_epoch",
         std::make_shared<DataTypeUInt64>(),
         "Catalog epoch of the last exact authority root observed by the verifier."},
        {"udt_verification_last_root_authority_anchor",
         std::make_shared<DataTypeString>(),
         "Lowercase hexadecimal anchor of the last exact authority root observed by the verifier; empty when unavailable."},
        {"udt_verification_last_successful_root_catalog_epoch",
         std::make_shared<DataTypeUInt64>(),
         "Catalog epoch of the last exact authority root accepted by a clean scheduled batch or quarantine-release re-verification."},
        {"udt_verification_last_successful_root_authority_anchor",
         std::make_shared<DataTypeString>(),
         "Lowercase hexadecimal anchor of the last exact authority root accepted by a clean scheduled batch or quarantine-release "
         "re-verification; empty when unavailable."},
        {"udt_verification_last_planned_batch_sequence",
         std::make_shared<DataTypeUInt64>(),
         "Deterministic verification sequence of the last sealed batch plan."},
        {"udt_verification_failures", std::make_shared<DataTypeUInt64>(), "Verifier runs that failed before a schedulable outcome."},
        {"udt_verification_throttles", std::make_shared<DataTypeUInt64>(), "Verifier runs deferred by load or time budgets."},
        {"udt_verification_foreground_load_throttles",
         std::make_shared<DataTypeUInt64>(),
         "Verifier runs deferred by foreground query load."},
        {"udt_verification_background_load_throttles",
         std::make_shared<DataTypeUInt64>(),
         "Verifier runs deferred by competing background load."},
        {"udt_verification_wall_time_budget_yields",
         std::make_shared<DataTypeUInt64>(),
         "Verifier runs cooperatively yielded at the monotonic wall-time budget."},
        {"udt_verification_cpu_time_budget_yields",
         std::make_shared<DataTypeUInt64>(),
         "Verifier runs cooperatively yielded at the thread CPU-time budget."},
        {"udt_verification_last_observed_foreground_queries",
         std::make_shared<DataTypeUInt64>(),
         "Foreground query load observed by the last verifier run."},
        {"udt_verification_last_observed_competing_background_tasks",
         std::make_shared<DataTypeUInt64>(),
         "Competing background task load observed by the last verifier run."},
        {"udt_verification_repair_attempts", std::make_shared<DataTypeUInt64>(), "Automatic exact-repair attempts."},
        {"udt_verification_repair_successes",
         std::make_shared<DataTypeUInt64>(),
         "Automatic repairs or reverifications that released quarantine."},
        {"udt_verification_repair_unavailable",
         std::make_shared<DataTypeUInt64>(),
         "Automatic repair attempts without an exact releasable result."},
        {"udt_verification_last_repair_transaction_id",
         std::make_shared<DataTypeUInt64>(),
         "Durable schema-WAL transaction of the last exact UDT repair; zero for re-verification-only results."},
        {"udt_verification_last_repair_local_wal_sources",
         std::make_shared<DataTypeUInt64>(),
         "Artifacts in the last exact UDT repair sourced from the local schema WAL."},
        {"udt_verification_last_repair_replicated_authority_sources",
         std::make_shared<DataTypeUInt64>(),
         "Artifacts in the last exact UDT repair sourced from authenticated replicated authority."},
        {"udt_verification_last_repair_verified_backup_sources",
         std::make_shared<DataTypeUInt64>(),
         "Artifacts in the last exact UDT repair sourced from a verified backup."},
        {"udt_verification_last_repair_provenance_available",
         std::make_shared<DataTypeUInt8>(),
         "Whether the last exact repair has authenticated durable provenance recoverable after restart."},
        {"udt_verification_last_repair_damaged_artifacts",
         std::make_shared<DataTypeUInt64>(),
         "Exact damaged artifact target count committed by the last repair provenance."},
        {"udt_verification_last_repair_manifest_digest",
         std::make_shared<DataTypeString>(),
         "Lowercase hexadecimal digest of the exact damaged-artifact manifest; empty when unavailable."},
        {"udt_verification_last_repair_previous_catalog_epoch",
         std::make_shared<DataTypeUInt64>(),
         "Catalog epoch immediately before the last exact repair."},
        {"udt_verification_last_repair_previous_authority_anchor",
         std::make_shared<DataTypeString>(),
         "Lowercase hexadecimal authority anchor immediately before the last exact repair; empty when unavailable."},
        {"udt_verification_last_repair_repaired_catalog_epoch",
         std::make_shared<DataTypeUInt64>(),
         "Content-neutral successor catalog epoch committed by the last exact repair."},
        {"udt_verification_last_repair_repaired_authority_anchor",
         std::make_shared<DataTypeString>(),
         "Lowercase hexadecimal authority anchor committed by the last exact repair; empty when unavailable."},
        {"udt_verification_runtime_status_available",
         std::make_shared<DataTypeUInt8>(),
         "Whether one exact lock-free UDT runtime status snapshot was available."},
        {"udt_verification_runtime_fail_closed",
         std::make_shared<DataTypeUInt8>(),
         "Whether the UDT runtime currently rejects operations fail-closed."},
        {"udt_verification_runtime_revision",
         std::make_shared<DataTypeUInt64>(),
         "Revision of the exact immutable UDT runtime status snapshot."},
        {"udt_verification_quarantine_failing_seeds",
         std::make_shared<DataTypeUInt64>(),
         "Failing seed objects in the current immutable quarantine."},
        {"udt_verification_quarantined_objects",
         std::make_shared<DataTypeUInt64>(),
         "Objects in the current complete quarantine graph closure."},
        {"udt_verification_max_snapshot_targets",
         std::make_shared<DataTypeUInt64>(),
         "Effective object cap for one exact-root verification snapshot."},
        {"udt_verification_max_targets_per_batch", std::make_shared<DataTypeUInt64>(), "Effective object cap for one verification batch."},
        {"udt_verification_max_buckets", std::make_shared<DataTypeUInt64>(), "Effective deterministic scheduler bucket cap."},
        {"udt_verification_max_reverse_dependency_count",
         std::make_shared<DataTypeUInt64>(),
         "Effective per-target reverse-dependency count cap."},
        {"udt_verification_max_canonical_bytes_per_batch",
         std::make_shared<DataTypeUInt64>(),
         "Effective canonical-byte cap for one verification batch."},
        {"udt_verification_max_work_units_per_batch",
         std::make_shared<DataTypeUInt64>(),
         "Effective CPU-work cap for one verification batch."},
        {"udt_verification_max_transient_bytes_per_batch",
         std::make_shared<DataTypeUInt64>(),
         "Effective transient-memory cap for one verification batch."},
        {"udt_verification_max_io_bytes_per_batch", std::make_shared<DataTypeUInt64>(), "Effective I/O cap for one verification batch."},
        {"udt_verification_max_rooted_target_canonical_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact-root hard bound for one indivisible target's canonical bytes."},
        {"udt_verification_max_rooted_target_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Exact-root hard bound for one indivisible target's verifier work."},
        {"udt_verification_max_rooted_target_transient_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact-root hard bound for one indivisible target's transient bytes."},
        {"udt_verification_max_rooted_target_io_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact-root hard bound for one indivisible target's I/O bytes."},
        {"udt_verification_max_planner_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Effective deterministic planner work cap for one exact-root decision."},
        {"udt_verification_max_planner_scratch_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective deterministic planner scratch-memory cap."},
        {"udt_verification_max_planner_retained_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective canonical bytes retained by a resumable planner continuation."},
        {"udt_verification_max_cooperative_work_items_per_pass",
         std::make_shared<DataTypeUInt64>(),
         "Effective shared snapshot/planner/executor cooperative work quantum per scheduler run."},
        {"udt_verification_max_run_wall_time_ms",
         std::make_shared<DataTypeUInt64>(),
         "Effective monotonic wall-time cap for one verifier run."},
        {"udt_verification_max_run_cpu_time_ms", std::make_shared<DataTypeUInt64>(), "Effective thread CPU-time cap for one verifier run."},
        {"udt_verification_successful_batch_interval_ms",
         std::make_shared<DataTypeUInt64>(),
         "Effective delay after a clean verifier batch."},
        {"udt_verification_load_throttle_retry_interval_ms",
         std::make_shared<DataTypeUInt64>(),
         "Effective retry delay after load/time throttling."},
        {"udt_verification_max_foreground_queries", std::make_shared<DataTypeUInt64>(), "Effective foreground-load admission threshold."},
        {"udt_verification_max_competing_background_tasks",
         std::make_shared<DataTypeUInt64>(),
         "Effective competing-background-load admission threshold."},
        {"udt_verification_os_thread_nice_value", std::make_shared<DataTypeInt32>(), "Effective best-effort verifier thread nice value."},
        {"udt_quota_state", low_cardinality_string, "Immutable current authority-root quota state: ACTIVE, OVER_QUOTA, or UNAVAILABLE."},
        {"udt_quota_revision", std::make_shared<DataTypeUInt64>(), "Immutable authority-root quota revision."},
        {"udt_quota_definitions", std::make_shared<DataTypeUInt64>(), "Current definitions charged to the database quota."},
        {"udt_quota_deterministic_catalog_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Current deterministic catalog bytes charged to the database quota."},
        {"udt_quota_verification_targets",
         std::make_shared<DataTypeUInt64>(),
         "Current verification targets charged to the database quota."},
        {"udt_quota_verification_buckets",
         std::make_shared<DataTypeUInt64>(),
         "Exact verification-bucket requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_canonical_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact indivisible-target canonical-byte requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Exact indivisible-target verifier-work requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_transient_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact indivisible-target transient-byte requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_io_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact indivisible-target I/O-byte requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_planner_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Exact deterministic-planner work requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_planner_scratch_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact deterministic-planner scratch-byte requirement of the current immutable root charged to the database quota."},
        {"udt_quota_verification_retained_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Exact resumable-planner retained-byte requirement of the current immutable root charged to the database quota."},
        {"udt_quota_durable_dependent_object_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Current canonical dependent-object metadata, sidecar, and installation-record bytes charged to the database quota."},
        {"udt_quota_limit_definitions", std::make_shared<DataTypeUInt64>(), "Effective rooted definition limit for this database."},
        {"udt_quota_limit_deterministic_catalog_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective rooted deterministic-catalog byte limit for this database."},
        {"udt_quota_limit_verification_targets",
         std::make_shared<DataTypeUInt64>(),
         "Effective rooted verification-target limit for this database."},
        {"udt_quota_limit_verification_buckets",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for verification buckets in this database."},
        {"udt_quota_limit_verification_canonical_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for indivisible-target canonical bytes in this database."},
        {"udt_quota_limit_verification_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for indivisible-target verifier work in this database."},
        {"udt_quota_limit_verification_transient_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for indivisible-target transient bytes in this database."},
        {"udt_quota_limit_verification_io_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for indivisible-target I/O bytes in this database."},
        {"udt_quota_limit_verification_planner_work_units",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for deterministic-planner work in this database."},
        {"udt_quota_limit_verification_planner_scratch_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for deterministic-planner scratch bytes in this database."},
        {"udt_quota_limit_verification_retained_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective immutable-root admission ceiling for resumable-planner retained bytes in this database."},
        {"udt_quota_limit_durable_dependent_object_bytes",
         std::make_shared<DataTypeUInt64>(),
         "Effective rooted canonical dependent-object metadata, sidecar, and installation-record byte limit for this database."},
        {"udt_quota_limit_occurrence_paths_per_object",
         std::make_shared<DataTypeUInt64>(),
         "Effective persisted occurrence-path limit per dependent object."},
        {"udt_quota_limit_persisted_specializations_per_template",
         std::make_shared<DataTypeUInt64>(),
         "Effective unique persisted-specialization limit per template."},
        {"udt_quota_limit_sidecar_bytes_per_object",
         std::make_shared<DataTypeUInt64>(),
         "Effective canonical sidecar byte limit per dependent object."},
        {"udt_quota_max_occurrence_paths_per_object",
         std::make_shared<DataTypeUInt64>(),
         "Maximum exact persisted occurrence paths in any rooted dependent object."},
        {"udt_quota_max_persisted_specializations_per_template",
         std::make_shared<DataTypeUInt64>(),
         "Maximum exact unique persisted specializations for any rooted template."},
        {"udt_quota_max_sidecar_bytes_per_object",
         std::make_shared<DataTypeUInt64>(),
         "Maximum exact canonical sidecar bytes in any rooted dependent object."},
        {"udt_root_dependent_objects",
         std::make_shared<DataTypeUInt64>(),
         "Dependent objects represented by the exact root-owned usage index."},
        {"udt_root_total_occurrence_paths",
         std::make_shared<DataTypeUInt64>(),
         "Total exact persisted occurrence paths in the current authority root."},
        {"udt_root_unique_persisted_specializations",
         std::make_shared<DataTypeUInt64>(),
         "Global exact unique persisted specializations in the current authority root."},
    };

    description.setAliases({{"database", std::make_shared<DataTypeString>(), "name"}});

    return description;
}

static String getEngineFull(const ContextPtr & ctx, const DatabasePtr & database)
{
    DDLGuardPtr guard;
    while (true)
    {
        String name = database->getDatabaseName();
        guard = DatabaseCatalog::instance().getDDLGuard(name, "", nullptr);

        /// Ensure that the database was not renamed before we acquired the lock
        auto locked_database = DatabaseCatalog::instance().tryGetDatabase(name);

        if (locked_database.get() == database.get())
            break;

        /// Database was dropped
        if (name == database->getDatabaseName())
            return {};

        guard.reset();
        LOG_TRACE(getLogger("StorageSystemDatabases"), "Failed to lock database {} ({}), will retry", name, database->getUUID());
    }

    ASTPtr ast = database->getCreateDatabaseQuery();
    auto * ast_create = ast->as<ASTCreateQuery>();

    if (!ast_create || !ast_create->storage)
        return {};

    String engine_full = format({ctx, *ast_create->storage});
    static const char * const extra_head = " ENGINE = ";

    if (startsWith(engine_full, extra_head))
        engine_full = engine_full.substr(strlen(extra_head));

    return engine_full;
}

Block StorageSystemDatabases::getFilterSampleBlock() const
{
    /// Must list every column of the block passed to filterBlockWithPredicate in getFilteredDatabases.
    return {
        {{}, std::make_shared<DataTypeString>(), "name"},
        {{}, std::make_shared<DataTypeString>(), "engine"},
        {{}, std::make_shared<DataTypeUUID>(), "uuid"},
    };
}

static ColumnPtr getFilteredDatabases(const Databases & databases, const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr name_column = ColumnString::create();
    MutableColumnPtr engine_column = ColumnString::create();
    MutableColumnPtr uuid_column = ColumnUUID::create();

    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        name_column->insert(database_name);
        engine_column->insert(database->getEngineName());
        uuid_column->insert(database->getUUID());
    }

    Block block{
        ColumnWithTypeAndName(std::move(name_column), std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine"),
        ColumnWithTypeAndName(std::move(uuid_column), std::make_shared<DataTypeUUID>(), "uuid")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

void StorageSystemDatabases::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const
{
    const auto access = context->getAccess();
    const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
    /// Data lake catalogs and remote databases are always shown in `system.databases` regardless of system-table settings.
    /// Listing a database name is purely local metadata and never requires expensive calls to an external service.
    /// The settings only guard operations like `system.tables` / `system.columns` that enumerate a database's contents.
    const auto databases
        = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true});
    ColumnPtr filtered_databases_column = getFilteredDatabases(databases, predicate, context);

    for (size_t i = 0; i < filtered_databases_column->size(); ++i)
    {
        auto database_name = filtered_databases_column->getDataAt(i);

        if (need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// filter out the internal database for temporary tables in system.databases, asynchronous metric "NumberOfDatabases" behaves the same way

        auto database_it = databases.find(database_name);
        if (database_it == databases.end())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database_name);
        const auto & database = database_it->second;
        UDT::AuthorityVerificationSchedulerStatus verification_status;
        if (columns_mask.size() > 8
            && std::any_of(columns_mask.begin() + 8, columns_mask.end(), [](UInt8 selected) { return selected != 0; }))
        {
            if (const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database))
                verification_status = atomic->getUDTAuthorityVerificationSchedulerStatus();
        }

        size_t src_index = 0;
        size_t res_index = 0;
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database_name);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getEngineName());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(context->getPath() + database->getDataPath());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getMetadataPath());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getUUID());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(getEngineFull(context, database));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getDatabaseComment());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->isExternal());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verificationStateName(verification_status));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verificationThrottleReasonName(verification_status.last_throttle_reason));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verificationLastError(verification_status));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(overrideStateName(
                verification_status.verification_scheduler_override_configured,
                verification_status.verification_scheduler_override_effective,
                verification_status.verification_scheduler_override_persisted));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(overrideStateName(
                verification_status.database_resource_quota_override_configured,
                verification_status.database_resource_quota_override_effective,
                verification_status.database_resource_quota_override_persisted));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.runs);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.cached_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.planned_batches);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.planned_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.terminal_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.verified_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.damaged_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.cursor_advances);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.incomplete_batches);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_completed_rotations);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_root_catalog_epoch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(
                verification_status.last_root_catalog_epoch ? digestToLowerHex(verification_status.last_root_authority_anchor) : String{});
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_successful_root_catalog_epoch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(
                verification_status.last_successful_root_catalog_epoch
                    ? digestToLowerHex(verification_status.last_successful_root_authority_anchor)
                    : String{});
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_planned_batch_sequence);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.failures);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.throttles);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.foreground_load_throttles);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.background_load_throttles);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.wall_time_budget_yields);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.cpu_time_budget_yields);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_observed_foreground_queries);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_observed_competing_background_tasks);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.repair_attempts);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.repair_successes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.repair_unavailable);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_transaction_id);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_local_wal_sources);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_replicated_authority_sources);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_verified_backup_sources);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_provenance_available);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_damaged_artifacts);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(
                verification_status.last_repair_provenance_available
                    ? digestToLowerHex(verification_status.last_repair_damaged_artifact_manifest_digest)
                    : String{});
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_previous_catalog_epoch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(
                verification_status.last_repair_provenance_available
                    ? digestToLowerHex(verification_status.last_repair_previous_authority_anchor)
                    : String{});
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.last_repair_repaired_catalog_epoch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(
                verification_status.last_repair_provenance_available
                    ? digestToLowerHex(verification_status.last_repair_repaired_authority_anchor)
                    : String{});
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.runtime_status_available);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.runtime_fail_closed);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.runtime_revision);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.quarantine_failing_seeds);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.quarantined_objects);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_snapshot_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_targets_per_batch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_buckets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_reverse_dependency_count);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_canonical_bytes_per_batch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_work_units_per_batch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_transient_bytes_per_batch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_io_bytes_per_batch);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_rooted_target_canonical_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_rooted_target_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_rooted_target_transient_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_rooted_target_io_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_planner_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_planner_scratch_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_planner_retained_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_cooperative_work_items_per_pass);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_run_wall_time_ms);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_run_cpu_time_ms);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_successful_batch_interval_ms);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_load_throttle_retry_interval_ms);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_foreground_queries_for_admission);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_maximum_competing_background_tasks_for_admission);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.effective_os_thread_nice_value);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(rootQuotaStateName(verification_status));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_revision);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_definitions);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_deterministic_catalog_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_buckets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_canonical_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_transient_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_io_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_planner_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_planner_scratch_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_verification_retained_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_durable_dependent_object_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_definitions);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_deterministic_catalog_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_targets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_buckets);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_canonical_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_transient_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_io_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_planner_work_units);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_planner_scratch_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_verification_retained_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_durable_dependent_object_bytes);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_occurrence_paths_per_object);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_persisted_specializations_per_template);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_limit_sidecar_bytes_per_object);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_maximum_occurrence_paths_per_object);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_maximum_persisted_specializations_per_template);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_quota_maximum_sidecar_bytes_per_object);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_usage_dependent_objects);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_usage_total_occurrence_paths);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(verification_status.root_usage_unique_persisted_specializations);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB
{
REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDatabases)
}
