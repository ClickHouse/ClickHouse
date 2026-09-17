#include <Access/ContextAccess.h>
#include <Storages/System/SystemTableSourceRegistry.h>
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
    auto description = ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Database name."},
        {"engine", std::make_shared<DataTypeString>(), "Database engine."},
        {"data_path", std::make_shared<DataTypeString>(), "Data path."},
        {"metadata_path", std::make_shared<DataTypeString>(), "Metadata path."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Database UUID."},
        {"engine_full", std::make_shared<DataTypeString>(), "Parameters of the database engine."},
        {"comment", std::make_shared<DataTypeString>(), "Database comment."},
        {"is_external", std::make_shared<DataTypeUInt8>(), "Database is external (i.e. PostgreSQL/DataLakeCatalog)."},
    };

    description.setAliases({
        {"database", std::make_shared<DataTypeString>(), "name"}
    });

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
{ REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDatabases)
}
