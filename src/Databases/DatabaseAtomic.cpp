#include <exception>
#include <filesystem>
#include <thread>
#include <Access/UDTUsageAccess.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AtomicAuthorityStartup.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AtomicLifecycleAdapter.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Databases/UDT/DatabaseResourceQuotaSettings.h>
#include <Databases/UDT/ResourceLimitAdapters.h>
#include <Disks/IStoragePolicy.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageView.h>
#include <Storages/Utils.h>
#include <base/getMemoryAmount.h>
#include <base/isSharedPtrUnique.h>
#include <base/scope_guard.h>
#include <Common/AsyncLoader.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/PoolId.h>
#include <Common/ProfileEvents.h>
#include <Common/UniqueLock.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/atomicRename.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace ProfileEvents
{
extern const Event UDTAuthorityMappedOperationAdmissions;
extern const Event UDTAuthorityMappedOperationRejections;
}

namespace DB
{
namespace FailPoints
{
extern const char udt_authority_shutdown_pause_before_fence[];
}

namespace Setting
{
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsBool check_table_dependencies;
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_query_size;
} // namespace Setting

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_DATABASE;
    extern const int TABLE_ALREADY_EXISTS;
extern const int CANNOT_ASSIGN_ALTER;
extern const int DATABASE_NOT_EMPTY;
extern const int NOT_IMPLEMENTED;
extern const int FILE_ALREADY_EXISTS;
extern const int INCORRECT_QUERY;
extern const int ABORTED;
extern const int UNKNOWN_TYPE;
extern const int LOGICAL_ERROR;
extern const int UNFINISHED;
extern const int QUERY_IS_TOO_LARGE;
} // namespace ErrorCodes

namespace DatabaseMetadataDiskSetting
{
extern const DatabaseMetadataDiskSettingsString disk;
}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base) noexcept
        : DatabaseTablesSnapshotIterator(std::move(base))
    {
    }
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

namespace UDT
{;

} // namespace UDT

namespace
{

constexpr UInt64 maximum_concurrent_udt_restore_publication_leases = 65'536;

UDT::SchemaObjectKind mappedSchemaObjectKindForStorage(const IStorage & storage) noexcept
{
    if (storage.isDictionary())
        return UDT::SchemaObjectKind::Dictionary;
    if (storage.isView())
        return UDT::SchemaObjectKind::View;
    return UDT::SchemaObjectKind::Table;
}

UDT::AuthorityRootGraphIdentity authorityRootGraphIdentity(const UDT::AuthorityRoot & root)
{
    const auto & state = root.getAuthorityState();
    return {
        .authority_root = {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        .schema_graph_root = state.schema_graph_root,
    };
}

UDT::AuthorityVerificationSchedulerLimits applyEffectiveDatabaseVerificationLimits(
    UDT::AuthorityVerificationSchedulerLimits scheduler_limits,
    const UDT::EffectiveResourceLimits & database_limits,
    const UDT::AuthorityRoot * existing_root = nullptr)
{
    const auto resource_schedule = UDT::makeAuthorityVerificationScheduleLimits(database_limits);
    scheduler_limits.schedule.maximum_snapshot_targets
        = std::min(scheduler_limits.schedule.maximum_snapshot_targets, resource_schedule.maximum_snapshot_targets);
    scheduler_limits.schedule.maximum_targets_per_batch
        = std::min(scheduler_limits.schedule.maximum_targets_per_batch, resource_schedule.maximum_targets_per_batch);
    scheduler_limits.schedule.maximum_buckets = std::min(scheduler_limits.schedule.maximum_buckets, resource_schedule.maximum_buckets);
    scheduler_limits.schedule.maximum_canonical_bytes_per_batch
        = std::min(scheduler_limits.schedule.maximum_canonical_bytes_per_batch, resource_schedule.maximum_canonical_bytes_per_batch);
    scheduler_limits.schedule.maximum_verification_work_units_per_batch = std::min(
        scheduler_limits.schedule.maximum_verification_work_units_per_batch, resource_schedule.maximum_verification_work_units_per_batch);
    scheduler_limits.schedule.maximum_transient_bytes_per_batch
        = std::min(scheduler_limits.schedule.maximum_transient_bytes_per_batch, resource_schedule.maximum_transient_bytes_per_batch);
    scheduler_limits.schedule.maximum_io_bytes_per_batch
        = std::min(scheduler_limits.schedule.maximum_io_bytes_per_batch, resource_schedule.maximum_io_bytes_per_batch);
    scheduler_limits.schedule.maximum_planner_work_units
        = std::min(scheduler_limits.schedule.maximum_planner_work_units, resource_schedule.maximum_planner_work_units);
    scheduler_limits.schedule.maximum_planner_scratch_bytes
        = std::min(scheduler_limits.schedule.maximum_planner_scratch_bytes, resource_schedule.maximum_planner_scratch_bytes);
    scheduler_limits.schedule.maximum_retained_canonical_bytes
        = std::min(scheduler_limits.schedule.maximum_retained_canonical_bytes, resource_schedule.maximum_retained_canonical_bytes);
    scheduler_limits.schedule.maximum_rooted_target_canonical_bytes = scheduler_limits.schedule.maximum_canonical_bytes_per_batch;
    scheduler_limits.schedule.maximum_rooted_target_verification_work_units
        = scheduler_limits.schedule.maximum_verification_work_units_per_batch;
    scheduler_limits.schedule.maximum_rooted_target_transient_bytes = scheduler_limits.schedule.maximum_transient_bytes_per_batch;
    scheduler_limits.schedule.maximum_rooted_target_io_bytes = scheduler_limits.schedule.maximum_io_bytes_per_batch;

    if (existing_root)
    {
        /// Mutable policy is an admission ceiling, not a kill switch. Widen
        /// only the exact immutable-root requirements: aggregate batch caps
        /// stay lowered, while one indivisible rooted target and deterministic
        /// planning state remain executable. The root's quota state is not
        /// changed by this process-local escape.
        constexpr UDT::AuthorityVerificationScheduleLimits implementation;
        const auto & usage = existing_root->getDatabaseResourceQuota().getUsage();
        const UInt64 existing_snapshot_targets = existing_root->getInventorySummary().leaf_count;
        const auto widen = [](UInt64 configured, UInt64 rooted, UInt64 hard_maximum, std::string_view description)
        {
            if (rooted > hard_maximum)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Existing Atomic UDT authority {} exceeds its verifier implementation domain", description);
            return std::max(configured, rooted);
        };
        scheduler_limits.schedule.maximum_snapshot_targets = widen(
            scheduler_limits.schedule.maximum_snapshot_targets,
            existing_snapshot_targets,
            implementation.maximum_snapshot_targets,
            "inventory");
        scheduler_limits.schedule.maximum_buckets = widen(
            scheduler_limits.schedule.maximum_buckets,
            scheduler_limits.policy.bucket_count,
            implementation.maximum_buckets,
            "bucket topology");
        scheduler_limits.schedule.maximum_rooted_target_canonical_bytes = widen(
            scheduler_limits.schedule.maximum_rooted_target_canonical_bytes,
            usage.get(UDT::ResourceLimit::VerificationCanonicalBytesPerBatch),
            implementation.maximum_rooted_target_canonical_bytes,
            "rooted canonical target requirement");
        scheduler_limits.schedule.maximum_rooted_target_verification_work_units = widen(
            scheduler_limits.schedule.maximum_rooted_target_verification_work_units,
            usage.get(UDT::ResourceLimit::VerificationWorkUnitsPerBatch),
            implementation.maximum_rooted_target_verification_work_units,
            "rooted verification-work requirement");
        scheduler_limits.schedule.maximum_rooted_target_transient_bytes = widen(
            scheduler_limits.schedule.maximum_rooted_target_transient_bytes,
            usage.get(UDT::ResourceLimit::VerificationTransientBytesPerBatch),
            implementation.maximum_rooted_target_transient_bytes,
            "rooted transient requirement");
        scheduler_limits.schedule.maximum_rooted_target_io_bytes = widen(
            scheduler_limits.schedule.maximum_rooted_target_io_bytes,
            usage.get(UDT::ResourceLimit::VerificationIOBytesPerBatch),
            implementation.maximum_rooted_target_io_bytes,
            "rooted I/O requirement");

        const auto planning = UDT::computeAuthorityVerificationPlanningRequirements(
            existing_snapshot_targets, scheduler_limits.policy, scheduler_limits.schedule.maximum_targets_per_batch);
        scheduler_limits.schedule.maximum_planner_work_units = widen(
            scheduler_limits.schedule.maximum_planner_work_units,
            planning.planner_work_units,
            implementation.maximum_planner_work_units,
            "planner-work requirement");
        scheduler_limits.schedule.maximum_planner_scratch_bytes = widen(
            scheduler_limits.schedule.maximum_planner_scratch_bytes,
            planning.planner_scratch_bytes,
            implementation.maximum_planner_scratch_bytes,
            "planner-scratch requirement");
        scheduler_limits.schedule.maximum_retained_canonical_bytes = widen(
            scheduler_limits.schedule.maximum_retained_canonical_bytes,
            planning.retained_canonical_bytes,
            implementation.maximum_retained_canonical_bytes,
            "planner-retained requirement");
    }

    /// One cooperative pass can never consume more snapshot/planning items
    /// than exist in the effective target domain. This also keeps a lowered
    /// target quota internally valid for a newly admitted database.
    scheduler_limits.maximum_snapshot_targets_per_pass
        = std::min(scheduler_limits.maximum_snapshot_targets_per_pass, scheduler_limits.schedule.maximum_snapshot_targets);

    /// Exact-repair release reuses the same target snapshot, planner and
    /// executor boundary as periodic verification. It must therefore inherit
    /// the same effective two-domain limits instead of silently retaining the
    /// implementation defaults.
    scheduler_limits.automatic_repair.execution.verification_schedule = scheduler_limits.schedule;
    scheduler_limits.automatic_repair.execution.verification_executor.object_verifier = scheduler_limits.executor.object_verifier;
    scheduler_limits.automatic_repair.execution.verification_executor.maximum_terminal_targets
        = scheduler_limits.executor.maximum_terminal_targets;
    return UDT::AuthorityVerificationScheduler::validateEffectiveLimits(std::move(scheduler_limits));
}

} // namespace

struct DatabaseAtomic::UDTAuthorityConfiguration final
{
    UDTAuthorityConfiguration(
        UDT::AuthorityVerificationSchedulerLimits global_verification_scheduler_limits_,
        UDT::AtomicDatabaseUDTPersistedConfigurationV2 configured_persisted_configuration_,
        UDT::ResourceLimitLayer server_resource_limit_layer_,
        UDT::EffectiveResourceLimits effective_database_limits_,
        UDT::AuthorityVerificationSchedulerLimits effective_verification_scheduler_limits_)
        : global_verification_scheduler_limits(std::move(global_verification_scheduler_limits_))
        , configured_persisted_configuration(std::move(configured_persisted_configuration_))
        , selected_persisted_configuration(configured_persisted_configuration)
        , server_resource_limit_layer(std::move(server_resource_limit_layer_))
        , effective_database_limits(std::move(effective_database_limits_))
        , effective_verification_scheduler_limits(std::move(effective_verification_scheduler_limits_))
    {
    }

    UDT::AuthorityVerificationSchedulerLimits global_verification_scheduler_limits;
    UDT::AtomicDatabaseUDTPersistedConfigurationV2 configured_persisted_configuration;
    UDT::AtomicDatabaseUDTPersistedConfigurationV2 selected_persisted_configuration;
    UDT::ResourceLimitLayer server_resource_limit_layer;
    UDT::EffectiveResourceLimits effective_database_limits;
    UDT::AuthorityVerificationSchedulerLimits effective_verification_scheduler_limits;
};

DatabaseAtomic::DatabaseAtomic(
    String name_,
    String metadata_path_,
    UUID uuid,
    const String & logger_name,
    ContextPtr context_,
    DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseAtomic(
          std::move(name_),
          std::move(metadata_path_),
          uuid,
          logger_name,
          context_,
          AuthorityMode::Enabled,
          std::move(database_metadata_disk_settings_))
{
}

DatabaseAtomic::DatabaseAtomic(
    String name_,
    String metadata_path_,
    UUID uuid,
    const String & logger_name,
    ContextPtr context_,
    AuthorityMode udt_authority_mode_,
    DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseOrdinary(
          name_, metadata_path_, DatabaseCatalog::getStoreDirPath() / "", logger_name, context_, database_metadata_disk_settings_)
    , path_to_table_symlinks(DatabaseCatalog::getDataDirPath(name_) / "")
    , path_to_metadata_symlink(DatabaseCatalog::getMetadataDirPath(name_))
    , db_uuid(uuid)
    , udt_authority_mode(udt_authority_mode_)
    , udt_lifecycle_adapter(udt_authority_mode == AuthorityMode::Enabled ? std::make_unique<UDT::AtomicLifecycleAdapter>(*this) : nullptr)
{
    chassert(db_uuid != UUIDHelpers::Nil);
    if (udt_authority_mode == AuthorityMode::Enabled)
    {
        const auto & config = getContext()->getConfigRef();
        auto scheduler_configuration = UDT::resolveAuthorityVerificationSchedulerConfigurationFromConfig(config, db_uuid);
        auto quota_configuration
            = UDT::resolveDatabaseResourceQuotaConfigurationFromConfig(config, db_uuid, static_cast<UInt64>(getMemoryAmount()));
        UDT::AtomicDatabaseUDTPersistedConfigurationV2 persisted_configuration{
            .verification_scheduler_override = std::move(scheduler_configuration.encoded_database_override),
            .resource_quota_override = std::move(quota_configuration.encoded_database_override),
        };
        const auto database_layer = persisted_configuration.resource_quota_override
            ? UDT::decodeDatabaseResourceQuotaOverrideV2(*persisted_configuration.resource_quota_override, db_uuid)
            : UDT::makeDatabaseDefaultResourceLimitLayer();
        auto effective_database_limits = UDT::calculateEffectiveDatabaseResourceLimits(
            quota_configuration.server_layer, database_layer, UDT::atomicDatabaseAuthorityCapabilities().limits);
        auto effective_scheduler_limits = persisted_configuration.verification_scheduler_override
            ? UDT::mergeAuthorityVerificationSchedulerLimits(
                  scheduler_configuration.global_limits,
                  UDT::decodeAuthorityVerificationSchedulerOverrideV2(*persisted_configuration.verification_scheduler_override, db_uuid))
            : UDT::AuthorityVerificationScheduler::validateEffectiveLimits(scheduler_configuration.global_limits);
        udt_authority_configuration = std::make_unique<UDTAuthorityConfiguration>(
            std::move(scheduler_configuration.global_limits),
            std::move(persisted_configuration),
            std::move(quota_configuration.server_layer),
            std::move(effective_database_limits),
            std::move(effective_scheduler_limits));
        udt_lifecycle_adapter->configureEffectiveDatabaseResourceLimitsForStartup(udt_authority_configuration->effective_database_limits);
    }
}

DatabaseAtomic::DatabaseAtomic(
    String name_, String metadata_path_, UUID uuid, ContextPtr context_, DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseAtomic(name_, std::move(metadata_path_), uuid, "DatabaseAtomic (" + name_ + ")", context_, database_metadata_disk_settings_)
{
}

DatabaseAtomic::~DatabaseAtomic()
{
    try
    {
        DatabaseAtomic::shutdown();
    }
    catch (...)
    {
        active_udt_authority.store(nullptr, std::memory_order_release);
        active_udt_verification_runtime.store(nullptr, std::memory_order_release);
        if (udt_authority)
            udt_authority->setPublicationObserver(nullptr);
        if (udt_verification_scheduler)
            udt_verification_scheduler->shutdownAndDrain();
        if (udt_verification_runtime)
            udt_verification_runtime->shutdownAndDrain();
        if (udt_authority)
            udt_authority->shutdownAndDrain();
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

const UDT::TypeAuthorityCapabilities & DatabaseAtomic::getSupportedUDTAuthorityCapabilities() const noexcept
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
        return IDatabase::getSupportedUDTAuthorityCapabilities();
    static constexpr auto capabilities = UDT::atomicDatabaseAuthorityCapabilities();
    return capabilities;
}

const UDT::IAuthorityAdapter & DatabaseAtomic::getUDTAuthorityAdapter() const noexcept
{
    if (auto * authority = active_udt_authority.load(std::memory_order_acquire))
        return *authority;
    return UDT::getUnsupportedAuthorityAdapter();
}

UDT::ILifecycleAdapter & DatabaseAtomic::getUDTLifecycleAdapter() noexcept
{
    if (udt_authority_mode == AuthorityMode::Enabled)
        return *udt_lifecycle_adapter;
    return UDT::getUnsupportedLifecycleAdapter();
}

UDT::AtomicAuthority &
DatabaseAtomic::initializeUDTAuthorityUnlocked(std::unique_ptr<const UDT::AuthorityRoot> recovered_root, bool activate_recovered_authority)
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} databases cannot activate durable user-defined types", getEngineName());
    if (udt_authority_shutdown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot activate user-defined types after database shutdown");
    if (udt_degraded_startup_status)
        throw Exception(ErrorCodes::ABORTED, "Cannot activate an invalid or incomplete recovered user-defined type authority");
    if (udt_authority)
    {
        if (recovered_root || !udt_verification_runtime || !udt_verification_scheduler)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "User-defined type authority is already initialized");
        return *udt_authority;
    }

    if (activate_recovered_authority && !recovered_root)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot activate an Atomic user-defined type authority "
            "without a recovered root");
    if (!udt_authority_configuration)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT authority configuration was not initialized");
    std::unique_ptr<const UDT::DatabaseSchemaWALExactRepairProvenance> recovered_repair_provenance;
    if (recovered_root)
    {
        if (!udt_mutation_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Recovered Atomic authority has no durable mutation storage");
        if (auto provenance = udt_mutation_storage->loadLatestExactRepairProvenance())
        {
            recovered_repair_provenance = std::make_unique<const UDT::DatabaseSchemaWALExactRepairProvenance>(std::move(*provenance));
        }
    }
    auto verification_scheduler_limits = applyEffectiveDatabaseVerificationLimits(
        udt_authority_configuration->effective_verification_scheduler_limits,
        udt_authority_configuration->effective_database_limits,
        recovered_root.get());
    if (recovered_root)
    {
        recovered_root = recovered_root->cloneWithVerificationPlanningDomainForStartup(
            verification_scheduler_limits.policy, verification_scheduler_limits.schedule.maximum_targets_per_batch);
    }
    auto verification_cursor = UDT::makeAuthorityVerificationScheduleCursor(
        db_uuid, verification_scheduler_limits.policy, verification_scheduler_limits.schedule);
    if (activate_recovered_authority)
    {
        if (!udt_mutation_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Recovered Atomic authority has no durable mutation storage");
        bool persist_fresh_cursor = true;
        if (auto durable_cursor = udt_mutation_storage->loadAuthorityVerificationCursor())
        {
            if (durable_cursor->contract_abi != UDT::authority_verification_schedule_contract_abi
                || durable_cursor->database_uuid != db_uuid)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Persisted Atomic UDT verification cursor has an incompatible contract or database identity");
            }
            if (durable_cursor->bucket_count == verification_scheduler_limits.policy.bucket_count
                && durable_cursor->bucket_seed == verification_scheduler_limits.policy.bucket_seed)
            {
                verification_cursor = std::move(*durable_cursor);
                persist_fresh_cursor = false;
            }
            else
            {
                /// Bucket count/seed are administrator-owned scheduling policy,
                /// not authority truth. A validated policy change starts a fresh
                /// deterministic rotation instead of making the database unable
                /// to start; the next complete clean batch replaces the old cursor.
                LOG_INFO(
                    log,
                    "Resetting Atomic UDT verification rotation after scheduler policy changed from {}/{} to {}/{}",
                    durable_cursor->bucket_count,
                    durable_cursor->bucket_seed,
                    verification_scheduler_limits.policy.bucket_count,
                    verification_scheduler_limits.policy.bucket_seed);
            }
        }
        if (persist_fresh_cursor)
        {
            /// Cursor policy is part of the durable scheduler identity. Publish
            /// a fresh zero-progress cursor while startup still owns schema
            /// serialization, before exposing the runtime or scheduling work.
            udt_mutation_storage->persistAuthorityVerificationCursor(verification_cursor);
        }
    }
    auto verification_runtime = std::make_unique<UDT::AuthorityVerificationRuntimeState>(db_uuid, std::move(verification_cursor));
    auto verification_scheduler = std::make_unique<UDT::AuthorityVerificationScheduler>(*this, verification_scheduler_limits);
    auto authority = std::make_unique<UDT::AtomicAuthority>(db_uuid, getSupportedUDTAuthorityCapabilities(), std::move(recovered_root));
    auto * result = authority.get();
    auto * runtime = verification_runtime.get();
    udt_verification_runtime = std::move(verification_runtime);
    udt_verification_scheduler = std::move(verification_scheduler);
    udt_last_exact_repair_provenance = std::move(recovered_repair_provenance);
    udt_authority = std::move(authority);
    udt_authority->setPublicationObserver(udt_verification_runtime.get());
    if (activate_recovered_authority)
    {
        active_udt_verification_runtime.store(runtime, std::memory_order_release);
        active_udt_authority.store(result, std::memory_order_release);
    }
    return *result;
}

void DatabaseAtomic::activateUDTAuthorityAfterFirstPublication() noexcept
{
    std::lock_guard lock(udt_authority_mutex);
    if (udt_authority_mode == AuthorityMode::Unsupported || !udt_authority || !udt_verification_runtime || !udt_verification_scheduler)
        std::terminate();

    auto * authority = udt_authority.get();
    auto * runtime = udt_verification_runtime.get();
    auto * active = active_udt_authority.load(std::memory_order_acquire);
    if (udt_authority_shutdown)
    {
        /// The first mutation may already own the schema fence when shutdown
        /// publishes its latch. Its durable Commit remains successful, but the
        /// newly built runtime must stay private so shutdown can drain it and
        /// startup can recover the committed root on the next process image.
        if (active || active_udt_verification_runtime.load(std::memory_order_acquire) || !authority->isFirstPublicationReadyForActivation())
            std::terminate();
        return;
    }
    if (active == authority)
    {
        if (active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
            std::terminate();
        if (udt_database_startup_complete.load(std::memory_order_acquire))
            udt_verification_scheduler->activateAfterDatabaseStartup();
        return;
    }
    if (active || !authority->isFirstPublicationReadyForActivation())
        std::terminate();
    active_udt_verification_runtime.store(runtime, std::memory_order_release);
    active_udt_authority.store(authority, std::memory_order_release);
    if (udt_database_startup_complete.load(std::memory_order_acquire))
        udt_verification_scheduler->activateAfterDatabaseStartup();
}

void DatabaseAtomic::transitionPendingUDTAuthorityToDegraded(std::unique_lock<std::mutex> schema_mutation_lock)
{
    if (!schema_mutation_lock.owns_lock() || schema_mutation_lock.mutex() != &udt_schema_mutation_mutex)
        std::terminate();

    std::unique_ptr<UDT::AuthorityVerificationScheduler> failed_scheduler;
    std::unique_ptr<UDT::AuthorityVerificationRuntimeState> failed_runtime;
    std::unique_ptr<UDT::AtomicAuthority> failed_authority;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        /// shutdown() owns the pending state after publishing this latch while
        /// holding the same schema->authority lock order. A late AsyncLoader
        /// failure must yield to that cleanup and must not publish a degraded
        /// image after shutdown has begun.
        if (udt_authority_shutdown)
            return;
        if (!udt_table_startup_state || !udt_table_startup_state->unavailable_root_status || udt_degraded_startup_status || !udt_authority
            || !udt_verification_runtime || !udt_verification_scheduler || active_udt_authority.load(std::memory_order_acquire)
            || active_udt_verification_runtime.load(std::memory_order_acquire))
        {
            std::terminate();
        }

        active_udt_authority.store(nullptr, std::memory_order_release);
        active_udt_verification_runtime.store(nullptr, std::memory_order_release);
        udt_degraded_startup_status = std::move(udt_table_startup_state->unavailable_root_status);
        udt_table_startup_state.reset();
        udt_authority->setPublicationObserver(nullptr);
        failed_scheduler = std::move(udt_verification_scheduler);
        failed_runtime = std::move(udt_verification_runtime);
        failed_authority = std::move(udt_authority);
    }

    /// Worker and hazard draining may re-enter unrelated database-owned
    /// resources. The durable storage and degraded image are already visible;
    /// release schema serialization before destroying the private runtime.
    schema_mutation_lock.unlock();
    if (failed_scheduler)
    {
        failed_scheduler->requestStop();
        failed_scheduler->shutdownAndDrain();
    }
    if (failed_runtime)
        failed_runtime->shutdownAndDrain();
    if (failed_authority)
        failed_authority->shutdownAndDrain();
}

bool DatabaseAtomic::hasActiveUDTAuthority() const noexcept
{
    return active_udt_authority.load(std::memory_order_acquire) != nullptr;
}

bool DatabaseAtomic::hasDurableUDTAuthorityState() const
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
        return false;
    std::lock_guard authority_lock(udt_authority_mutex);
    return udt_mutation_storage && udt_mutation_storage->hasDurableAuthorityMarker();
}

UDT::AuthorityQuarantineAdmissionDecision DatabaseAtomic::decideUDTQuarantineAdmission(
    const UDT::AuthorityQuarantineOperationView & operation, const UDT::AuthorityQuarantineAdmissionLimits & limits) const noexcept
{
    if (!active_udt_authority.load(std::memory_order_acquire))
        return {.status = UDT::AuthorityQuarantineAdmissionStatus::RuntimeFailClosed, .statistics = {}};
    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!runtime)
        return {.status = UDT::AuthorityQuarantineAdmissionStatus::RuntimeFailClosed, .statistics = {}};
    return runtime->decideOperation(operation, limits);
}

void DatabaseAtomic::assertUDTTypeLifecycleOperationAllowed(
    const UDT::AuthorityRoot * exact_active_root,
    std::span<const UDT::SchemaObjectID> sorted_unique_touched_objects,
    std::string_view operation) const
{
    using UDT::AtomicAuthority;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::AuthorityQuarantineOperationTiming;
    using UDT::AuthorityVerificationRuntimeState;

    if (operation.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT lifecycle quarantine gate has no operation name");

    /// RESTORE owns no authority manifest. Once its inactive preflight has
    /// succeeded, the first type mutation must not install even private
    /// authority components until every restored-object publication releases
    /// its lease.
    if (!exact_active_root)
    {
        if (udt_restore_publication_leases.load(std::memory_order_acquire))
        {
            throw Exception(ErrorCodes::ABORTED, "Cannot {} while an Atomic RESTORE publication is in flight", operation);
        }

        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown)
            throw Exception(ErrorCodes::ABORTED, "Cannot {} after Atomic database shutdown", operation);
        if (udt_table_startup_state || udt_degraded_startup_status || udt_authority || udt_mutation_storage || udt_verification_runtime
            || udt_verification_scheduler || active_udt_authority.load(std::memory_order_acquire)
            || active_udt_verification_runtime.load(std::memory_order_acquire))
        {
            throw Exception(
                ErrorCodes::ABORTED, "Cannot {} because the Atomic UDT authority no longer has the exact never-enabled image", operation);
        }
        return;
    }

    if (sorted_unique_touched_objects.empty() || !std::is_sorted(sorted_unique_touched_objects.begin(), sorted_unique_touched_objects.end())
        || std::adjacent_find(sorted_unique_touched_objects.begin(), sorted_unique_touched_objects.end())
            != sorted_unique_touched_objects.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Atomic UDT lifecycle operation {} supplied an incomplete or non-canonical touch set", operation);
    }
    for (const auto & object : sorted_unique_touched_objects)
    {
        if (!object.isValid() || object.database_uuid != db_uuid)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Atomic UDT lifecycle operation {} supplied a foreign or invalid touched object", operation);
        }
    }

    AtomicAuthority * authority = nullptr;
    AuthorityVerificationRuntimeState * runtime = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> current_snapshot;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown || udt_table_startup_state || udt_degraded_startup_status)
            throw Exception(ErrorCodes::ABORTED, "Cannot {} because the Atomic UDT authority is unavailable", operation);
        authority = udt_authority.get();
        runtime = udt_verification_runtime.get();
        if (!authority || !runtime || active_udt_authority.load(std::memory_order_acquire) != authority
            || active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        {
            throw Exception(ErrorCodes::ABORTED, "Cannot {} without one exact active Atomic UDT authority runtime", operation);
        }
        current_snapshot.emplace(authority->acquireCurrentRoot());
        if (!*current_snapshot || std::addressof(current_snapshot->get()) != exact_active_root)
        {
            throw Exception(
                ErrorCodes::ABORTED, "Cannot {} because the Atomic UDT authority root changed before quarantine admission", operation);
        }
    }

    const auto decision = runtime->decideOperation({
        .kind = AuthorityQuarantineOperationKind::DDL,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = authorityRootGraphIdentity(current_snapshot->get()),
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = sorted_unique_touched_objects,
        .continuation_proof_set_is_complete = false,
        .sorted_unique_continuation_proofs = {},
    });
    if (!decision.isAllowed())
    {
        throw Exception(
            ErrorCodes::ABORTED, "Atomic UDT quarantine rejected {} (status {})", operation, static_cast<unsigned>(decision.status));
    }
}

void DatabaseAtomic::assertUDTNewDefinitionClosureOperationAllowed(
    const UDT::BoundObjectTypeReferences & bound_references, UDT::AuthorityQuarantineOperationKind kind) const
{
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::AuthorityQuarantineOperationTiming;
    using UDT::collectAuthorityVerificationRequiredDefinitions;
    using UDT::SchemaObjectID;
    using UDT::SchemaObjectKind;

    if (kind != AuthorityQuarantineOperationKind::DDL && kind != AuthorityQuarantineOperationKind::Attach)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT prospective definition-closure gate received an invalid operation kind");
    auto * authority = active_udt_authority.load(std::memory_order_acquire);
    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!authority || !runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT prospective definition-closure gate has no active authority runtime");
    auto root = authority->acquireCurrentRoot();
    if (!root || active_udt_authority.load(std::memory_order_acquire) != authority
        || active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT authority changed during prospective definition-closure admission");
    const auto required_definitions = collectAuthorityVerificationRequiredDefinitions(bound_references);
    std::vector<SchemaObjectID> touched;
    touched.reserve(required_definitions.size());
    for (const auto & definition : required_definitions)
    {
        const auto rooted_definition = root.get().findByIdentity(definition);
        if (!rooted_definition)
            throw Exception(ErrorCodes::ABORTED, "Atomic UDT prospective definition closure is stale");
        touched.push_back({
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = definition.database_uuid,
            .object_uuid = definition.type_uuid,
        });
    }
    std::sort(touched.begin(), touched.end());
    touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
    const auto decision = runtime->decideOperation({
        .kind = kind,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = authorityRootGraphIdentity(root.get()),
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = touched,
        .continuation_proof_set_is_complete = false,
        .sorted_unique_continuation_proofs = {},
    });
    if (!decision.isAllowed())
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Atomic UDT quarantine rejected a prospective definition-closure operation (status {})",
            static_cast<unsigned>(decision.status));
    }
}

UDT::AuthorityVerificationScheduleCursor DatabaseAtomic::getUDTAuthorityVerificationCursor() const
{
    waitDatabaseStarted();
    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!active_udt_authority.load(std::memory_order_acquire) || !runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic user-defined type verification runtime is not active");
    return runtime->getCursor();
}

UDT::AuthorityVerificationSchedulerStatus DatabaseAtomic::getUDTAuthorityVerificationSchedulerStatus() const noexcept
{
    try
    {
        std::lock_guard lock(udt_authority_mutex);
        auto status = udt_verification_scheduler ? udt_verification_scheduler->getStatus() : UDT::AuthorityVerificationSchedulerStatus{};
        if (udt_authority_configuration)
        {
            const auto & configured = udt_authority_configuration->configured_persisted_configuration;
            const auto & selected = udt_authority_configuration->selected_persisted_configuration;
            const bool authority_is_published
                = udt_authority && active_udt_authority.load(std::memory_order_acquire) == udt_authority.get();
            status.verification_scheduler_override_configured = configured.verification_scheduler_override.has_value();
            status.verification_scheduler_override_effective = selected.verification_scheduler_override.has_value();
            status.verification_scheduler_override_persisted = authority_is_published && status.verification_scheduler_override_effective;
            status.database_resource_quota_override_configured = configured.resource_quota_override.has_value();
            status.database_resource_quota_override_effective = selected.resource_quota_override.has_value();
            status.database_resource_quota_override_persisted = authority_is_published && status.database_resource_quota_override_effective;
        }
        if (udt_last_exact_repair_provenance)
        {
            const auto & provenance = *udt_last_exact_repair_provenance;
            status.last_repair_provenance_available = true;
            status.last_repair_transaction_id = provenance.transaction_id;
            status.last_repair_damaged_artifacts = provenance.damaged_artifact_count;
            status.last_repair_damaged_artifact_manifest_digest = provenance.damaged_artifact_manifest_digest;
            status.last_repair_local_wal_sources = provenance.local_wal_sources;
            status.last_repair_replicated_authority_sources = provenance.replicated_authority_sources;
            status.last_repair_verified_backup_sources = provenance.verified_backup_sources;
            status.last_repair_previous_catalog_epoch = provenance.previous_catalog_epoch;
            status.last_repair_previous_authority_anchor = provenance.previous_authority_anchor;
            status.last_repair_repaired_catalog_epoch = provenance.repaired_catalog_epoch;
            status.last_repair_repaired_authority_anchor = provenance.repaired_authority_anchor;
        }
        if (udt_degraded_startup_status)
        {
            status.scheduler_status_available = false;
            status.runtime_status_available = true;
            status.runtime_fail_closed = true;
            status.last_error_code = 0;
            switch (udt_degraded_startup_status->getGlobalStatus())
            {
                case UDT::AuthorityDefinitionStatus::Conflicted:
                    status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::StartupConflicted;
                    break;
                case UDT::AuthorityDefinitionStatus::Invalid:
                    status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::StartupInvalid;
                    break;
                case UDT::AuthorityDefinitionStatus::Incomplete:
                    status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::StartupIncomplete;
                    break;
                case UDT::AuthorityDefinitionStatus::Active:
                case UDT::AuthorityDefinitionStatus::Quarantined:
                case UDT::AuthorityDefinitionStatus::OverQuota:
                    status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed;
                    break;
            }
        }
        if (!udt_degraded_startup_status && udt_verification_runtime)
        {
            auto runtime = udt_verification_runtime->acquireSnapshot();
            status.runtime_status_available = true;
            status.runtime_fail_closed = runtime.isFailClosed()
                || active_udt_verification_runtime.load(std::memory_order_acquire) != udt_verification_runtime.get();
            status.runtime_revision = runtime.getRevision();
            if (status.runtime_fail_closed)
            {
                status.last_error_kind
                    = runtime.getLastErrorKind() == UDT::AuthorityVerificationRuntimeLastErrorKind::QuarantineConstructionFailed
                    ? UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeQuarantineConstructionFailed
                    : UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed;
                status.last_error_code = 0;
            }
            if (const auto & quarantine = runtime.getQuarantine())
            {
                status.quarantine_failing_seeds = static_cast<UInt64>(quarantine->getFailingSeeds().size());
                status.quarantined_objects = static_cast<UInt64>(quarantine->getQuarantinedObjects().size());
                if (!status.runtime_fail_closed && status.last_error_kind == UDT::AuthorityVerificationSchedulerLastErrorKind::None)
                {
                    status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::IntegrityDamageQuarantined;
                }
            }
        }
        if (!udt_degraded_startup_status && udt_authority)
        {
            auto root = udt_authority->acquireCurrentRoot();
            if (root)
            {
                const auto & quota = root.get().getDatabaseResourceQuota();
                const auto & quota_limits = quota.getLimits();
                const auto & usage = quota.getUsage();
                const auto & indexed_usage = root.get().getResourceUsageSummary();
                status.root_quota_status_available = true;
                status.root_quota_over_quota = quota.getState() == UDT::DatabaseResourceQuotaState::OverQuota;
                status.root_quota_revision = quota.getRevision();
                status.root_quota_definitions = usage.get(UDT::ResourceLimit::DefinitionsPerDatabase);
                status.root_quota_deterministic_catalog_bytes = usage.get(UDT::ResourceLimit::DeterministicCatalogBytesPerDatabase);
                status.root_quota_verification_targets = usage.get(UDT::ResourceLimit::VerificationTargetsPerDatabase);
                status.root_quota_verification_buckets = usage.get(UDT::ResourceLimit::VerificationBucketsPerDatabase);
                status.root_quota_verification_canonical_bytes = usage.get(UDT::ResourceLimit::VerificationCanonicalBytesPerBatch);
                status.root_quota_verification_work_units = usage.get(UDT::ResourceLimit::VerificationWorkUnitsPerBatch);
                status.root_quota_verification_transient_bytes = usage.get(UDT::ResourceLimit::VerificationTransientBytesPerBatch);
                status.root_quota_verification_io_bytes = usage.get(UDT::ResourceLimit::VerificationIOBytesPerBatch);
                status.root_quota_verification_planner_work_units = usage.get(UDT::ResourceLimit::VerificationPlannerWorkUnitsPerBatch);
                status.root_quota_verification_planner_scratch_bytes
                    = usage.get(UDT::ResourceLimit::VerificationPlannerScratchBytesPerBatch);
                status.root_quota_verification_retained_bytes = usage.get(UDT::ResourceLimit::VerificationRetainedBytesPerBatch);
                status.root_quota_durable_dependent_object_bytes = usage.get(UDT::ResourceLimit::DurableDependentObjectBytesPerDatabase);
                status.root_quota_limit_definitions = quota_limits.get(UDT::ResourceLimit::DefinitionsPerDatabase);
                status.root_quota_limit_deterministic_catalog_bytes
                    = quota_limits.get(UDT::ResourceLimit::DeterministicCatalogBytesPerDatabase);
                status.root_quota_limit_verification_targets = quota_limits.get(UDT::ResourceLimit::VerificationTargetsPerDatabase);
                status.root_quota_limit_verification_buckets = quota_limits.get(UDT::ResourceLimit::VerificationBucketsPerDatabase);
                status.root_quota_limit_verification_canonical_bytes
                    = quota_limits.get(UDT::ResourceLimit::VerificationCanonicalBytesPerBatch);
                status.root_quota_limit_verification_work_units = quota_limits.get(UDT::ResourceLimit::VerificationWorkUnitsPerBatch);
                status.root_quota_limit_verification_transient_bytes
                    = quota_limits.get(UDT::ResourceLimit::VerificationTransientBytesPerBatch);
                status.root_quota_limit_verification_io_bytes = quota_limits.get(UDT::ResourceLimit::VerificationIOBytesPerBatch);
                status.root_quota_limit_verification_planner_work_units
                    = quota_limits.get(UDT::ResourceLimit::VerificationPlannerWorkUnitsPerBatch);
                status.root_quota_limit_verification_planner_scratch_bytes
                    = quota_limits.get(UDT::ResourceLimit::VerificationPlannerScratchBytesPerBatch);
                status.root_quota_limit_verification_retained_bytes
                    = quota_limits.get(UDT::ResourceLimit::VerificationRetainedBytesPerBatch);
                status.root_quota_limit_durable_dependent_object_bytes
                    = quota_limits.get(UDT::ResourceLimit::DurableDependentObjectBytesPerDatabase);
                status.root_quota_limit_occurrence_paths_per_object = quota_limits.get(UDT::ResourceLimit::OccurrencePathsPerObject);
                status.root_quota_limit_persisted_specializations_per_template
                    = quota_limits.get(UDT::ResourceLimit::PersistedSpecializationsPerTemplate);
                status.root_quota_limit_sidecar_bytes_per_object = quota_limits.get(UDT::ResourceLimit::SidecarBytesPerObject);
                status.root_quota_maximum_occurrence_paths_per_object = indexed_usage.maximum_occurrence_paths_per_object;
                status.root_quota_maximum_persisted_specializations_per_template
                    = indexed_usage.maximum_persisted_specializations_per_template;
                status.root_quota_maximum_sidecar_bytes_per_object = indexed_usage.maximum_sidecar_bytes_per_object;
                status.root_usage_dependent_objects = indexed_usage.object_count;
                status.root_usage_total_occurrence_paths = indexed_usage.total_occurrence_paths;
                status.root_usage_unique_persisted_specializations = indexed_usage.unique_persisted_specializations;
            }
        }
        if (udt_authority_shutdown)
        {
            status.runtime_fail_closed = true;
            status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed;
            status.last_error_code = 0;
        }
        return status;
    }
    catch (...)
    {
        auto status = UDT::AuthorityVerificationSchedulerStatus{};
        status.runtime_fail_closed = true;
        status.last_error_kind = UDT::AuthorityVerificationSchedulerLastErrorKind::RuntimeFailClosed;
        return status;
    }
}

void DatabaseAtomic::configureUDTAuthorityVerificationSchedulerForStartup(
    const UDT::AuthorityVerificationSchedulerLimits & effective_limits)
{
    auto validated = UDT::AuthorityVerificationScheduler::validateEffectiveLimits(effective_limits);
    std::lock_guard lock(udt_authority_mutex);
    if (udt_authority_mode != AuthorityMode::Enabled)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} databases cannot configure durable UDT verification", getEngineName());
    if (udt_authority || udt_verification_scheduler || udt_database_startup_complete.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification limits must be configured before authority startup");
    if (!udt_authority_configuration)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT authority configuration was not initialized");
    udt_authority_configuration->global_verification_scheduler_limits = std::move(validated);
    const auto & persisted = udt_authority_configuration->selected_persisted_configuration.verification_scheduler_override;
    udt_authority_configuration->effective_verification_scheduler_limits = persisted
        ? UDT::mergeAuthorityVerificationSchedulerLimits(
              udt_authority_configuration->global_verification_scheduler_limits,
              UDT::decodeAuthorityVerificationSchedulerOverrideV2(*persisted, db_uuid))
        : udt_authority_configuration->global_verification_scheduler_limits;
}

UDT::PreparedAtomicDatabaseUDTConfigurationV2 DatabaseAtomic::prepareConfiguredUDTConfigurationForFirstActivationV2()
{
    std::lock_guard lock(udt_authority_mutex);
    if (udt_authority_mode != AuthorityMode::Enabled || !udt_authority_configuration || !udt_mutation_storage || !udt_authority
        || udt_authority_shutdown || active_udt_authority.load(std::memory_order_acquire))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Atomic UDT first-activation configuration requires one private initialized authority and storage");
    }
    return udt_mutation_storage->prepareUDTConfigurationForFirstActivationV2(
        udt_authority_configuration->configured_persisted_configuration);
}

const UDT::EffectiveResourceLimits & DatabaseAtomic::getConfiguredUDTEffectiveDatabaseLimitsForFirstActivation() const
{
    if (udt_authority_mode != AuthorityMode::Enabled || !udt_authority_configuration)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT database resource limits were not initialized");
    return udt_authority_configuration->effective_database_limits;
}

void DatabaseAtomic::applyConfiguredUDTVerificationLimitsForFirstActivation(UDT::AuthorityRootBuildLimits & root_limits) const
{
    if (udt_authority_mode != AuthorityMode::Enabled || !udt_authority_configuration)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT verification planning domain was not initialized");
    const auto effective = applyEffectiveDatabaseVerificationLimits(
        udt_authority_configuration->effective_verification_scheduler_limits, udt_authority_configuration->effective_database_limits);
    root_limits.verification_policy = effective.policy;
    root_limits.verification_maximum_targets_per_batch = effective.schedule.maximum_targets_per_batch;
}

std::shared_ptr<const UDT::AuthorityVerificationBatchReceipt> DatabaseAtomic::executeUDTAuthorityVerificationBatch(
    const UDT::AuthorityVerificationBatchPlan & plan,
    const UDT::AuthorityVerificationBatchExecutorLimits & limits,
    bool wait_for_startup,
    const UDT::AuthorityVerificationBatchReceipt * verified_prefix)
{
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::AuthorityVerificationBatchExecutor;
    using UDT::AuthorityVerificationRuntimeState;
    using UDT::AuthorityVerificationScheduleCursor;
    using UDT::AuthorityVerificationTrustedBatch;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::definition_authority_capability_mask;
    using UDT::dependent_object_authority_capability_mask;

    if (wait_for_startup)
        waitDatabaseStarted();
    std::unique_lock schema_lock(udt_schema_mutation_mutex);

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    AuthorityVerificationRuntimeState * runtime = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> root;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_mode != AuthorityMode::Enabled)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} databases cannot verify durable user-defined types", getEngineName());
        if (udt_authority_shutdown)
            throw Exception(ErrorCodes::ABORTED, "Cannot verify user-defined types after database shutdown");
        if (udt_table_startup_state)
            throw Exception(ErrorCodes::ABORTED, "Cannot verify user-defined types while mapped-table startup is pending");

        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        runtime = udt_verification_runtime.get();
        if (!authority || !storage || !runtime || active_udt_authority.load(std::memory_order_acquire) != authority
            || active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type verification components are inconsistent");
        root.emplace(authority->acquireCurrentRoot());
    }

    const UInt64 capability_mask = *root ? root->get().getPersistentCapabilityMask() : 0;
    if (capability_mask != definition_authority_capability_mask && capability_mask != dependent_object_authority_capability_mask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type verification has no supported authority root");
    const auto durable_state = storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != root->get().getAuthorityState())
        throw DatabaseSchemaMutationReplayConflictError("Atomic verification root differs from the durable authority head");
    if (storage->getRecoveryRequiredTransactionID())
        throw DatabaseSchemaMutationReplayConflictError("Atomic verification is fail-stopped by an incomplete schema mutation");

    AuthorityVerificationTrustedBatch trusted_batch(*this, *storage, std::move(schema_lock));
    auto receipt = AuthorityVerificationBatchExecutor::executeTrusted(*root, plan, trusted_batch, limits, verified_prefix);
    static_cast<void>(runtime->consume(
        root->get(),
        plan,
        *receipt,
        [storage](const AuthorityVerificationScheduleCursor & advanced_cursor)
        { storage->persistAuthorityVerificationCursor(advanced_cursor); }));
    return receipt;
}

void DatabaseAtomic::assertUDTDatabaseAllowsDetach(std::string_view operation) const
{
    /// The database-level DETACH path prepares and shuts down every table
    /// before it reaches the individual DatabaseAtomic::detachTable guards.
    /// Inspect the database-owned inventory up front, using a nil selector to
    /// mean "any mapped table". The inventory helper also validates that the
    /// published authority and durable WAL head agree and fails closed while
    /// recovery is pending.
    if (!hasDatabaseOwnedTableExpectationForCrossDatabaseMove(UUIDHelpers::Nil))
        return;

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot {} database {} while it contains mapped user-defined type tables; "
        "database detach metadata transactions are not implemented",
        operation,
        backQuote(getDatabaseName()));
}

DatabaseAtomic::UDTDetachGuard DatabaseAtomic::acquireUDTDatabaseDetachGuard(std::string_view operation) const
{
    waitDatabaseStarted();
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
    assertUDTDatabaseAllowsDetach(operation);
    return UDTDetachGuard(std::move(schema_mutation_lock), UDTDetachGuard::Kind::Database, {});
}

bool DatabaseAtomic::empty() const
{
    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    if (!DatabaseOrdinary::empty())
        return false;

    std::optional<UDT::AtomicAuthority::RootSnapshot> snapshot;
    UDT::AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority)
            snapshot.emplace(udt_authority->acquireCurrentRoot());
        storage = udt_mutation_storage.get();
    }

    if (snapshot && *snapshot)
        return snapshot->get().getDefinitionRecords().empty();
    return !storage || !storage->hasDurableAuthorityMarker();
}

bool DatabaseAtomic::emptyForDrop() const
{
    /// DROP removes the whole metadata directory, including the Atomic UDT
    /// authority. Definitions must still block DETACH, but not DROP once all
    /// tables have been removed under the database-exclusive DDL guard.
    return DatabaseOrdinary::empty();
}

bool DatabaseAtomic::isReservedMetadataDirectory(const String & directory_name) const
{
    if (directory_name != "types" || udt_authority_mode != AuthorityMode::Enabled)
        return false;
    std::lock_guard lock(udt_authority_mutex);
    return udt_table_startup_state || udt_degraded_startup_status || active_udt_authority.load(std::memory_order_acquire);
}

void DatabaseAtomic::reclaimRetiredUDTRootsNoThrow() noexcept
{
    /// Root reclamation may destroy the last owner of a complete authority
    /// payload. Keep the authority alive, but never run that destruction while
    /// the database schema-mutation mutex is held.
    std::lock_guard authority_lock(udt_authority_mutex);
    if (udt_authority_shutdown || !udt_authority)
        return;
    try
    {
        static_cast<void>(udt_authority->scanRetired());
    }
    catch (...)
    {
    }
}

void DatabaseAtomic::shutdown()
{
    UDT::AtomicAuthority * authority;
    UDT::AuthorityVerificationRuntimeState * verification_runtime;
    UDT::AuthorityVerificationScheduler * verification_scheduler = nullptr;
    {
        /// Publish the shutdown owner before borrowing any component pointer.
        /// The pending-startup failure transition checks this latch under the
        /// same authority mutex immediately before moving those components, so
        /// whichever side wins that mutex owns their lifetime. The schema lock
        /// below remains the final-operation fence.
        std::lock_guard authority_lock(udt_authority_mutex);
        udt_authority_shutdown = true;
        verification_scheduler = udt_verification_scheduler.get();
    }
    if (verification_scheduler)
        verification_scheduler->requestStop();
    FailPointInjection::pauseFailPoint(FailPoints::udt_authority_shutdown_pause_before_fence);
    std::unique_ptr<UDT::AtomicTableStartupState> pending_table_startup_state;
    std::shared_ptr<const UDT::AtomicAuthorityStartupStatusSnapshot> degraded_startup_status;
    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        std::lock_guard authority_lock(udt_authority_mutex);
        active_udt_authority.store(nullptr, std::memory_order_release);
        active_udt_verification_runtime.store(nullptr, std::memory_order_release);
        authority = udt_authority.get();
        verification_runtime = udt_verification_runtime.get();
        if (authority)
            authority->setPublicationObserver(nullptr);
        if (verification_scheduler != udt_verification_scheduler.get())
            std::terminate();
    }

    if (verification_scheduler)
        verification_scheduler->shutdownAndDrain();

    std::exception_ptr first_error;
    try
    {
        DatabaseOnDisk::shutdown();
    }
    catch (...)
    {
        first_error = std::current_exception();
    }

    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        std::lock_guard authority_lock(udt_authority_mutex);
        pending_table_startup_state = std::move(udt_table_startup_state);
        degraded_startup_status = std::move(udt_degraded_startup_status);
    }
    pending_table_startup_state.reset();
    degraded_startup_status.reset();
    if (verification_runtime)
        verification_runtime->shutdownAndDrain();
    if (authority)
        authority->shutdownAndDrain();
    if (first_error)
        std::rethrow_exception(first_error);
}

void DatabaseAtomic::createDirectories()
{
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
}

void DatabaseAtomic::createDirectoriesUnlocked()
{
    auto db_disk = getDisk();

    DatabaseOnDisk::createDirectoriesUnlocked();
    db_disk->createDirectories(DatabaseCatalog::getMetadataDirPath());
    if (db_disk->isSymlinkSupported())
        db_disk->createDirectories(path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database {}", table_name, database_name);
    chassert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    chassert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseAtomic::drop(ContextPtr)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::drop");
    waitDatabaseStarted();
    {
        std::lock_guard lock(mutex);
        chassert(tables.empty());
    }

    auto db_disk = getDisk();
    try
    {
        if (db_disk->isSymlinkSupported() && !db_disk->isReadOnly())
        {
            db_disk->removeFileIfExists(path_to_metadata_symlink);
            db_disk->removeRecursive(path_to_table_symlinks);
        }
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
    if (!db_disk->isReadOnly())
        db_disk->removeRecursive(getMetadataPath());
}

void DatabaseAtomic::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::attachTable");
    chassert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    ensurePopulated();

    // it is important to call the destructors of not_in_use without
    // locked mutex to avoid potential deadlock.
    DetachedTables not_in_use;
    StoragePtr detached_table;
    {
        std::lock_guard lock(mutex);
        detached_table = DatabaseOrdinary::detachTableUnlocked(name);
        table_name_to_path.erase(name);
        detached_tables.emplace(detached_table->getStorageID().uuid, detached_table);
        not_in_use = cleanupDetachedTables();
    }

    if (!not_in_use.empty())
    {
        not_in_use.clear();
        LOG_DEBUG(log, "Finished removing not used detached tables");
    }

    return detached_table;
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::dropTable");
    waitDatabaseStarted();
    auto table = tryGetTable(table_name, local_context);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (table)
        table->dropInnerTableIfAny(sync, local_context);
    else
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(table_name));

    dropTableImpl(local_context, table_name, sync);
}

void DatabaseAtomic::dropTableImpl(ContextPtr local_context, const String & table_name, bool sync)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop;
    StoragePtr table;
    auto db_disk = getDisk();
    {
        std::lock_guard lock(mutex);
        table = getTableUnlocked(table_name);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());

        db_disk->createDirectories(fs::path(table_metadata_path_drop).parent_path());

        auto txn = local_context->getZooKeeperMetadataTransaction();
        if (txn && !local_context->isInternalSubquery())
            txn->commit();      /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// We apply changes in ZooKeeper before applying changes in local metadata file
        /// to reduce probability of failures between these operations
        /// (it's more likely to lost connection, than to fail before applying local changes).
        /// TODO better detection and recovery

        db_disk->replaceFile(table_metadata_path, table_metadata_path_drop); /// Mark table as dropped
        DatabaseOrdinary::detachTableUnlocked(table_name);  /// Should never throw
        table_name_to_path.erase(table_name);
        snapshot_detached_tables.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, db_disk, table_metadata_path_drop, sync);
}

void DatabaseAtomic::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// TSA does not support conditional locking
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::renameTable");
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(&to_database))
        {
            /// Allow moving tables between Atomic and Ordinary (with table lock)
            DatabaseOnDisk::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
            return;
        }

        if (!allowMoveTableToOtherDatabaseEngine(to_database))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");
    }

    std::string message;
    if (exchange && !supportsAtomicRename(&message))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported because exchanging files is not supported by the OS ({})", message);

    createDirectories();
    waitDatabaseStarted();

    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;

    ensurePopulated();
    if (!inside_database)
        other_db.ensurePopulated();

    if (!inside_database)
        other_db.createDirectories();

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        /// Path can be not set for DDL dictionaries, but it does not matter for StorageDictionary.
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        chassert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        /// This path bypasses detachTableUnlocked, so clear stale async-load names
        /// here too, otherwise getAllTableNames keeps suggesting the old name (#91777).
        db.eraseAsyncLoadState(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_) TSA_REQUIRES(db.mutex)
    {
        db.tables.emplace(table_name_, table_);
        if (table_data_path_.empty())
            return;
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        if (table_->storesDataOnDisk())
            db.tryCreateSymlink(table_);
    };

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_)
    {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move MaterializedView with inner table to other database");
        if (const auto * ts = dynamic_cast<const StorageTimeSeries *>(table_.get()))
            if (ts->hasInnerTables())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move TimeSeries table with inner tables to other database");
    };

    String table_data_path;
    String other_table_data_path;

    if (inside_database && table_name == to_table_name)
        return;

    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;
    if (inside_database)
        db_lock = std::unique_lock{mutex};
    else if (this < &other_db)
    {
        db_lock = std::unique_lock{mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{mutex};
    }

    if (!exchange)
        other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name);

    StoragePtr table = getTableUnlocked(table_name);

    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");

    StorageID old_table_id = table->getStorageID();
    StorageID new_table_id = {other_db.database_name, to_table_name, old_table_id.uuid};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table_new_id = {database_name, table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
    }

    /// Table renaming actually begins here
    auto txn = local_context->getZooKeeperMetadataTransaction();
    if (txn && !local_context->isInternalSubquery())
        txn->commit();     /// Commit point (a sort of) for Replicated database

    auto db_disk = getDisk();

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery
    if (exchange)
        db_disk->renameExchange(old_metadata_path, new_metadata_path);
    else
        db_disk->moveFile(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not throw (if they do, it's a logical error)
    table_data_path = detach(*this, table_name, table->storesDataOnDisk());
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

    table->renameInMemory(new_table_id);
    if (exchange)
        other_table->renameInMemory(other_table_new_id);

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(*this, table_name, other_table_data_path, other_table);
}

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr query_context)
{
    auto db_disk = getDisk();

    createDirectories();
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    try
    {
        std::lock_guard lock{mutex};
        if (query.getDatabase() != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                            database_name, query.getDatabase());
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        chassert(DatabaseCatalog::instance().hasUUIDMapping(query.uuid));

        auto txn = query_context->getZooKeeperMetadataTransaction();
        if (txn && !query_context->isInternalSubquery())
            txn->commit();     /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following renameNoReplace(...)
        /// TODO better detection and recovery

        /// It throws if `table_metadata_path` already exists (it's possible if table was detached)
        db_disk->moveFile(table_metadata_tmp_path, table_metadata_path); /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
    if (table->storesDataOnDisk())
        tryCreateSymlink(table);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path,
                                      const String & /*statement*/, ContextPtr query_context)
{
    auto db_disk = getDisk();

    bool check_file_exists = true;
    SCOPE_EXIT({
        if (check_file_exists)
            db_disk->removeFileIfExists(table_metadata_tmp_path);
    });

    std::lock_guard lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot alter table because it was renamed");

    auto txn = query_context->getZooKeeperMetadataTransaction();
    if (txn && !query_context->isInternalSubquery())
        txn->commit();      /// Commit point (a sort of) for Replicated database

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    check_file_exists = db_disk->renameExchangeIfSupported(table_metadata_tmp_path, table_metadata_path);
    if (!check_file_exists)
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
}

void DatabaseAtomic::assertDetachedTableNotInUse(const UUID & uuid)
{
    /// Without this check the following race is possible since table RWLocks are not used:
    /// 1. INSERT INTO table ...;
    /// 2. DETACH TABLE table; (INSERT still in progress, it holds StoragePtr)
    /// 3. ATTACH TABLE table; (new instance of Storage with the same UUID is created, instances share data on disk)
    /// 4. INSERT INTO table ...; (both Storage instances writes data without any synchronization)
    /// To avoid it, we remember UUIDs of detached tables and does not allow ATTACH table with such UUID until detached instance still in use.
    if (detached_tables.contains(uuid))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot attach table with UUID {}, "
                        "because it was detached but still used by some query. Retry later.", uuid);
}

void DatabaseAtomic::setDetachedTableNotInUseForce(const UUID & uuid)
{
    std::lock_guard lock{mutex};
    detached_tables.erase(uuid);
}

DatabaseAtomic::DetachedTables DatabaseAtomic::cleanupDetachedTables()
{
    DetachedTables not_in_use;
    if (detached_tables.empty())
        return not_in_use;
    auto it = detached_tables.begin();
    LOG_DEBUG(log, "There are {} detached tables. Start searching non used tables.", detached_tables.size());
    while (it != detached_tables.end())
    {
        if (isSharedPtrUnique(it->second))
        {
            not_in_use.emplace(it->first, it->second);
            it = detached_tables.erase(it);
        }
        else
            ++it;
    }
    LOG_DEBUG(log, "Found {} non used tables in detached tables.", not_in_use.size());
    /// It should be destroyed in caller with released database mutex
    return not_in_use;
}

void DatabaseAtomic::assertCanBeDetached(bool cleanup)
{
    if (cleanup)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock(mutex);
            not_in_use = cleanupDetachedTables();
        }
    }
    std::lock_guard lock(mutex);
    if (!detached_tables.empty())
        throw Exception(
            ErrorCodes::DATABASE_NOT_EMPTY,
            "Database {} cannot be detached, because some tables are still in use. "
            "Retry later.",
            backQuoteIfNeed(database_name));
}

DatabaseTablesIteratorPtr
DatabaseAtomic::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    auto base_iter = DatabaseOrdinary::getTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::beforeLoadingMetadata(ContextMutablePtr /*context*/, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    if (udt_authority_mode == AuthorityMode::Enabled)
    {
        std::vector<UDT::AtomicAuthorityRecoveredDroppedTable> recovered_dropped_tables;
        {
            std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
            bool authority_is_initialized_or_shut_down;
            {
                std::lock_guard authority_lock(udt_authority_mutex);
                authority_is_initialized_or_shut_down = udt_mutation_storage || udt_authority || udt_degraded_startup_status
                    || udt_table_startup_state || udt_authority_shutdown;
            }
            if (!authority_is_initialized_or_shut_down)
            {
                const String current_database_name = getDatabaseName();
                const UDT::AtomicDatabaseSchemaMutationPaths paths(metadata_path, db_uuid, current_database_name);
                if (db_disk->existsFileOrDirectory(paths.typesDirectory()) || db_disk->existsFileOrDirectory(paths.activationMarkerPath())
                    || db_disk->existsFileOrDirectory(paths.activationMarkerTemporaryPath())
                    || db_disk->existsFileOrDirectory(paths.verificationCursorPath())
                    || db_disk->existsFileOrDirectory(paths.verificationCursorTemporaryPath())
                    || db_disk->existsFileOrDirectory(paths.udtConfigurationV2Path())
                    || db_disk->existsFileOrDirectory(paths.udtConfigurationV2TemporaryPath())
                    || db_disk->existsFileOrDirectory(paths.verificationSchedulerOverrideV2Path())
                    || db_disk->existsFileOrDirectory(paths.verificationSchedulerOverrideV2TemporaryPath())
                    || db_disk->existsFileOrDirectory(paths.resourceQuotaOverrideV2Path())
                    || db_disk->existsFileOrDirectory(paths.resourceQuotaOverrideV2TemporaryPath()))
                {
                    auto recovery_storage = std::make_unique<UDT::AtomicDatabaseSchemaMutationStorage>(
                        db_disk, db_uuid, metadata_path, current_database_name);
                    UDT::AtomicAuthorityStartupLimits startup_limits;
                    UDT::AtomicAuthorityStartupResult recovery;
                    try
                    {
                        /// A temporary-only activation marker is an interrupted
                        /// first publication. It must reach WAL recovery without
                        /// being mistaken for an active configuration head.
                        if (recovery_storage->hasCompleteDurableActivationMarker())
                        {
                            UDT::AtomicDatabaseUDTPersistedConfigurationV2 configured;
                            UDT::ResourceLimitLayer server_layer(UDT::ResourceLimitLayerKind::Server);
                            UDT::AuthorityVerificationSchedulerLimits global_scheduler_limits;
                            {
                                std::lock_guard authority_lock(udt_authority_mutex);
                                if (!udt_authority_configuration)
                                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT startup lost its resolved configuration");
                                configured = udt_authority_configuration->configured_persisted_configuration;
                                server_layer = udt_authority_configuration->server_resource_limit_layer;
                                global_scheduler_limits = udt_authority_configuration->global_verification_scheduler_limits;
                            }

                            const auto current_persisted = recovery_storage->readUDTConfigurationForActiveStartupV2();
                            auto selected = current_persisted;
                            if (configured.verification_scheduler_override)
                                selected.verification_scheduler_override = configured.verification_scheduler_override;
                            if (configured.resource_quota_override)
                                selected.resource_quota_override = configured.resource_quota_override;

                            const auto database_layer = selected.resource_quota_override
                                ? UDT::decodeDatabaseResourceQuotaOverrideV2(*selected.resource_quota_override, db_uuid)
                                : UDT::makeDatabaseDefaultResourceLimitLayer();
                            auto effective_database_limits = UDT::calculateEffectiveDatabaseResourceLimits(
                                server_layer, database_layer, UDT::atomicDatabaseAuthorityCapabilities().limits);
                            auto effective_scheduler_limits = selected.verification_scheduler_override
                                ? UDT::mergeAuthorityVerificationSchedulerLimits(
                                      global_scheduler_limits,
                                      UDT::decodeAuthorityVerificationSchedulerOverrideV2(
                                          *selected.verification_scheduler_override, db_uuid))
                                : UDT::AuthorityVerificationScheduler::validateEffectiveLimits(global_scheduler_limits);
                            if (configured.verification_scheduler_override
                                && selected.verification_scheduler_override != current_persisted.verification_scheduler_override)
                            {
                                const auto current_scheduler_limits = current_persisted.verification_scheduler_override
                                    ? UDT::mergeAuthorityVerificationSchedulerLimits(
                                          global_scheduler_limits,
                                          UDT::decodeAuthorityVerificationSchedulerOverrideV2(
                                              *current_persisted.verification_scheduler_override, db_uuid))
                                    : UDT::AuthorityVerificationScheduler::validateEffectiveLimits(global_scheduler_limits);
                                /// A persisted policy replacement is new
                                /// admission, not an existing-root escape.
                                if (current_scheduler_limits.policy != effective_scheduler_limits.policy
                                    || current_scheduler_limits.schedule != effective_scheduler_limits.schedule)
                                {
                                    static_cast<void>(
                                        applyEffectiveDatabaseVerificationLimits(effective_scheduler_limits, effective_database_limits));
                                }
                            }
                            auto persisted = recovery_storage->reconcileUDTConfigurationForActiveStartupV2(configured);
                            if (persisted != selected)
                            {
                                throw Exception(
                                    ErrorCodes::LOGICAL_ERROR,
                                    "Atomic UDT configuration reconciliation differs from its admitted replacement");
                            }
                            startup_limits.recovery.effective_database_limits = effective_database_limits;
                            {
                                std::lock_guard authority_lock(udt_authority_mutex);
                                if (!udt_authority_configuration)
                                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT startup lost its resolved configuration");
                                udt_authority_configuration->selected_persisted_configuration = std::move(persisted);
                                udt_authority_configuration->server_resource_limit_layer = std::move(server_layer);
                                udt_authority_configuration->effective_database_limits = std::move(effective_database_limits);
                                udt_authority_configuration->effective_verification_scheduler_limits
                                    = std::move(effective_scheduler_limits);
                                udt_lifecycle_adapter->configureEffectiveDatabaseResourceLimitsForStartup(
                                    udt_authority_configuration->effective_database_limits);
                            }
                        }
                        recovery = UDT::recoverAndActivateAtomicAuthorityAtStartup(*recovery_storage, startup_limits);
                        if (recovery.authority_root && recovery.degraded_status)
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR, "Atomic UDT recovery returned both an executable root and degraded status");
                        if (!recovery.authority_root && !recovery.degraded_status && recovery_storage->hasDurableAuthorityMarker())
                        {
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Atomic UDT recovery retained a durable activation marker without an executable or degraded result");
                        }
                    }
                    catch (const UDT::AtomicDatabaseSchemaMutationStorageError & error)
                    {
                        if (!UDT::isDegradableAtomicAuthorityStartupStorageError(error.code))
                            throw;
                        recovery = {};
                        recovery.degraded_status = UDT::makeGlobalIncompleteAtomicAuthorityStartupStatus(
                            db_uuid, "durable authority startup preflight cannot be read or reconciled safely");
                    }
                    recovered_dropped_tables = std::move(recovery.recovered_dropped_tables);
                    if (recovery.authority_root)
                    {
                        std::unique_ptr<UDT::AtomicTableStartupState> pending_state;
                        if (!recovery.pending_tables.empty())
                        {
                            std::vector<UDT::AtomicAuthorityStartupDependentObjectIdentity> pending_identities;
                            pending_identities.reserve(recovery.pending_tables.size());
                            for (const auto & pending : recovery.pending_tables)
                            {
                                pending_identities.push_back({
                                    .object_uuid = pending.expectation.object.object_uuid,
                                    .object_name = pending.object_name,
                                });
                            }
                            auto unavailable_root_status = UDT::AtomicAuthorityStartupStatusSnapshot::createForUnavailableRoot(
                                *recovery.authority_root, pending_identities, "mapped bind failed");
                            pending_state = std::make_unique<UDT::AtomicTableStartupState>(
                                db_uuid, std::move(recovery.pending_tables), std::move(unavailable_root_status));
                        }
                        std::lock_guard authority_lock(udt_authority_mutex);
                        if (udt_mutation_storage || udt_authority || udt_degraded_startup_status || udt_table_startup_state
                            || udt_authority_shutdown)
                        {
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Atomic user-defined type storage was initialized "
                                "twice or after shutdown");
                        }
                        udt_mutation_storage = std::move(recovery_storage);
                        udt_table_startup_state = std::move(pending_state);
                        try
                        {
                            initializeUDTAuthorityUnlocked(std::move(recovery.authority_root), !udt_table_startup_state);
                        }
                        catch (...)
                        {
                            active_udt_authority.store(nullptr, std::memory_order_release);
                            active_udt_verification_runtime.store(nullptr, std::memory_order_release);
                            if (udt_authority)
                                udt_authority->setPublicationObserver(nullptr);
                            udt_verification_runtime.reset();
                            udt_verification_scheduler.reset();
                            udt_last_exact_repair_provenance.reset();
                            udt_authority.reset();
                            udt_table_startup_state.reset();
                            udt_mutation_storage.reset();
                            throw;
                        }
                    }
                    else if (recovery.degraded_status)
                    {
                        if (!recovery.pending_tables.empty())
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR, "Degraded Atomic UDT recovery retained executable mapped-object startup state");
                        std::lock_guard authority_lock(udt_authority_mutex);
                        if (udt_mutation_storage || udt_authority || udt_degraded_startup_status || udt_table_startup_state
                            || udt_verification_runtime || udt_verification_scheduler || udt_authority_shutdown)
                        {
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Atomic user-defined type degraded startup state was installed twice or after authority initialization");
                        }
                        udt_mutation_storage = std::move(recovery_storage);
                        udt_degraded_startup_status = std::move(recovery.degraded_status);
                    }
                }
            }
        }

        if (!recovered_dropped_tables.empty())
        {
            /// The server-wide metadata_dropped scan precedes Atomic authority
            /// recovery. Check the exact terminal committed DROP: enqueue a
            /// tombstone the earlier scan missed, but do not duplicate one it
            /// already owns or recreate one consumed by completed cleanup.
            const auto already_marked = DatabaseCatalog::instance().getTablesMarkedDropped();
            for (const auto & recovered : recovered_dropped_tables)
            {
                const StorageID table_id{getDatabaseName(), recovered.table_name, recovered.table_uuid};
                const String dropped_metadata_path = DatabaseCatalog::instance().getPathForDroppedMetadata(table_id);
                const auto existing = std::find_if(
                    already_marked.begin(),
                    already_marked.end(),
                    [&](const auto & marked) { return marked.table_id.uuid == recovered.table_uuid; });
                if (existing != already_marked.end())
                {
                    if (existing->table_id != table_id || existing->metadata_path != dropped_metadata_path || existing->db_disk != db_disk)
                        throw Exception(ErrorCodes::ABORTED, "Recovered Atomic mapped DROP conflicts with queued dropped-table identity");
                    continue;
                }
                if (!db_disk->existsFile(dropped_metadata_path))
                {
                    if (recovered.tombstone_replayed)
                        throw Exception(ErrorCodes::ABORTED, "Recovered Atomic mapped DROP did not publish its durable tombstone");
                    /// A CompleteCommitted marker proves that this tombstone was
                    /// published durably. Its later absence means the ordinary
                    /// background cleanup already consumed it.
                    continue;
                }
                DatabaseCatalog::instance().enqueueDroppedTableCleanup(table_id, nullptr, db_disk, dropped_metadata_path, false);
            }
        }
    }

    if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
        return;

    if (!db_disk->isSymlinkSupported())
        return;

    // When `db_disk` is a `DiskLocal` object, `existsDirectory` will return false
    // if the input path is a symlink. So we use `existsFileOrDirectory` here to
    // check if the symlink exists.
    if (!db_disk->existsFileOrDirectory(path_to_table_symlinks))
        return;

    /// Recreate symlinks to table data dirs in case of force restore, because
    /// some of them may be broken
    for (const auto it = db_disk->iterateDirectory(path_to_table_symlinks); it->isValid(); it->next())
    {
        auto table_path = fs::path(it->path());
        if (table_path.filename().empty())
            table_path = table_path.parent_path();
        if (!db_disk->isSymlink(table_path))
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "'{}' is not a symlink. Atomic database should contains "
                "only symlinks.",
                std::string(table_path));
        }

        db_disk->removeFileIfExists(table_path);
    }
}

LoadTaskPtr DatabaseAtomic::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    auto base = DatabaseOrdinary::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Atomic database {}", getDatabaseName()),
        [this, mode, db_disk](AsyncLoader &, const LoadJobPtr &)
        {
            {
                std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
                activateUDTAuthorityAfterPendingTableStartup();
            }
            {
                std::lock_guard authority_lock(udt_authority_mutex);
                udt_database_startup_complete.store(true, std::memory_order_release);
                if (!udt_authority_shutdown && udt_verification_scheduler
                    && active_udt_authority.load(std::memory_order_acquire) == udt_authority.get()
                    && active_udt_verification_runtime.load(std::memory_order_acquire) == udt_verification_runtime.get())
                    udt_verification_scheduler->activateAfterDatabaseStartup();
            }
            if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
                return;
            NameToPathMap table_names;
            {
                std::lock_guard lock{mutex};
                table_names = table_name_to_path;
            }
            if (db_disk->isSymlinkSupported())
                db_disk->createDirectories(path_to_table_symlinks);
            for (const auto & table : table_names)
            {
                /// All tables in database should be loaded at this point
                StoragePtr table_ptr = tryGetTable(table.first, getContext());
                if (table_ptr)
                {
                    if (table_ptr->storesDataOnDisk())
                        tryCreateSymlink(table_ptr, true);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not loaded before database startup", table.first);
            }
        });
    std::scoped_lock lock(mutex);
    return startup_atomic_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseAtomic::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_atomic_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task, false);
}

void DatabaseAtomic::stopLoading()
{
    LoadTaskPtr stop_atomic_database;
    {
        std::scoped_lock lock(mutex);
        stop_atomic_database.swap(startup_atomic_database_task);
    }
    stop_atomic_database.reset();
    DatabaseOrdinary::stopLoading();
}

void DatabaseAtomic::tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    if (table->getDataPaths().empty())
        return;

    const auto table_data_path = fs::path(table->getDataPaths().front()).lexically_normal();

    try
    {
        String table_name = table->getStorageID().getTableName();

        if (!table->storesDataOnDisk())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} doesn't have data path to create symlink", table_name);

        String link = path_to_table_symlinks / escapeForFileName(table_name);

        LOG_DEBUG(
            log,
            "Trying to create a symlink for table {}, data_path {}, link {}",
            table->getStorageID().getNameForLogs(),
            table_data_path,
            link);

        /// If it already points where needed.
        if (db_disk->equivalentNoThrow(table_data_path, link))
            return;

        if (if_data_path_exist && !db_disk->existsFileOrDirectory(data_path))
            return;

        db_disk->createDirectorySymlink(table_data_path, link);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryRemoveSymlink(const String & table_name)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    try
    {
        String path = path_to_table_symlinks / escapeForFileName(table_name);
        db_disk->removeFileIfExists(path);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    auto db_disk = getDisk();
    if (!db_disk->isSymlinkSupported())
        return;

    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by
    /// ClickHouse, it's needed only for convenient introspection.
    chassert(path_to_metadata_symlink != metadata_path);
    if (db_disk->existsFileOrDirectory(path_to_metadata_symlink))
    {
        if (!db_disk->isSymlink(path_to_metadata_symlink))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} already exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
            /// fs::exists could return false for broken symlink
            if (db_disk->isSymlinkNoThrow(path_to_metadata_symlink))
                db_disk->removeFileIfExists(path_to_metadata_symlink);

            LOG_DEBUG(
                log,
                "Creating directory symlink, path_to_metadata_symlink: {}, "
                "metadata_path: {}",
                path_to_metadata_symlink,
                metadata_path);

            db_disk->createDirectorySymlink(metadata_path, path_to_metadata_symlink);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void DatabaseAtomic::renameDatabase(ContextPtr query_context, const String & new_name)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::renameDatabase");
    waitDatabaseStarted();
    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    std::optional<UDT::AtomicAuthority::RootSnapshot> udt_snapshot;
    UDT::AtomicDatabaseSchemaMutationStorage * udt_storage = nullptr;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority)
            udt_snapshot.emplace(udt_authority->acquireCurrentRoot());
        udt_storage = udt_mutation_storage.get();
    }
    /// The durable storage embeds the current database name in mapped-table
    /// installation and dropped-metadata paths. Until a rename transaction can
    /// rebuild those paths atomically, database rename must fail closed even
    /// for an empty authority with definition-only or dependent-object capabilities.
    const bool has_udt_authority = (udt_snapshot && *udt_snapshot) || udt_storage;
    if (has_udt_authority)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "RENAME DATABASE is not supported while Atomic database {} "
            "contains durable user-defined types or pending recovery",
            getDatabaseName());
    }

    /// CREATE, ATTACH, DROP, DETACH and RENAME DATABASE must hold DDLGuard
    createDirectories();
    std::lock_guard lock(mutex);

    /// A longer database name leaves less room for the table name in the
    /// dropped-metadata file name metadata_dropped/{db}.{table}.{uuid}.sql, so a
    /// rename can leave a table that cannot be dropped. Detached tables are
    /// checked too, because ATTACH does not re-check the length.
    for (const auto & table : tables)
        checkTableNameLengthUnlocked(new_name, table.first, getContext());
    for (const auto & detached_table : snapshot_detached_tables)
        checkTableNameLengthUnlocked(new_name, detached_table.first, getContext());

    bool check_ref_deps = query_context->getSettingsRef()[Setting::check_referential_table_dependencies];
    bool check_loading_deps = !check_ref_deps && query_context->getSettingsRef()[Setting::check_table_dependencies];
    if (check_ref_deps || check_loading_deps)
    {
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first}, check_ref_deps, check_loading_deps);
    }

    try
    {
        auto db_disk = getDisk();
        if (db_disk->isSymlinkSupported())
            db_disk->removeFileIfExists(path_to_metadata_symlink);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }

    auto old_metadata_file_path = DatabaseCatalog::getMetadataFilePath(database_name);
    auto new_metadata_file_path = DatabaseCatalog::getMetadataFilePath(new_name);
    auto default_db_disk = getContext()->getDatabaseDisk();
    default_db_disk->moveFile(old_metadata_file_path, new_metadata_file_path);

    String old_path_to_table_symlinks;

    {
        {
            Strings table_names;
            table_names.reserve(tables.size());
            for (auto & table : tables)
                table_names.push_back(table.first);
            DatabaseCatalog::instance().updateDatabaseName(database_name, new_name, table_names);
        }
        database_name = new_name;

        for (auto & table : tables)
        {
            auto table_id = table.second->getStorageID();
            table_id.database_name = database_name;
            table.second->renameInMemory(table_id);
        }

        for (auto & [detached_table_name, snapshot] : snapshot_detached_tables)
        {
            snapshot.database = database_name;
        }

        path_to_metadata_symlink = DatabaseCatalog::getMetadataDirPath(new_name);
        old_path_to_table_symlinks = path_to_table_symlinks;
        path_to_table_symlinks = DatabaseCatalog::getDataDirPath(new_name) / "";
    }

    auto db_disk = getDisk();
    if (db_disk->isSymlinkSupported())
    {
        db_disk->moveDirectory(old_path_to_table_symlinks, path_to_table_symlinks);
        tryCreateMetadataSymlink();
    }
}

void DatabaseAtomic::waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled)
{
    /// Table is in use while its shared_ptr counter is greater than 1.
    /// We cannot trigger condvar on shared_ptr destruction, so it's busy wait.
    LOG_DEBUG(log, "Waiting for detached table {} to be no longer in use", toString(uuid));

    unsigned iterations = 0;
    while (!DatabaseCatalog::instance().isShuttingDown())
    {
        bool found = true;
        int64_t use_count = 0;
        bool log_slow_wait = false;
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            if (!detached_tables.contains(uuid))
            {
                found = false;
            }
            else if (iterations > 0 && iterations % 100 == 0)
            {
                auto it = detached_tables.find(uuid);
                if (it != detached_tables.end() && it->second)
                    use_count = it->second.use_count();
                log_slow_wait = true;
            }
        }
        /// not_in_use destroyed here (after lock released) — StoragePtrs freed without holding mutex

        if (!found)
        {
            LOG_DEBUG(log, "Detached table {} is no longer in use", toString(uuid));
            return;
        }

        /// Check cancellation after verifying the table is still tracked.
        /// This ordering avoids throwing a cancellation exception when
        /// the wait has already completed.
        if (throw_if_cancelled)
            throw_if_cancelled();

        if (log_slow_wait)
            LOG_INFO(log, "Still waiting for detached table {} to be no longer in use (use_count={}, elapsed ~{}s)",
                toString(uuid), use_count, iterations / 10);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ++iterations;
    }

    /// Server is shutting down. Do one final cleanup pass — the table may have
    /// become free just before or during shutdown.
    bool still_tracked = false;
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            still_tracked = detached_tables.contains(uuid);
        }
    }

    if (!still_tracked)
    {
        LOG_DEBUG(log, "Detached table {} is no longer in use (resolved during shutdown)", toString(uuid));
        return;
    }

    throw Exception(ErrorCodes::UNFINISHED,
        "Did not finish waiting for detached table {} to be no longer in use "
        "because the server is shutting down", uuid);
}

void DatabaseAtomic::checkDetachedTableNotInUse(const UUID & uuid)
{
    DetachedTables not_in_use;
    std::lock_guard lock{mutex};
    not_in_use = cleanupDetachedTables();
    assertDetachedTableNotInUse(uuid);
}

void registerDatabaseAtomic(DatabaseFactory & factory);

void registerDatabaseAtomic(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (args.database_name.ends_with(DatabaseReplicated::BROKEN_REPLICATED_TABLES_SUFFIX))
            args.context->addOrUpdateWarningMessage(
                Context::WarningType::MAYBE_BROKEN_TABLES,
                PreformattedMessage::create(
                    "The database {} is probably created during recovering a lost "
                    "replica. If it has no tables, it can be deleted. If it "
                    "has tables, it worth to check why they were considered broken.",
                    backQuoteIfNeed(args.database_name)));

        DatabaseMetadataDiskSettings database_metadata_disk_settings;
        auto * engine_define = args.create_query.storage;
        chassert(engine_define);
        database_metadata_disk_settings.loadFromQuery(*engine_define, args.context, isLoadingFromExistingMetadata(args.mode));

        return make_shared<DatabaseAtomic>(
            args.database_name, args.metadata_path, args.uuid, args.context, database_metadata_disk_settings);
    };
    factory.registerDatabase(
        "Atomic",
        create_fn,
        /*features=*/{.supports_settings = true},
        Documentation{
            .description = R"DOCS_MD(
The `Atomic` engine supports non-blocking [`DROP TABLE`](#drop-detach-table) and [`RENAME TABLE`](#rename-table) queries, and atomic [`EXCHANGE TABLES`](#exchange-tables) queries. The `Atomic` database engine is used by default in open-source ClickHouse.

:::note
On ClickHouse Cloud, the [`Shared` database engine](/products/cloud/features/infrastructure/shared-catalog#shared-database-engine) is used by default and also supports
the above mentioned operations.
:::

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

## Specifics and recommendations {#specifics-and-recommendations}

### Table UUID {#table-uuid}

Each table in the `Atomic` database has a persistent [UUID](/reference/data-types/uuid) and stores its data in the following directory:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is the UUID of the table.

By default, the UUID is generated automatically. However, users can explicitly specify the UUID when creating a table, though this is not recommended.

For example:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
You can use the [show_table_uuid_in_table_create_query_if_not_nil](/reference/settings/session-settings/show#show_table_uuid_in_table_create_query_if_not_nil) setting to display the UUID with the `SHOW CREATE` query.
:::

### RENAME TABLE {#rename-table}

[`RENAME`](/reference/statements/rename) queries do not modify the UUID or move table data. These queries execute immediately and do not wait for other queries that are using the table to complete.

### DROP/DETACH TABLE {#drop-detach-table}

When using `DROP TABLE`, no data is removed. The `Atomic` engine just marks the table as dropped by moving it's metadata to `/clickhouse_path/metadata_dropped/` and notifies the background thread. The delay before the final table data deletion is specified by the [`database_atomic_delay_before_drop_table_sec`](/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec) setting.
You can specify synchronous mode using `SYNC` modifier. Use the [`database_atomic_wait_for_drop_and_detach_synchronously`](/reference/settings/session-settings/database#database_atomic_wait_for_drop_and_detach_synchronously) setting to do this. In this case `DROP` waits for running `SELECT`, `INSERT` and other queries which are using the table to finish. The table will be removed when it's not in use.

### EXCHANGE TABLES/DICTIONARIES {#exchange-tables}

The [`EXCHANGE`](/reference/statements/exchange) query swaps tables or dictionaries atomically. For instance, instead of this non-atomic operation:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```
you can use an atomic one:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

### ReplicatedMergeTree in atomic database {#replicatedmergetree-in-atomic-database}

For [`ReplicatedMergeTree`](/reference/engines/table-engines/mergetree-family/replication) tables, it is recommended not to specify the engine parameters for the path in ZooKeeper and the replica name. In this case, the configuration parameters [`default_replica_path`](/reference/settings/server-settings/settings/default-replica#default_replica_path) and [`default_replica_name`](/reference/settings/server-settings/settings/default-replica#default_replica_name) will be used. If you want to specify engine parameters explicitly, it is recommended to use the `{uuid}` macros. This ensures that unique paths are automatically generated for each table in ZooKeeper.

### Metadata disk {#metadata-disk}
When `disk` is specified in `SETTINGS`, the disk is used to store table metadata files.
For example:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```
If unspecified, the disk defined in `database_disk.disk` is used by default.

## See also {#see-also}

- [system.databases](/reference/system-tables/databases) system table
)DOCS_MD",
            .syntax = "ENGINE = Atomic",
            .related = {"Replicated", "Ordinary"}});
}

} // namespace DB
