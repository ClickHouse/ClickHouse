#include <exception>
#include <filesystem>
#include <thread>
#include <Access/UDTUsageAccess.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/TableColumnTypeAlterBindings.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
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
#include <Databases/UDT/AtomicCrossDatabaseGuard.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AtomicLifecycleAdapter.h>
#include <Databases/UDT/AtomicStoredObjectUDTMetadataValidator.h>
#include <Databases/UDT/AtomicTableMetadataValidator.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>
#include <Databases/UDT/DatabaseResourceQuotaSettings.h>
#include <Databases/UDT/DependentObjectActivationPlanner.h>
#include <Databases/UDT/DependentObjectAdmissionCoordinator.h>
#include <Databases/UDT/DependentObjectAdmissionPlanner.h>
#include <Databases/UDT/DependentObjectMutationCoordinator.h>
#include <Databases/UDT/DependentObjectMutationPlanner.h>
#include <Databases/UDT/ResourceLimitAdapters.h>
#include <Databases/UDT/StoredObjectUDTPublicationCoordinator.h>
#include <Disks/IStoragePolicy.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageInMemoryMetadata.h>
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
{

struct AtomicTableStartupState
{
    struct Entry
    {
        AtomicAuthorityPendingTable image;
        StoragePtr attached_table;
        StorageMetadataPtr bound_metadata;
        bool attached = false;
        bool started = false;
    };

    AtomicTableStartupState(
        UUID database_uuid,
        std::vector<AtomicAuthorityPendingTable> pending_tables,
        AtomicAuthorityStartupStatusSnapshot::Ptr unavailable_root_status_)
        : unavailable_root_status(std::move(unavailable_root_status_))
    {
        if (pending_tables.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic pending Table startup state is empty");
        if (!unavailable_root_status || unavailable_root_status->getDatabaseUUID() != database_uuid
            || unavailable_root_status->hasUnknownDependentObjectScope())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic pending Table startup state has no exact fail-closed fallback image");
        }

        entries.reserve(pending_tables.size());
        for (auto & pending : pending_tables)
        {
            const auto & object = pending.expectation.object;
            if (!object.isValid()
                || (object.kind != SchemaObjectKind::Table && object.kind != SchemaObjectKind::View
                    && object.kind != SchemaObjectKind::Dictionary)
                || object.database_uuid != database_uuid || pending.object_name.empty() || !pending.expectation.installation_record_hash
                || pending.canonical_metadata_bytes.empty() || pending.canonical_sidecar_bytes.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic pending Table startup image is invalid");
            }
            const auto * fallback_identity = unavailable_root_status->findExpectedDependentObject(object.object_uuid);
            if (!fallback_identity || fallback_identity->object_name != pending.object_name)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Atomic pending Table startup fallback differs from a durable dependent-object identity");
            }

            const size_t index = entries.size();
            if (!by_uuid.emplace(object.object_uuid, index).second || !by_name.emplace(pending.object_name, index).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic pending Table startup identities are not unique");
            entries.push_back({
                .image = std::move(pending),
                .attached_table = {},
                .bound_metadata = {},
                .attached = false,
                .started = false,
            });
        }
        remaining = entries.size();
    }

    std::optional<size_t> findExact(UUID table_uuid, std::string_view table_name) const
    {
        const auto uuid_it = by_uuid.find(table_uuid);
        const auto name_it = by_name.find(String(table_name));
        if (uuid_it == by_uuid.end() && name_it == by_name.end())
            return std::nullopt;
        if (uuid_it == by_uuid.end() || name_it == by_name.end() || uuid_it->second != name_it->second)
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic mapped table startup identity differs from its "
                "durable installation mapping");
        return uuid_it->second;
    }

    const Entry * findExactEntry(UUID table_uuid, std::string_view table_name) const
    {
        const auto index = findExact(table_uuid, table_name);
        return index ? &entries[*index] : nullptr;
    }

    Entry * findExactEntry(UUID table_uuid, std::string_view table_name)
    {
        const auto index = findExact(table_uuid, table_name);
        return index ? &entries[*index] : nullptr;
    }

    StorageMetadataPtr validateBoundTable(
        const Entry & entry, const StoragePtr & table, std::string_view database_name, bool require_same_metadata_snapshot) const
    {
        if (!table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup lost its attached storage");
        const auto storage_id = table->getStorageID();
        if (storage_id.database_name != database_name || storage_id.table_name != entry.image.object_name
            || storage_id.uuid != entry.image.expectation.object.object_uuid)
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic mapped table startup storage identity changed "
                "before authority publication");
        }

        /// Keep the exact published version here. MergeTree returns defensive
        /// metadata copies through the virtual getter in debug builds, which
        /// cannot be used for the identity check performed at publication.
        auto metadata_handle = table->IStorage::getInMemoryMetadataPtr(nullptr, true);
        if (!metadata_handle)
            throw Exception(ErrorCodes::ABORTED, "Atomic mapped table startup storage has no metadata snapshot");
        StorageMetadataPtr metadata = metadata_handle;
        if (require_same_metadata_snapshot && metadata != entry.bound_metadata)
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic mapped table startup metadata changed after "
                "successful startup binding");

        metadata->validateBoundUDTReferences();
        const auto & bound = metadata->getBoundUDTReferences();
        const auto & expectation = metadata->getBoundUDTExpectation();
        if (!bound || !expectation || *expectation != entry.image.expectation || bound->getObject() != entry.image.expectation.object
            || bound->getObjectSchemaRevision() != entry.image.expectation.object_schema_revision
            || bound->getSidecarHash() != entry.image.expectation.sidecar_hash
            || bound->getPhysicalSchemaFingerprint() != entry.image.expectation.physical_schema_fingerprint)
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic mapped table startup binding changed before "
                "authority publication");
        }
        return metadata;
    }

    void markAttached(Entry & entry, const StoragePtr & table, std::string_view database_name)
    {
        if (entry.attached || !remaining)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup entry was attached twice");
        entry.bound_metadata = validateBoundTable(entry, table, database_name, false);
        entry.attached_table = table;
        entry.attached = true;
        --remaining;
    }

    void markStarted(Entry & entry, const StoragePtr & table, std::string_view database_name)
    {
        if (!entry.attached || entry.started || entry.attached_table != table)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Atomic mapped table startup completion differs from its "
                "attached storage");
        entry.bound_metadata = validateBoundTable(entry, table, database_name, false);
        entry.started = true;
    }

    std::vector<Entry> entries;
    std::map<UUID, size_t> by_uuid;
    std::map<String, size_t, std::less<>> by_name;
    AtomicAuthorityStartupStatusSnapshot::Ptr unavailable_root_status;
    std::exception_ptr retryable_failure;
    size_t remaining = 0;
};

} // namespace UDT

namespace
{

constexpr UInt64 maximum_concurrent_udt_restore_publication_leases = 65'536;

ASTPtr
parseMappedTableStartupMetadata(std::string_view canonical_metadata_bytes, std::string_view database_name, std::string_view table_name)
{
    ParserCreateQuery parser;
    auto ast = parseQuery(
        parser,
        canonical_metadata_bytes.data(),
        canonical_metadata_bytes.data() + canonical_metadata_bytes.size(),
        "Atomic mapped table startup metadata",
        16ULL << 20,
        256,
        100'000);
    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw Exception(ErrorCodes::ABORTED, "Atomic mapped table startup metadata is not a CREATE query");
    create->setDatabase(String(database_name));
    create->setTable(String(table_name));
    return ast;
}

UDT::SchemaObjectKind mappedSchemaObjectKindForStorage(const IStorage & storage) noexcept
{
    if (storage.isDictionary())
        return UDT::SchemaObjectKind::Dictionary;
    if (storage.isView())
        return UDT::SchemaObjectKind::View;
    return UDT::SchemaObjectKind::Table;
}

bool isAtomicMaterializedViewInnerTableName(std::string_view table_name) noexcept
{
    return table_name.starts_with(".inner_id.") || table_name.starts_with(".tmp.inner_id.");
}

std::optional<UUID> tryGetAtomicMaterializedViewOwnerUUID(std::string_view table_name)
{
    constexpr std::string_view inner_prefix = ".inner_id.";
    constexpr std::string_view temporary_inner_prefix = ".tmp.inner_id.";
    std::string_view uuid_text;
    if (table_name.starts_with(inner_prefix))
        uuid_text = table_name.substr(inner_prefix.size());
    else if (table_name.starts_with(temporary_inner_prefix))
        uuid_text = table_name.substr(temporary_inner_prefix.size());
    else
        return std::nullopt;

    /// Generated Atomic inner names use the canonical 36-byte UUID spelling.
    /// A user-created lookalike with another suffix is not an ownership key.
    if (uuid_text.size() != 36)
        return std::nullopt;
    try
    {
        return parse<UUID>(uuid_text);
    }
    catch (const Exception &)
    {
        return std::nullopt;
    }
}

std::vector<UDT::SchemaObjectID> collectMappedObjectDependencies(
    const DatabaseAtomic & database,
    const UDT::AuthorityRoot & root,
    const ASTPtr & exact_create_query,
    const QualifiedTableName & dependent_name,
    const UDT::SchemaObjectID & dependent_object,
    ContextPtr query_context)
{
    if (!query_context || !exact_create_query || !dependent_object.isValid() || dependent_object.database_uuid != root.getDatabaseUUID()
        || dependent_name.database != database.getDatabaseName())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot resolve mapped object dependencies from an invalid CREATE boundary");
    }

    const auto named_dependencies = getDependenciesFromCreateQuery(
        query_context->getGlobalContext(), dependent_name, exact_create_query, query_context->getCurrentDatabase(), true);
    std::vector<UDT::SchemaObjectID> result;
    result.reserve(named_dependencies.dependencies.size());
    for (const auto & dependency_name : named_dependencies.dependencies)
    {
        const String dependency_database = dependency_name.database.empty() ? database.getDatabaseName() : dependency_name.database;
        if (dependency_database != database.getDatabaseName())
            continue;

        auto dependency_storage = database.tryGetTable(dependency_name.table, query_context);
        if (!dependency_storage)
            continue;
        const auto storage_id = dependency_storage->getStorageID();
        if (storage_id.database_name != database.getDatabaseName() || storage_id.table_name != dependency_name.table
            || storage_id.uuid == UUIDHelpers::Nil)
        {
            throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency changed identity during mapped-object admission");
        }

        const UDT::SchemaObjectID dependency_object{
            .kind = mappedSchemaObjectKindForStorage(*dependency_storage),
            .database_uuid = root.getDatabaseUUID(),
            .object_uuid = storage_id.uuid,
        };
        const UDT::SidecarExpectationRecord * rooted_expectation = nullptr;
        for (const auto candidate_kind : {UDT::SchemaObjectKind::Table, UDT::SchemaObjectKind::View, UDT::SchemaObjectKind::Dictionary})
        {
            const auto * candidate = root.findExpectationRecord({
                .kind = candidate_kind,
                .database_uuid = root.getDatabaseUUID(),
                .object_uuid = storage_id.uuid,
            });
            if (!candidate)
                continue;
            if (rooted_expectation)
                throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency has ambiguous mapped identity");
            rooted_expectation = candidate;
        }
        const auto metadata = dependency_storage->getInMemoryMetadataPtr(query_context, false);
        if (!metadata)
            throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency has no metadata snapshot");
        metadata->validateBoundUDTReferences();
        if (metadata->getPendingUDTColumnAlter())
            throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency has an unpublished logical ALTER");
        if (!rooted_expectation)
        {
            if (metadata->getBoundUDTReferences() || metadata->getBoundUDTExpectation())
                throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency has unrooted logical provenance");
            continue;
        }
        if (rooted_expectation->object != dependency_object || !root.getSchemaObjectDependencyGraph().containsNode(dependency_object)
            || dependency_object == dependent_object)
        {
            throw Exception(ErrorCodes::ABORTED, "A stored CREATE dependency differs from the pinned mapped authority root");
        }

        const auto & bound = metadata->getBoundUDTReferences();
        const auto & expectation = metadata->getBoundUDTExpectation();
        if (!bound || !expectation || *expectation != *rooted_expectation || bound->getObject() != dependency_object
            || bound->getObjectSchemaRevision() != rooted_expectation->object_schema_revision
            || bound->getSidecarHash() != rooted_expectation->sidecar_hash
            || bound->getPhysicalSchemaFingerprint() != rooted_expectation->physical_schema_fingerprint)
        {
            throw Exception(ErrorCodes::ABORTED, "A mapped stored CREATE dependency has stale logical provenance");
        }
        result.push_back(dependency_object);
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

bool hasExactDegradedUDTObjectArtifacts(
    const DiskPtr & disk, const UDT::AtomicDatabaseSchemaMutationPaths & paths, UUID database_uuid, UUID object_uuid)
{
    if (!disk || database_uuid == UUIDHelpers::Nil || object_uuid == UUIDHelpers::Nil)
        return false;

    const UDT::AuthorityInventoryKey expectation_key{
        .format_version = UDT::authority_inventory_format_version,
        .record_kind = UDT::AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object_uuid,
    };
    if (disk->existsFileOrDirectory(paths.authorityRecordPath(expectation_key)))
        return true;
    for (const auto kind : {UDT::SchemaObjectKind::Table, UDT::SchemaObjectKind::View, UDT::SchemaObjectKind::Dictionary})
    {
        const UDT::SchemaObjectID object{
            .kind = kind,
            .database_uuid = database_uuid,
            .object_uuid = object_uuid,
        };
        if (disk->existsFileOrDirectory(paths.tableReferencesPath(object))
            || disk->existsFileOrDirectory(paths.metadataInstallationRecordPath(object)))
            return true;
    }
    return false;
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

UDT::AtomicDatabaseSchemaMutationDependentObjectImage
findExactDependentObjectImage(UDT::AtomicDatabaseSchemaMutationReconciliation reconciliation, const UDT::SchemaObjectID & object)
{
    auto it = std::find_if(
        reconciliation.dependent_objects.begin(),
        reconciliation.dependent_objects.end(),
        [&](const auto & image) { return image.expectation.object == object; });
    if (it == reconciliation.dependent_objects.end())
        throw Exception(ErrorCodes::ABORTED, "Mapped table durable image disappeared before schema mutation");
    return std::move(*it);
}

UDT::PublishedDependentObjectMutation commitMappedTableMutationWithRecovery(
    UDT::AtomicAuthority & authority,
    UDT::AtomicDatabaseSchemaMutationStorage & storage,
    UDT::DatabaseSchemaMutationGuard & mutation_guard,
    UDT::PreparedDependentObjectMutationCommit & prepared)
{
    using UDT::DatabaseSchemaMutationIndeterminateDurabilityError;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::DependentObjectMutationCoordinator;
    using UDT::discardUnpreparedDatabaseSchemaMutationStaging;
    using UDT::DurablyCommittedDependentObjectMutation;

    std::optional<DurablyCommittedDependentObjectMutation> durable;
    try
    {
        durable.emplace(DependentObjectMutationCoordinator::commitDurably(storage, mutation_guard, std::move(prepared)));
    }
    catch (const DatabaseSchemaMutationIndeterminateDurabilityError & error)
    {
        const auto original = std::current_exception();
        bool rolled_back = false;
        try
        {
            if (error.transaction_id != prepared.getTransactionID() || storage.getRecoveryRequiredTransactionID() != error.transaction_id)
                throw DatabaseSchemaMutationReplayConflictError("mapped-table mutation recovery latch changed identity");

            const auto transaction_ids = storage.listDurableTransactionIDs();
            if (!std::binary_search(transaction_ids.begin(), transaction_ids.end(), error.transaction_id))
            {
                discardUnpreparedDatabaseSchemaMutationStaging(storage, mutation_guard, error.transaction_id);
                rolled_back = true;
            }
            else
            {
                auto image = storage.loadTransactionForRecovery(error.transaction_id);
                const auto & transition = prepared.getRecoveryTransition();
                const auto expected_bytes = transition.getStagedArtifactBytes();
                if (image.prepare != transition.getPrepare() || image.staged_artifact_bytes.size() != expected_bytes.size()
                    || !std::equal(
                        image.staged_artifact_bytes.begin(),
                        image.staged_artifact_bytes.end(),
                        expected_bytes.begin(),
                        expected_bytes.end()))
                    throw DatabaseSchemaMutationReplayConflictError(
                        "mapped-table mutation recovery image differs from the retained transition");
                auto recovered
                    = DependentObjectMutationCoordinator::recoverDurably(storage, mutation_guard, std::move(prepared), image.commit);
                rolled_back = !recovered;
                if (recovered)
                    durable.emplace(std::move(*recovered));
            }
        }
        catch (...)
        {
            std::terminate();
        }
        if (rolled_back)
            std::rethrow_exception(original);
    }

    if (!durable)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table mutation produced no durable publication");
    return DependentObjectMutationCoordinator::publish(authority, std::move(*durable));
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

void checkUsageAccessForFinalPersistedUDTDescriptors(const ContextPtr & query_context, const UDT::PersistedTypeReferences & references)
{
    if (!query_context || !references.object.isValid() || references.descriptors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT access check received an incomplete persisted binding");

    std::vector<UDT::AccessTarget> targets;
    targets.reserve(references.descriptors.size());
    for (const auto & descriptor : references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        if (identity.database_uuid != references.object.database_uuid || identity.type_uuid == UUIDHelpers::Nil || !identity.revision)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT access check received a foreign or invalid descriptor identity");
        targets.push_back({
            .database_uuid = identity.database_uuid,
            .type_uuid = identity.type_uuid,
        });
    }

    /// The access boundary canonicalizes and deduplicates stable identities,
    /// so multiple specializations/revisions of one type require one USAGE_TYPE
    /// decision without weakening the final persisted descriptor set.
    UDT::checkUsageAccess(query_context, targets);
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

void DatabaseAtomic::activateUDTAuthorityAfterPendingTableStartup()
{
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::dependent_object_authority_capability_mask;

    /// Startup publication validates the live catalog and the private
    /// authority as one image. Keep the catalog -> authority lock order used
    /// by ordinary Atomic DDL.
    std::lock_guard tables_lock(mutex);
    std::lock_guard authority_lock(udt_authority_mutex);
    if (!udt_table_startup_state)
        return;
    if (udt_table_startup_state->retryable_failure)
        std::rethrow_exception(udt_table_startup_state->retryable_failure);
    if (udt_table_startup_state->remaining)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Cannot publish Atomic user-defined type authority while "
            "{} mapped table startup entries remain unattached",
            udt_table_startup_state->remaining);
    }
    if (udt_authority_shutdown || !udt_authority || !udt_mutation_storage || !udt_verification_runtime || !udt_verification_scheduler
        || active_udt_authority.load(std::memory_order_acquire))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup authority components are inconsistent");
    }

    const auto snapshot = udt_authority->acquireCurrentRoot();
    if (!snapshot || snapshot.get().getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup has no private dependent-object-capable authority root");
    const auto durable_state = udt_mutation_storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != snapshot.get().getAuthorityState() || udt_mutation_storage->getRecoveryRequiredTransactionID())
    {
        throw DatabaseSchemaMutationReplayConflictError(
            "Atomic mapped table startup authority differs from its durable "
            "recovery head");
    }

    for (const auto & entry : udt_table_startup_state->entries)
    {
        if (!entry.attached || !entry.started || !entry.attached_table || !entry.bound_metadata)
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic mapped table did not complete binding and "
                "startup before authority publication");
        const auto table_it = tables.find(entry.image.object_name);
        if (table_it == tables.end() || table_it->second != entry.attached_table)
            throw Exception(ErrorCodes::ABORTED, "Atomic mapped table is no longer the exact live startup storage");
        static_cast<void>(udt_table_startup_state->validateBoundTable(entry, table_it->second, database_name, true));
    }
    active_udt_verification_runtime.store(udt_verification_runtime.get(), std::memory_order_release);
    active_udt_authority.store(udt_authority.get(), std::memory_order_release);
    udt_table_startup_state.reset();
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

std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence> DatabaseAtomic::acquireUDTStorageReadContinuationEvidenceImpl(
    const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const
{
    using UDT::AuthorityObjectImageIdentity;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::AuthorityQuarantineOperationTiming;
    using UDT::AuthorityStorageReadContinuationEvidence;
    using UDT::AuthorityVerificationStamp;
    using UDT::AuthorityVerificationStampError;
    using UDT::SchemaObjectID;
    using UDT::validateAndRebaseAuthorityVerificationStamp;

    if (!metadata)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT storage operation has no immutable metadata image");
    metadata->validateBoundUDTReferences();
    const auto & bound = metadata->getBoundUDTReferences();
    if (!bound)
        return {};
    const auto & expectation = metadata->getBoundUDTExpectation();
    const auto & stamp = metadata->getBoundUDTVerificationStamp();
    if (!expectation || !stamp)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic UDT storage has no exact verification stamp");

    if (kind < AuthorityQuarantineOperationKind::Read || kind > AuthorityQuarantineOperationKind::Attach)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT storage operation kind is invalid");

    auto * authority = active_udt_authority.load(std::memory_order_acquire);
    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!authority || !runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT storage operation has no active authority runtime");
    auto root = authority->acquireCurrentRoot();
    if (!root || active_udt_authority.load(std::memory_order_acquire) != authority
        || active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT authority changed while admitting a storage operation");
    const auto & exact_root = root.get();
    const auto * rooted_expectation = exact_root.findExpectationRecord(bound->getObject());
    if (!rooted_expectation || *rooted_expectation != *expectation)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic UDT storage differs from its current authority expectation");
    const AuthorityObjectImageIdentity object_image{
        .object = bound->getObject(),
        .object_schema_revision = bound->getObjectSchemaRevision(),
        .sidecar_hash = bound->getSidecarHash(),
        .physical_schema_fingerprint = bound->getPhysicalSchemaFingerprint(),
    };
    const auto pinned_root = authorityRootGraphIdentity(exact_root);
    if (stamp->getVerifiedObject() != object_image)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic UDT storage verification stamp belongs to another object image");
    AuthorityVerificationStamp::Ptr operation_stamp = stamp;
    if (stamp->getVerifiedRoot() != pinned_root.authority_root)
    {
        try
        {
            operation_stamp = validateAndRebaseAuthorityVerificationStamp(exact_root, *expectation, *bound, *stamp);
        }
        catch (const AuthorityVerificationStampError &)
        {
            throw Exception(
                ErrorCodes::ABORTED, "Mapped Atomic UDT storage verification stamp cannot be reanchored to the current authority root");
        }
    }

    const std::array<SchemaObjectID, 1> touched{object_image.object};
    const auto decision = runtime->decideOperation({
        .kind = kind,
        .timing = AuthorityQuarantineOperationTiming::New,
        .pinned_root = pinned_root,
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = touched,
        .continuation_proof_set_is_complete = false,
        .sorted_unique_continuation_proofs = {},
    });
    if (!decision.isAllowed())
    {
        ProfileEvents::increment(ProfileEvents::UDTAuthorityMappedOperationRejections);
        throw Exception(
            ErrorCodes::ABORTED,
            "Atomic UDT quarantine rejected a new storage operation (status {})",
            static_cast<unsigned>(decision.status));
    }
    ProfileEvents::increment(ProfileEvents::UDTAuthorityMappedOperationAdmissions);
    return AuthorityStorageReadContinuationEvidence::Ptr(
        new AuthorityStorageReadContinuationEvidence(pinned_root, object_image, std::move(operation_stamp)));
}

std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence>
DatabaseAtomic::acquireUDTStorageReadContinuationEvidence(const StorageMetadataPtr & metadata) const
{
    return acquireUDTStorageReadContinuationEvidenceImpl(metadata, UDT::AuthorityQuarantineOperationKind::Read);
}

void DatabaseAtomic::assertUDTNewStorageOperationAllowed(
    const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const
{
    static_cast<void>(acquireUDTStorageReadContinuationEvidenceImpl(metadata, kind));
}

UDT::AuthorityStorageNewOperationCommitGuard DatabaseAtomic::acquireUDTNewStorageOperationCommitGuard(
    const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const
{
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::AuthorityStorageNewOperationCommitGuard;

    if (!metadata)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT storage final commit has no immutable metadata image");
    metadata->validateBoundUDTReferences();
    if (!metadata->getBoundUDTReferences())
        return {};
    if (kind == AuthorityQuarantineOperationKind::Read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT storage final commit cannot use the read operation kind");

    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT storage final commit has no active verification runtime");

    auto database_owner = shared_from_this();
    AuthorityStorageNewOperationCommitGuard commit_guard(std::move(database_owner), runtime);
    /// This repeats the complete exact-root/stamp/quarantine admission only
    /// after the fence is held. Runtime publication cannot cross the check or
    /// the engine commit that retains the returned guard.
    assertUDTNewStorageOperationAllowed(metadata, kind);
    return commit_guard;
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

void DatabaseAtomic::assertUDTStorageReadContinuationAllowed(
    const UDT::AuthorityStorageReadContinuationEvidence & evidence, const StorageMetadataPtr & metadata) const
{
    using UDT::AuthorityObjectImageIdentity;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::AuthorityQuarantineOperationTiming;
    using UDT::AuthorityReadContinuationObjectProofView;
    using UDT::AuthorityRootGraphIdentity;
    using UDT::AuthorityVerificationStamp;
    using UDT::AuthorityVerificationStampError;
    using UDT::collectAuthorityVerificationRequiredDefinitions;
    using UDT::SchemaObjectID;
    using UDT::validateAndRebaseAuthorityVerificationStamp;

    if (!metadata)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT read continuation has no immutable metadata image");
    metadata->validateBoundUDTReferences();
    const auto & bound = metadata->getBoundUDTReferences();
    const auto & expectation = metadata->getBoundUDTExpectation();
    const auto & stamp = metadata->getBoundUDTVerificationStamp();
    if (!bound || !expectation || !stamp || !evidence.getVerificationStamp())
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT read continuation lost its verification evidence");

    const AuthorityObjectImageIdentity current_object{
        .object = bound->getObject(),
        .object_schema_revision = bound->getObjectSchemaRevision(),
        .sidecar_hash = bound->getSidecarHash(),
        .physical_schema_fingerprint = bound->getPhysicalSchemaFingerprint(),
    };
    if (current_object != evidence.getObjectImage())
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT read continuation metadata changed after admission");
    const auto required_definitions = collectAuthorityVerificationRequiredDefinitions(*bound);
    AuthorityRootGraphIdentity continuation_root = evidence.getPinnedRoot();
    AuthorityVerificationStamp::Ptr continuation_stamp = evidence.getVerificationStamp();
    if (continuation_stamp->getVerifiedRoot() != evidence.getPinnedRoot().authority_root
        || continuation_stamp->getVerifiedObject() != current_object
        || continuation_stamp->getRequiredDefinitions().size() != required_definitions.size()
        || !std::equal(required_definitions.begin(), required_definitions.end(), continuation_stamp->getRequiredDefinitions().begin()))
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT read continuation evidence is stale or incomplete");

    /// A disjoint authority publication may occur after the read pins its
    /// immutable storage image and before an unrelated object is quarantined.
    /// Rebase the compact proof onto the current exact root only after proving
    /// that the rooted expectation, object image, retained definitions, and
    /// dependency edges are byte-for-byte unchanged.  This preserves the
    /// started-read contract without treating an old aggregate root identity
    /// as evidence for the new quarantine closure.
    auto * authority = active_udt_authority.load(std::memory_order_acquire);
    auto * runtime = active_udt_verification_runtime.load(std::memory_order_acquire);
    if (!authority || !runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT read continuation has no active authority runtime");
    auto current_root = authority->acquireCurrentRoot();
    if (!current_root || active_udt_authority.load(std::memory_order_acquire) != authority
        || active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        throw Exception(ErrorCodes::ABORTED, "Atomic UDT authority changed while rebasing a read continuation");
    const AuthorityRootGraphIdentity current_root_identity = authorityRootGraphIdentity(current_root.get());
    if (current_root_identity != continuation_root)
    {
        try
        {
            continuation_stamp = validateAndRebaseAuthorityVerificationStamp(current_root.get(), *expectation, *bound, *continuation_stamp);
            continuation_root = current_root_identity;
        }
        catch (const AuthorityVerificationStampError &)
        {
            throw Exception(
                ErrorCodes::ABORTED, "Atomic UDT read continuation cannot prove its unchanged image against the current authority root");
        }
    }
    const std::array<SchemaObjectID, 1> touched{current_object.object};
    const std::array<AuthorityReadContinuationObjectProofView, 1> proofs{AuthorityReadContinuationObjectProofView{
        .current_object = current_object,
        .last_verification_stamp = continuation_stamp.get(),
        .sorted_unique_current_required_definitions = required_definitions,
    }};

    const auto decision = runtime->decideOperation({
        .kind = AuthorityQuarantineOperationKind::Read,
        .timing = AuthorityQuarantineOperationTiming::StartedBeforeQuarantine,
        .pinned_root = continuation_root,
        .touch_set_is_complete = true,
        .sorted_unique_touched_objects = touched,
        .continuation_proof_set_is_complete = true,
        .sorted_unique_continuation_proofs = proofs,
    });
    if (!decision.isAllowed())
    {
        throw Exception(
            ErrorCodes::ABORTED, "Atomic UDT quarantine rejected a read continuation (status {})", static_cast<unsigned>(decision.status));
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

DatabaseAtomic::CrossDatabaseMoveGuard::CrossDatabaseMoveGuard(
    std::unique_lock<std::mutex> lock_,
    Kind kind_,
    StoragePtr source_table_,
    String source_table_name_,
    UUID source_table_uuid_,
    String source_relative_table_path_,
    String target_table_name_for_logs_,
    UUID target_incoming_table_uuid_) noexcept
    : lock(std::move(lock_))
    , kind(kind_)
    , source_table(std::move(source_table_))
    , source_table_name(std::move(source_table_name_))
    , source_table_uuid(source_table_uuid_)
    , source_relative_table_path(std::move(source_relative_table_path_))
    , target_table_name_for_logs(std::move(target_table_name_for_logs_))
    , target_incoming_table_uuid(target_incoming_table_uuid_)
{
}

DatabaseAtomic::UDTDetachGuard::UDTDetachGuard(std::unique_lock<std::mutex> lock_, Kind kind_, StoragePtr expected_table_) noexcept
    : lock(std::move(lock_))
    , kind(kind_)
    , expected_table(std::move(expected_table_))
{
}

struct DatabaseAtomic::MappedObjectAuthorityImage
{
    UDT::AtomicAuthority::RootSnapshot root;
    UDT::AtomicDatabaseSchemaMutationDependentObjectImage image;
    ASTPtr trusted_create_query;
};

DatabaseAtomic::MappedObjectAuthorityImage
DatabaseAtomic::loadExactMappedObjectAuthorityImage(UUID object_uuid, std::string_view object_name) const
{
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::SchemaObjectKind;

    if (object_uuid == UUIDHelpers::Nil || object_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot load an Atomic mapped-object image without an exact identity");

    const auto current_database_name = getDatabaseName();
    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> root;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown || udt_table_startup_state || !udt_authority || !udt_mutation_storage
            || active_udt_authority.load(std::memory_order_acquire) != udt_authority.get())
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Mapped object {}.{} requires one active, fully recovered Atomic authority",
                current_database_name,
                object_name);
        }
        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        root.emplace(authority->acquireCurrentRoot());
    }
    if (!*root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped-object lookup has no authority root");

    const auto * expectation = root->get().findExpectationRecord(object_uuid);
    if (!expectation || expectation->object.database_uuid != db_uuid || expectation->object.object_uuid != object_uuid)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Mapped object {}.{} is absent or ambiguous in its exact Atomic authority root",
            current_database_name,
            object_name);
    }

    const auto inventory = root->get().pinAuthorityInventory();
    const auto graph = root->get().pinSchemaObjectDependencyGraph();
    if (!inventory || !graph)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped-object authority root has no inventory or dependency graph");

    const std::array selected_objects{expectation->object};
    auto reconciliation = storage->readAndReconcileAuthorityRecordsForObjects(*inventory, *graph, selected_objects);
    auto image = findExactDependentObjectImage(std::move(reconciliation), expectation->object);
    if (image.expectation != *expectation || image.object_name != object_name)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Mapped object {}.{} differs from its durable installation identity",
            current_database_name,
            object_name);
    }

    const String current_metadata = readMetadataFile(getDisk(), getObjectMetadataPath(String(object_name)));
    if (current_metadata != image.canonical_metadata_bytes)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Mapped object {}.{} metadata differs from its durable canonical image",
            current_database_name,
            object_name);
    }

    auto trusted_create_query = parseMappedTableStartupMetadata(image.canonical_metadata_bytes, current_database_name, image.object_name);
    const auto & trusted_create = trusted_create_query->as<const ASTCreateQuery &>();
    const auto trusted_kind = trusted_create.is_dictionary ? SchemaObjectKind::Dictionary
                                                           : (trusted_create.isView() ? SchemaObjectKind::View : SchemaObjectKind::Table);
    if (trusted_create.uuid != object_uuid || trusted_kind != expectation->object.kind)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Mapped object {}.{} canonical metadata describes another object",
            current_database_name,
            object_name);
    }

    return {
        .root = std::move(*root),
        .image = std::move(image),
        .trusted_create_query = std::move(trusted_create_query),
    };
}

bool DatabaseAtomic::isExactTemporarilyDetachedUDTObject(const UDT::SchemaObjectID & object, std::string_view object_name) const
{
    if (!object.isValid() || object.database_uuid != db_uuid || object_name.empty())
        return false;

    const auto current_database_name = getDatabaseName();
    const auto current_metadata_path = getObjectMetadataPath(String(object_name));
    /// The verifier already pins and validates the exact rooted expectation,
    /// installation, sidecar, and metadata image under this same schema lock.
    /// This seam contributes only the orthogonal O(1) live-catalog absence
    /// proof; lifecycle ATTACH/DETACH continues to use full reconciliation.
    std::lock_guard tables_lock(mutex);
    const auto detached = snapshot_detached_tables.find(String(object_name));
    return detached != snapshot_detached_tables.end() && detached->second.uuid == object.object_uuid
        && detached->second.database == current_database_name && detached->second.table == object_name
        && detached->second.metadata_path == current_metadata_path && !detached->second.is_permanently;
}

void DatabaseAtomic::validateMappedObjectForTemporaryDetach(const StoragePtr & table, ContextPtr local_context) const
{
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate a mapped DETACH without a storage");
    const auto table_id = table->getStorageID();
    if (table_id.database_name != getDatabaseName() || table_id.table_name.empty() || table_id.uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic DETACH storage identity is invalid");

    auto authority_image = loadExactMappedObjectAuthorityImage(table_id.uuid, table_id.table_name);
    if (mappedSchemaObjectKindForStorage(*table) != authority_image.image.expectation.object.kind)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic DETACH storage kind differs from its durable identity");

    const auto metadata = table->getInMemoryMetadataPtr(local_context, false);
    if (!metadata)
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic DETACH storage has no metadata snapshot");
    metadata->validateBoundUDTReferences();
    const auto & bound = metadata->getBoundUDTReferences();
    const auto & expectation = metadata->getBoundUDTExpectation();
    if (!bound || !expectation || *expectation != authority_image.image.expectation || bound->getObject() != expectation->object
        || bound->getObjectSchemaRevision() != expectation->object_schema_revision || bound->getSidecarHash() != expectation->sidecar_hash
        || bound->getPhysicalSchemaFingerprint() != expectation->physical_schema_fingerprint)
    {
        throw Exception(ErrorCodes::ABORTED, "Mapped Atomic DETACH runtime binding differs from its exact durable authority image");
    }
    assertUDTNewStorageOperationAllowed(metadata, UDT::AuthorityQuarantineOperationKind::DDL);
}

bool DatabaseAtomic::hasDatabaseOwnedTableExpectationForCrossDatabaseMove(UUID table_uuid) const
{
    std::optional<UDT::AtomicAuthority::RootSnapshot> snapshot;
    UDT::AtomicDatabaseSchemaMutationStorage * durable_storage = nullptr;
    std::shared_ptr<const UDT::AtomicAuthorityStartupStatusSnapshot> degraded_status;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown)
            throw Exception(
                ErrorCodes::ABORTED,
                "Cannot inspect mapped tables while their Atomic "
                "database is shutting down");
        if (udt_authority)
            snapshot.emplace(udt_authority->acquireCurrentRoot());
        durable_storage = udt_mutation_storage.get();
        degraded_status = udt_degraded_startup_status;
    }

    if (degraded_status)
    {
        /// A database-wide boundary must preserve the unavailable durable
        /// authority. Object boundaries remain exact: an ordinary physical
        /// table is not made unavailable merely because another object or the
        /// definition catalog failed recovery.
        if (table_uuid == UUIDHelpers::Nil)
            return true;
        if (degraded_status->hasUnknownDependentObjectScope())
            return true;
        if (degraded_status->containsExpectedDependentObject(table_uuid))
            return true;
        const UDT::AtomicDatabaseSchemaMutationPaths paths(metadata_path, db_uuid, getDatabaseName());
        return hasExactDegradedUDTObjectArtifacts(getDisk(), paths, db_uuid, table_uuid);
    }

    std::optional<UDT::AuthorityState> durable_state;
    std::optional<UInt64> recovery_required_transaction_id;
    bool durable_authority_marker = false;
    if (durable_storage)
    {
        recovery_required_transaction_id = durable_storage->getRecoveryRequiredTransactionID();
        durable_authority_marker = durable_storage->hasBoundedDurableAuthorityHead();
        durable_state = durable_storage->getCurrentAuthorityState();
    }

    return UDT::hasDatabaseOwnedTableExpectationForCrossDatabaseMove(
        {
            .database_uuid = db_uuid,
            .published_root = snapshot && *snapshot ? &snapshot->get() : nullptr,
            .durable_storage_present = durable_storage != nullptr,
            .durable_state = durable_state,
            .recovery_required_transaction_id = recovery_required_transaction_id,
            .durable_authority_marker = durable_authority_marker,
        },
        table_uuid);
}

bool DatabaseAtomic::hasDatabaseOwnedUDTObject(UUID object_uuid) const
{
    return hasDatabaseOwnedTableExpectationForCrossDatabaseMove(object_uuid);
}

bool DatabaseAtomic::hasDatabaseOwnedUDTObjectForQueryCache(UUID object_uuid) const
{
    return hasDatabaseOwnedUDTObject(object_uuid);
}

bool DatabaseAtomic::hasDatabaseOwnedUDTTableBinding(const StoragePtr & table, ContextPtr local_context) const
{
    /// Audit the database-owned published/durable view even when storage-local
    /// bindings are already present. Short-circuiting on the local metadata
    /// would let a split-brain authority mutate its mapped table.
    const bool has_database_owned_expectation = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(table->getStorageID().uuid);
    /// Durable UDT bindings belong to the outer storage metadata. In
    /// particular, `StorageMaterializedView::getInMemoryMetadataPtr` resolves
    /// its target through the catalog; doing that while `attachTable` owns the
    /// schema-mutation mutex can wait for another table whose attach needs the
    /// same mutex. Read the exact outer snapshot without storage decoration.
    const auto metadata = table->IStorage::getInMemoryMetadataPtr(local_context, true);
    if (!metadata)
        throw Exception(ErrorCodes::ABORTED, "Cannot inspect mapped table without a metadata snapshot");
    metadata->validateBoundUDTReferences();
    return has_database_owned_expectation || metadata->getBoundUDTReferences() || metadata->getBoundUDTExpectation();
}

void DatabaseAtomic::assertNotLiveMappedMaterializedViewInnerTable(const StoragePtr & table, std::string_view operation) const
{
    if (!table)
        return;
    assertNotLiveMappedMaterializedViewInnerTable(table->getStorageID().table_name, operation);
}

void DatabaseAtomic::assertNotLiveMappedMaterializedViewInnerTable(std::string_view table_name, std::string_view operation) const
{
    const auto owner_uuid = tryGetAtomicMaterializedViewOwnerUUID(table_name);
    if (!owner_uuid || !hasDatabaseOwnedTableExpectationForCrossDatabaseMove(*owner_uuid))
        return;

    /// Resolve ownership from the generated name plus the database authority,
    /// not from the live catalog: a temporarily detached mapped outer MV still
    /// owns its physical child and must protect it from independent mutation.
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot {} physical inner table {}.{} while mapped MaterializedView UUID {} still owns it",
        operation,
        getDatabaseName(),
        table_name,
        toString(*owner_uuid));
}

void DatabaseAtomic::assertUDTTableAllowsOrdinaryMetadataMutation(
    const StoragePtr & table, ContextPtr local_context, std::string_view operation) const
{
    if (!hasDatabaseOwnedUDTTableBinding(table, local_context))
        return;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot {} mapped table {} until its user-defined type "
        "metadata transaction is implemented",
        operation,
        table->getStorageID().getNameForLogs());
}

void DatabaseAtomic::assertUDTPhysicalInnerTableOperationAllowed(const StoragePtr & table, std::string_view operation) const
{
    waitDatabaseStarted();
    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    assertNotLiveMappedMaterializedViewInnerTable(table, operation);
}

void DatabaseAtomic::assertUDTPhysicalInnerTableNameOperationAllowed(std::string_view table_name, std::string_view operation) const
{
    waitDatabaseStarted();
    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    assertNotLiveMappedMaterializedViewInnerTable(table_name, operation);
}

void DatabaseAtomic::assertUDTTableUUIDAllowsOrdinaryMetadataMutation(
    UUID table_uuid, std::string_view table_name, std::string_view operation) const
{
    if (table_uuid == UUIDHelpers::Nil)
        return;

    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    if (!hasDatabaseOwnedTableExpectationForCrossDatabaseMove(table_uuid))
        return;

    /// Server startup has its own recovered-entry binding path. Preserve that
    /// internal admission while rejecting every user-visible metadata rewrite.
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_table_startup_state && udt_table_startup_state->findExactEntry(table_uuid, table_name))
        {
            return;
        }
    }

    const auto current_database_name = getDatabaseName();
    if (operation != "ATTACH")
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot {} mapped object {}.{}; only exact short ATTACH after a temporary DETACH is supported",
            operation,
            current_database_name,
            table_name);
    }

    const auto current_metadata_path = getObjectMetadataPath(String(table_name));
    {
        std::lock_guard tables_lock(mutex);
        const auto detached = snapshot_detached_tables.find(String(table_name));
        if (detached == snapshot_detached_tables.end() || detached->second.uuid != table_uuid
            || detached->second.database != current_database_name || detached->second.table != table_name
            || detached->second.metadata_path != current_metadata_path || detached->second.is_permanently)
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Mapped object {}.{} is not the exact temporarily detached object requested by ATTACH",
                current_database_name,
                table_name);
        }
    }

    /// Reconcile the exact durable installation and compare the current SQL
    /// bytes before StorageFactory or ATTACH-AS conversion can perform work.
    static_cast<void>(loadExactMappedObjectAuthorityImage(table_uuid, table_name));
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

DatabaseAtomic::UDTDetachGuard
DatabaseAtomic::acquireUDTTableDetachGuard(const StoragePtr & table, ContextPtr local_context, std::string_view operation) const
{
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot acquire an Atomic DETACH guard without a table");

    waitDatabaseStarted();
    UniqueLock schema_mutation_lock(udt_schema_mutation_mutex);
    assertNotLiveMappedMaterializedViewInnerTable(table, operation);
    if (hasDatabaseOwnedUDTTableBinding(table, local_context))
    {
        if (operation != "DETACH")
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot {} mapped object {}; only temporary DETACH with retained authority mapping is supported",
                operation,
                table->getStorageID().getNameForLogs());
        }
        validateMappedObjectForTemporaryDetach(table, local_context);
    }
    return UDTDetachGuard(std::move(schema_mutation_lock.getUnderlyingLock()), UDTDetachGuard::Kind::Table, table);
}

DatabaseAtomic::UDTDetachGuard DatabaseAtomic::acquireUDTDatabaseDetachGuard(std::string_view operation) const
{
    waitDatabaseStarted();
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
    assertUDTDatabaseAllowsDetach(operation);
    return UDTDetachGuard(std::move(schema_mutation_lock), UDTDetachGuard::Kind::Database, {});
}

void DatabaseAtomic::assertOwnsUDTDetachGuard(const UDTDetachGuard & guard, const StoragePtr & expected_table) const
{
    if (!guard.lock.owns_lock() || guard.lock.mutex() != &udt_schema_mutation_mutex
        || (guard.kind == UDTDetachGuard::Kind::Table && guard.expected_table != expected_table))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic UDT DETACH guard has the wrong database, table, or lock state");
    }
}

StoragePtr
DatabaseAtomic::detachTableUnderUDTGuard(ContextPtr, const String & name, const StoragePtr & expected_table, const UDTDetachGuard & guard)
{
    assertOwnsUDTDetachGuard(guard, expected_table);
    return detachTableWithoutUDTGuard(name, expected_table, false).first;
}

void DatabaseAtomic::detachTablePermanentlyUnderUDTGuard(
    ContextPtr, const String & name, const StoragePtr & expected_table, const UDTDetachGuard & guard)
{
    assertOwnsUDTDetachGuard(guard, expected_table);
    auto table = detachTableWithoutUDTGuard(name, expected_table, false).first;

    fs::path detached_permanently_flag(getObjectMetadataPath(name) + detached_suffix);
    try
    {
        auto db_disk = getDisk();
        db_disk->createFile(detached_permanently_flag);

        std::lock_guard tables_lock(mutex);
        const auto it = snapshot_detached_tables.find(name);
        if (it == snapshot_detached_tables.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Snapshot doesn't contain info about detached table `{}`", name);
        it->second.is_permanently = true;
    }
    catch (Exception & e)
    {
        e.addMessage(
            "while trying to set permanently detached flag. Table {}.{} may be reattached during server restart.",
            backQuote(getDatabaseName()),
            backQuote(name));
        throw;
    }
    static_cast<void>(table);
}

DatabaseAtomic::CrossDatabaseMoveGuard
DatabaseAtomic::acquireUDTCrossDatabaseTargetGuard(UUID incoming_table_uuid, std::string_view table_name_for_logs)
{
    waitDatabaseStarted();
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
    UDT::assertTableCanEnterAtomicDatabase(
        hasDatabaseOwnedTableExpectationForCrossDatabaseMove(incoming_table_uuid), table_name_for_logs, getDatabaseName());
    return CrossDatabaseMoveGuard(
        std::move(schema_mutation_lock),
        CrossDatabaseMoveGuard::Kind::Target,
        {},
        {},
        UUIDHelpers::Nil,
        {},
        String(table_name_for_logs),
        incoming_table_uuid);
}

DatabaseAtomic::CrossDatabaseMoveGuard DatabaseAtomic::acquireUDTCrossDatabaseSourceGuard(
    const String & name, const StoragePtr & table, const String & relative_table_path, ContextPtr local_context)
{
    /// DatabaseOnDisk acquires the table-exclusive lock before entering this
    /// boundary. Keep that table -> schema order consistent with ALTER.
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
    const auto table_id = table->getStorageID();
    {
        std::lock_guard tables_lock(mutex);
        const auto table_it = tables.find(name);
        const auto path_it = table_name_to_path.find(name);
        if (table_it == tables.end() || table_it->second != table || path_it == table_name_to_path.end()
            || path_it->second != relative_table_path || table_id.database_name != database_name || table_id.table_name != name
            || table_id.uuid == UUIDHelpers::Nil)
        {
            throw Exception(ErrorCodes::ABORTED, "Atomic source table changed before cross-database move admission");
        }
    }
    assertNotLiveMappedMaterializedViewInnerTable(table, "move across databases");
    UDT::assertTableCanLeaveAtomicDatabase(hasDatabaseOwnedUDTTableBinding(table, local_context), table->getStorageID().getNameForLogs());
    return CrossDatabaseMoveGuard(
        std::move(schema_mutation_lock),
        CrossDatabaseMoveGuard::Kind::Source,
        table,
        name,
        table_id.uuid,
        relative_table_path,
        {},
        UUIDHelpers::Nil);
}

void DatabaseAtomic::assertOwnsUDTCrossDatabaseGuard(const CrossDatabaseMoveGuard & guard, CrossDatabaseMoveGuard::Kind expected_kind) const
{
    if (!guard.lock.owns_lock() || guard.lock.mutex() != &udt_schema_mutation_mutex || guard.kind != expected_kind)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Atomic cross-database move guard has the wrong database, "
            "capability kind, or lock state");
    }
}

void DatabaseAtomic::releaseUDTCrossDatabaseGuardForNestedTableRename(CrossDatabaseMoveGuard & guard) const
{
    if (!guard.lock.owns_lock() || guard.lock.mutex() != &udt_schema_mutation_mutex)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic nested-table rename guard has the wrong database or lock state");
    guard.lock.unlock();
}

void DatabaseAtomic::reacquireUDTCrossDatabaseGuardAfterNestedTableRename(CrossDatabaseMoveGuard & guard) const
{
    if (guard.lock.owns_lock() || guard.lock.mutex() != &udt_schema_mutation_mutex)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic nested-table rename guard has the wrong database or lock state");
    }
    guard.lock.lock();
}

void DatabaseAtomic::validateUDTCrossDatabaseGuardAfterNestedTableRename(
    const CrossDatabaseMoveGuard & guard, ContextPtr local_context) const
{
    assertOwnsUDTCrossDatabaseGuard(guard, guard.kind);
    if (guard.kind == CrossDatabaseMoveGuard::Kind::Target)
    {
        if (guard.target_incoming_table_uuid == UUIDHelpers::Nil
            || !DatabaseCatalog::instance().hasUUIDMapping(guard.target_incoming_table_uuid))
        {
            throw Exception(ErrorCodes::ABORTED, "Atomic target UUID reservation was lost during nested-table rename");
        }
        UDT::assertTableCanEnterAtomicDatabase(
            hasDatabaseOwnedTableExpectationForCrossDatabaseMove(guard.target_incoming_table_uuid),
            guard.target_table_name_for_logs,
            getDatabaseName());
        return;
    }

    const auto table_id = guard.source_table->getStorageID();
    {
        std::lock_guard tables_lock(mutex);
        const auto detached_it = detached_tables.find(guard.source_table_uuid);
        if (tables.contains(guard.source_table_name) || table_name_to_path.contains(guard.source_table_name)
            || detached_it == detached_tables.end() || detached_it->second != guard.source_table
            || table_id.database_name != database_name || table_id.table_name != guard.source_table_name
            || table_id.uuid != guard.source_table_uuid)
        {
            throw Exception(ErrorCodes::ABORTED, "Atomic source table changed during nested-table cross-database rename");
        }
    }
    assertNotLiveMappedMaterializedViewInnerTable(guard.source_table, "continue a move across databases");
    UDT::assertTableCanLeaveAtomicDatabase(
        hasDatabaseOwnedUDTTableBinding(guard.source_table, local_context), table_id.getNameForLogs());
}

void DatabaseAtomic::attachTableUnderUDTCrossDatabaseGuard(
    const String & name, const StoragePtr & table, const String & relative_table_path, const CrossDatabaseMoveGuard & guard)
{
    assertOwnsUDTCrossDatabaseGuard(guard, CrossDatabaseMoveGuard::Kind::Source);
    const auto table_id = table->getStorageID();
    if (guard.source_table != table || guard.source_table_name != name || guard.source_table_uuid != table_id.uuid
        || guard.source_relative_table_path != relative_table_path || table_id.database_name != getDatabaseName()
        || table_id.table_name != name)
    {
        throw Exception(ErrorCodes::ABORTED, "Atomic source table changed before cross-database move rollback");
    }
    attachTableWithoutUDTGuard(name, table, relative_table_path);
}

StoragePtr DatabaseAtomic::detachTableUnderUDTCrossDatabaseGuard(const String & name, const CrossDatabaseMoveGuard & guard)
{
    assertOwnsUDTCrossDatabaseGuard(guard, CrossDatabaseMoveGuard::Kind::Source);
    const auto table_id = guard.source_table->getStorageID();
    if (guard.source_table_name != name || guard.source_table_uuid != table_id.uuid || table_id.database_name != getDatabaseName()
        || table_id.table_name != name)
    {
        throw Exception(ErrorCodes::ABORTED, "Atomic source table changed before cross-database move detach");
    }
    return detachTableWithoutUDTGuard(name, guard.source_table, false).first;
}

void DatabaseAtomic::ensureUDTDependentObjectCapabilities()
{
    using UDT::activateDependentObjectAuthority;
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::AuthorityState;
    using UDT::DatabaseSchemaMutationGuard;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::DatabaseSchemaWALRecoveryDecision;
    using UDT::definition_authority_capability_mask;
    using UDT::dependent_object_authority_capability_mask;
    using UDT::DependentObjectActivationPlanner;
    using UDT::discardUnpreparedDatabaseSchemaMutationStaging;
    using UDT::executeDatabaseSchemaMutation;
    using UDT::recoverDatabaseSchemaMutation;
    using UDT::retireRolledBackDatabaseSchemaMutation;

    /// The dependent-object caller already owns its ordinary
    /// database/table DDL guard. Acquiring an empty-key/catalog guard here would
    /// invert that external lock order; this boundary owns only the common
    /// Atomic schema-mutation serialization below.
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> snapshot;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_mode != AuthorityMode::Enabled)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "{} databases cannot activate dependent-object-capable user-defined types", getEngineName());
        if (udt_authority_shutdown)
            throw Exception(ErrorCodes::ABORTED, "Cannot activate dependent-object-capable user-defined types after database shutdown");
        if (udt_degraded_startup_status)
            throw Exception(
                ErrorCodes::ABORTED,
                "Cannot activate dependent-object-capable user-defined types while durable authority recovery is invalid or incomplete");
        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        auto * active_authority = active_udt_authority.load(std::memory_order_acquire);
        if (!authority && !storage && !active_authority)
        {
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Cannot admit a dependent user-defined type object before a definition-only "
                "user-defined type authority is published");
        }
        if (!authority || !storage || active_authority != authority)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Atomic user-defined type authority components have "
                "inconsistent activation state");
        snapshot.emplace(authority->acquireCurrentRoot());
    }

    if (!*snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Active Atomic user-defined type authority has no published root");
    const auto & current_root = snapshot->get();
    const UInt64 current_capabilities = current_root.getPersistentCapabilityMask();
    if (current_capabilities != definition_authority_capability_mask && current_capabilities != dependent_object_authority_capability_mask)
    {
        throw DatabaseSchemaMutationReplayConflictError("live Atomic authority has an unknown persistent capability set");
    }

    const AuthorityState current_state = current_root.getAuthorityState();
    const auto durable_state = storage->getCurrentAuthorityState();
    if (!durable_state)
        throw DatabaseSchemaMutationReplayConflictError("live Atomic authority has no durable authority state");

    const auto pending_recovery = storage->getRecoveryRequiredTransactionID();
    if (current_capabilities == dependent_object_authority_capability_mask)
    {
        if (*durable_state != current_state)
            throw DatabaseSchemaMutationReplayConflictError(
                "published dependent-object-capable authority differs from the durable WAL head");
        if (pending_recovery)
            throw DatabaseSchemaMutationReplayConflictError(
                "published dependent-object-capable authority is fail-stopped by another schema "
                "mutation");
        return;
    }

    const AuthorityState activated_state = activateDependentObjectAuthority(current_state);
    if (*durable_state != current_state && *durable_state != activated_state)
        throw DatabaseSchemaMutationReplayConflictError("published definition-only authority differs from the durable WAL head");

    const auto prepare_activation = [&](UInt64 transaction_id)
    {
        auto activation = DependentObjectActivationPlanner::plan(current_root, transaction_id, current_root.getDatabaseCatalogEpoch());
        auto publication = authority->preparePublication(activation.releaseReplacementRoot());
        return std::pair(std::move(activation), std::move(publication));
    };

    const auto finish_successful_publication = [&]
    {
        snapshot.reset();
        schema_mutation_lock.unlock();
        try
        {
            static_cast<void>(authority->scanRetired());
        }
        catch (...)
        {
        }
    };

    const auto validate_recovery_image = [](const auto & image, const auto & activation)
    {
        if (image.prepare != activation.getValidatedTransition().getPrepare() || !image.staged_artifact_bytes.empty())
        {
            throw DatabaseSchemaMutationReplayConflictError(
                "pending schema mutation is not the exact content-neutral dependent-object-capable "
                "activation");
        }
        if (image.recovery_decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared && image.commit)
            throw DatabaseSchemaMutationReplayConflictError("rolled-back dependent-object-capable activation also has a Commit marker");
        if (image.recovery_decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted && !image.commit)
            throw DatabaseSchemaMutationReplayConflictError("completed dependent-object-capable activation has no Commit marker");
    };

    const auto recover_activation = [&](UInt64 transaction_id, DatabaseSchemaMutationGuard & recovery_guard)
    {
        auto image = storage->loadTransactionForRecovery(transaction_id);
        auto [activation, publication] = prepare_activation(transaction_id);
        validate_recovery_image(image, activation);
        const auto decision = recoverDatabaseSchemaMutation(*storage, recovery_guard, activation.getValidatedTransition(), image.commit);
        if (decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
        {
            authority->publish(std::move(publication));
            return true;
        }
        retireRolledBackDatabaseSchemaMutation(*storage, recovery_guard, transaction_id);
        return false;
    };

    auto transaction_ids = storage->listDurableTransactionIDs();
    if (pending_recovery)
    {
        auto recovery_guard = storage->issueMutationGuard();
        const bool has_durable_prepare = std::binary_search(transaction_ids.begin(), transaction_ids.end(), *pending_recovery);
        if (!has_durable_prepare)
        {
            if (*durable_state != current_state || recovery_guard.getDurablePredecessorTransactionID() == std::numeric_limits<UInt64>::max()
                || *pending_recovery != recovery_guard.getDurablePredecessorTransactionID() + 1)
            {
                throw DatabaseSchemaMutationReplayConflictError(
                    "process-local schema recovery latch does not match the next "
                    "unprepared transaction");
            }
            discardUnpreparedDatabaseSchemaMutationStaging(*storage, recovery_guard, *pending_recovery);
        }
        else
        {
            if (transaction_ids.empty() || transaction_ids.back() != *pending_recovery)
                throw DatabaseSchemaMutationReplayConflictError(
                    "schema recovery latch does not name the terminal durable "
                    "transaction");
            if (recover_activation(*pending_recovery, recovery_guard))
            {
                finish_successful_publication();
                return;
            }
            /// This is a retry call, not the operation that reported the
            /// indeterminate result. Once rollback is durable it may make one
            /// fresh activation attempt below; any new failure is returned to
            /// the dependent-object caller without starting that mutation.
        }
        transaction_ids = storage->listDurableTransactionIDs();
    }
    else if (*durable_state == activated_state)
    {
        if (transaction_ids.empty())
            throw DatabaseSchemaMutationReplayConflictError("durable dependent-object-capable authority has no activation transaction");
        auto recovery_guard = storage->issueMutationGuard();
        if (!recover_activation(transaction_ids.back(), recovery_guard))
            throw DatabaseSchemaMutationReplayConflictError("durable dependent-object-capable authority recovered as rolled back");
        finish_successful_publication();
        return;
    }

    storage->maintainCheckpointBeforeMutation(current_root);
    auto guard = storage->issueMutationGuard();
    const UInt64 durable_predecessor = guard.getDurablePredecessorTransactionID();
    if (durable_predecessor == std::numeric_limits<UInt64>::max())
        throw DatabaseSchemaMutationReplayConflictError("Atomic schema transaction ID domain is exhausted");
    const UInt64 transaction_id = durable_predecessor + 1;
    auto [activation, publication] = prepare_activation(transaction_id);

    try
    {
        static_cast<void>(executeDatabaseSchemaMutation(*storage, guard, activation.getValidatedTransition()));
    }
    catch (...)
    {
        const auto original = std::current_exception();
        if (guard.getState() == DatabaseSchemaMutationGuard::State::Ready)
        {
            discardUnpreparedDatabaseSchemaMutationStaging(*storage, guard, transaction_id);
            std::rethrow_exception(original);
        }

        if (storage->getRecoveryRequiredTransactionID() != transaction_id)
            throw DatabaseSchemaMutationReplayConflictError("dependent-object-capable activation recovery latch changed identity");
        transaction_ids = storage->listDurableTransactionIDs();
        if (!std::binary_search(transaction_ids.begin(), transaction_ids.end(), transaction_id))
        {
            discardUnpreparedDatabaseSchemaMutationStaging(*storage, guard, transaction_id);
            std::rethrow_exception(original);
        }

        auto image = storage->loadTransactionForRecovery(transaction_id);
        validate_recovery_image(image, activation);
        const auto decision = recoverDatabaseSchemaMutation(*storage, guard, activation.getValidatedTransition(), image.commit);
        if (decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
        {
            authority->publish(std::move(publication));
            finish_successful_publication();
            return;
        }
        retireRolledBackDatabaseSchemaMutation(*storage, guard, transaction_id);
        std::rethrow_exception(original);
    }

    authority->publish(std::move(publication));
    finish_successful_publication();
}

class DatabaseAtomic::TableCreateGuard::Impl final
{
public:
    Impl(
        DatabaseAtomic & owner_,
        std::unique_lock<std::mutex> schema_lock_,
        UDT::AtomicAuthority & authority_,
        UDT::AtomicDatabaseSchemaMutationStorage & storage_,
        UDT::AtomicAuthority::RootSnapshot planning_root_) noexcept
        : owner(&owner_)
        , schema_lock(std::move(schema_lock_))
        , authority(&authority_)
        , storage(&storage_)
        , planning_root(std::move(planning_root_))
    {
    }

    DatabaseAtomic * owner;
    std::unique_lock<std::mutex> schema_lock;
    UDT::AtomicAuthority * authority;
    UDT::AtomicDatabaseSchemaMutationStorage * storage;
    UDT::AtomicAuthority::RootSnapshot planning_root;
};

DatabaseAtomic::TableCreateGuard::TableCreateGuard(std::unique_ptr<Impl> impl_) noexcept
    : impl(std::move(impl_))
{
}

DatabaseAtomic::TableCreateGuard::TableCreateGuard(TableCreateGuard &&) noexcept = default;

DatabaseAtomic::TableCreateGuard & DatabaseAtomic::TableCreateGuard::operator=(TableCreateGuard &&) noexcept = default;

DatabaseAtomic::TableCreateGuard::~TableCreateGuard() = default;

const UDT::IAuthorityAdapter & DatabaseAtomic::TableCreateGuard::getAuthorityAdapter() const noexcept
{
    if (!impl || !impl->authority)
        std::terminate();
    return *impl->authority;
}

DatabaseAtomic::TableCreateGuard DatabaseAtomic::acquireUDTTableCreateGuard()
{
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::dependent_object_authority_capability_mask;

    waitDatabaseStarted();
    std::unique_lock schema_lock(udt_schema_mutation_mutex);

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> planning_root;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_mode != AuthorityMode::Enabled)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} databases cannot admit user-defined type tables", getEngineName());
        if (udt_authority_shutdown)
            throw Exception(ErrorCodes::ABORTED, "Cannot admit a user-defined type table after database shutdown");
        if (udt_table_startup_state)
            throw Exception(
                ErrorCodes::ABORTED,
                "Cannot admit a user-defined type table while "
                "mapped-table startup is pending");
        if (udt_degraded_startup_status)
            throw Exception(
                ErrorCodes::ABORTED, "Cannot admit a user-defined type object while durable authority recovery is invalid or incomplete");

        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        if (!authority || !storage || active_udt_authority.load(std::memory_order_acquire) != authority)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Atomic user-defined type table admission components are "
                "inconsistent");
        planning_root.emplace(authority->acquireCurrentRoot());
    }

    if (!*planning_root || planning_root->get().getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Atomic user-defined type table admission has no dependent-object-capable "
            "authority root");

    const auto durable_state = storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != planning_root->get().getAuthorityState())
        throw DatabaseSchemaMutationReplayConflictError("Atomic table-admission root differs from the durable authority head");
    if (storage->getRecoveryRequiredTransactionID())
        throw DatabaseSchemaMutationReplayConflictError("Atomic table admission is fail-stopped by another schema mutation");

    return TableCreateGuard(
        std::make_unique<TableCreateGuard::Impl>(*this, std::move(schema_lock), *authority, *storage, std::move(*planning_root)));
}

void DatabaseAtomic::authorizeUDTTableSourceSidecarCopy(
    const TableCreateGuard & guard,
    UDT::StoredObjectSourceMode source_mode,
    const UDT::PersistedTypeReferences & source_references,
    const UDT::BoundObjectTypeReferences & bound_source_references) const
{
    if (!guard.impl || guard.impl->owner != this || !guard.impl->schema_lock.owns_lock()
        || guard.impl->schema_lock.mutex() != &udt_schema_mutation_mutex || !guard.impl->authority || !guard.impl->storage
        || !guard.impl->planning_root || !udt_lifecycle_adapter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic Table source-sidecar admission guard is invalid");
    udt_lifecycle_adapter->authorizeTableSourceSidecarCopy(
        guard.impl->planning_root.get(), *guard.impl->storage, source_mode, source_references, bound_source_references);
}

void DatabaseAtomic::createTableWithUDTBindings(
    TableCreateGuard guard,
    ContextPtr query_context,
    const ASTPtr & physical_create_query,
    const StoragePtr & table,
    UDT::PreparedTableColumnTypeBindings table_bindings,
    UDT::StoredObjectSourceMode selected_output_source_mode)
{
    using UDT::AtomicTableMetadataValidator;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::DatabaseSchemaMutationIndeterminateDurabilityError;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::DependentObjectAdmissionCoordinator;
    using UDT::DependentObjectAdmissionPlanner;
    using UDT::discardUnpreparedDatabaseSchemaMutationStaging;
    using UDT::DurablyCommittedDependentObjectAdmission;
    using UDT::SchemaObjectID;
    using UDT::SchemaObjectKind;
    using UDT::StoredObjectSourceMode;

    bool startup_attempted = false;
    bool durable_confirmed = false;
    bool publication_complete = false;
    SCOPE_EXIT({
        if (table && !publication_complete && !durable_confirmed)
        {
            if (guard.impl && guard.impl->schema_lock.owns_lock())
                guard.impl->schema_lock.unlock();
            if (startup_attempted)
            {
                try
                {
                    table->shutdown();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Failed to stop an unpublished Atomic user-defined type table");
                }
            }
            if (table->storesDataOnDisk())
            {
                try
                {
                    table->drop();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Failed to clean up an unpublished Atomic user-defined type table");
                }
            }
        }
    });

    if (!guard.impl || guard.impl->owner != this || !guard.impl->schema_lock.owns_lock()
        || guard.impl->schema_lock.mutex() != &udt_schema_mutation_mutex || !guard.impl->authority || !guard.impl->storage
        || !guard.impl->planning_root)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type table CREATE guard is invalid");
    }
    if (!query_context || !physical_create_query || !table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type table CREATE has an incomplete input");

    const auto * create = physical_create_query->as<ASTCreateQuery>();
    const auto table_id = table->getStorageID();
    if (isAtomicMaterializedViewInnerTableName(table_id.table_name))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Atomic MaterializedView physical inner tables cannot acquire a separate user-defined type authority identity");
    }
    const String engine_name = create && create->storage && create->storage->engine ? create->storage->engine->name : String{};
    const bool is_memory = engine_name == "Memory" && table->getName() == "Memory" && !table->storesDataOnDisk();
    const bool is_non_replicated_merge_tree = table->isMergeTree() && table->storesDataOnDisk() && !table->isSharedStorage()
        && table->getName() == engine_name && !engine_name.starts_with("Replicated") && !engine_name.starts_with("Shared");
    if (!create || create->isTemporary() || create->isView() || create->is_dictionary || create->attach || create->if_not_exists
        || !create->cluster.empty() || !create->storage || !create->storage->engine || (!is_memory && !is_non_replicated_merge_tree))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Mapped Atomic table CREATE supports only fresh local Memory or "
            "non-replicated, non-shared MergeTree-family tables");
    }
    if (create->getDatabase() != getDatabaseName() || create->getTable() != table_id.table_name || create->uuid != table_id.uuid
        || table_id.database_name != getDatabaseName() || table_id.uuid == UUIDHelpers::Nil)
    {
        throw Exception(ErrorCodes::ABORTED, "Atomic user-defined type table identity changed before admission");
    }

    const SchemaObjectID expected_object{
        .kind = SchemaObjectKind::Table,
        .database_uuid = db_uuid,
        .object_uuid = table_id.uuid,
    };
    if (!table_bindings.persisted_references || !table_bindings.bound_physical_schema || !table_bindings.sidecar_expectation
        || table_bindings.persisted_references->object != expected_object
        || table_bindings.persisted_references->object_schema_revision != 1
        || table_bindings.bound_physical_schema->object != expected_object
        || table_bindings.bound_physical_schema->object_schema_revision != 1
        || table_bindings.bound_physical_schema->physical_schema_fingerprint
            != table_bindings.persisted_references->physical_schema_fingerprint
        || table_bindings.physical_schema_fingerprint != table_bindings.persisted_references->physical_schema_fingerprint
        || table_bindings.sidecar_expectation->object != expected_object || table_bindings.sidecar_expectation->object_schema_revision != 1
        || table_bindings.sidecar_expectation->physical_schema_fingerprint
            != table_bindings.persisted_references->physical_schema_fingerprint)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Atomic user-defined type table CREATE has incomplete or "
            "foreign bindings");
    }

    auto initial_metadata_handle = table->getInMemoryMetadataPtr(query_context, false);
    if (!initial_metadata_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type table has no metadata snapshot");
    StorageMetadataPtr initial_metadata = initial_metadata_handle;
    if (initial_metadata->getColumns().getAllPhysical() != table_bindings.physical_columns)
        throw Exception(
            ErrorCodes::ABORTED,
            "Atomic user-defined type table physical columns changed "
            "after binding");

    auto & authority = *guard.impl->authority;
    auto & storage = *guard.impl->storage;
    if (selected_output_source_mode != StoredObjectSourceMode::Unclassified)
    {
        udt_lifecycle_adapter->authorizeTableSelectedOutputs(
            guard.impl->planning_root.get(),
            storage,
            selected_output_source_mode,
            static_cast<UInt64>(table_bindings.physical_columns.size()),
            *table_bindings.persisted_references);
    }
    checkUsageAccessForFinalPersistedUDTDescriptors(query_context, *table_bindings.persisted_references);
    auto object_dependencies = collectMappedObjectDependencies(
        *this,
        guard.impl->planning_root.get(),
        physical_create_query,
        table->getStorageID().getQualifiedName(),
        table_bindings.persisted_references->object,
        query_context);

    storage.maintainCheckpointBeforeMutation(guard.impl->planning_root.get());
    auto mutation_guard = storage.issueMutationGuard();
    const UInt64 durable_predecessor = mutation_guard.getDurablePredecessorTransactionID();
    if (durable_predecessor == std::numeric_limits<UInt64>::max())
        throw DatabaseSchemaMutationReplayConflictError("Atomic schema transaction ID domain is exhausted");

    AtomicTableMetadataValidator metadata_validator(db_uuid, physical_create_query, table);
    auto prepared_admission = DependentObjectAdmissionPlanner::planTableCreate(
        guard.impl->planning_root.get(),
        durable_predecessor + 1,
        guard.impl->planning_root.get().getDatabaseCatalogEpoch(),
        table_bindings,
        object_dependencies,
        getObjectDefinitionFromCreateQuery(physical_create_query),
        metadata_validator);
    auto prepared_commit = DependentObjectAdmissionCoordinator::prepareTableCreateCommit(
        std::move(guard.impl->planning_root), authority, storage, mutation_guard, std::move(prepared_admission));
    assertUDTNewDefinitionClosureOperationAllowed(*prepared_commit.getBoundUDTReferences(), AuthorityQuarantineOperationKind::DDL);

    StorageInMemoryMetadata bound_metadata(*initial_metadata);
    bound_metadata.setColumnsAndBoundUDTReferences(
        initial_metadata->getColumns(), prepared_commit.getBoundUDTReferences(), prepared_commit.getSidecarExpectation());
    bound_metadata.setBoundUDTVerificationStamp(prepared_commit.getVerificationStamp());
    table->setInMemoryMetadata(bound_metadata);
    /// MergeTreeData::getInMemoryMetadataPtr() deliberately returns a defensive
    /// copy in debug and sanitizer builds.  The admission guard needs the
    /// identity of the actual IStorage MultiVersion snapshot so it can detect a
    /// real publication between binding and the durable commit.
    auto bound_metadata_handle = table->IStorage::getInMemoryMetadataPtr(nullptr, true);
    StorageMetadataPtr exact_bound_metadata = bound_metadata_handle;
    if (!exact_bound_metadata)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type table lost its bound metadata snapshot");
    exact_bound_metadata->validateBoundUDTReferences();

    try
    {
        startup_attempted = true;
        table->startup();

        const String table_name = table_id.table_name;
        String table_data_path = getTableDataPath(*create);
        auto database = shared_from_this();
        auto attached_metrics = getAttachedCountersForStorage(table);
        createDirectories();

        UniqueLock tables_lock(mutex);
        if (create->getDatabase() != database_name || table_id.database_name != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed before mapped table publication");
        if (tables.contains(table_name) || table_name_to_path.contains(table_name) || snapshot_detached_tables.contains(table_name))
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists or is detached", table_id.getFullTableName());
        assertDetachedTableNotInUse(table_id.uuid);
        const auto reserved_mapping = DatabaseCatalog::instance().tryGetByUUID(table_id.uuid);
        if (!DatabaseCatalog::instance().hasUUIDMapping(table_id.uuid) || reserved_mapping.first || reserved_mapping.second)
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS, "UUID reservation for table {} is no longer empty", table_id.getNameForLogs());
        auto current_metadata_handle = table->IStorage::getInMemoryMetadataPtr(nullptr, true);
        StorageMetadataPtr current_metadata = current_metadata_handle;
        if (!current_metadata || current_metadata != exact_bound_metadata)
            throw Exception(
                ErrorCodes::ABORTED,
                "Atomic user-defined type table metadata changed before "
                "durable admission");

        auto [table_it, table_inserted] = tables.emplace(table_name, table);
        if (!table_inserted)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists", table_id.getFullTableName());
        try
        {
            const auto [path_it, path_inserted] = table_name_to_path.emplace(table_name, std::move(table_data_path));
            static_cast<void>(path_it);
            if (!path_inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table path already exists");
        }
        catch (...)
        {
            tables.erase(table_it);
            throw;
        }

        UDT::DurablyCommittedDependentObjectAdmission durable = [&]
        {
            try
            {
                return DependentObjectAdmissionCoordinator::commitPreparedTableCreateDurably(
                    storage, mutation_guard, std::move(prepared_commit));
            }
            catch (const DatabaseSchemaMutationIndeterminateDurabilityError & error)
            {
                const auto original = std::current_exception();
                bool rolled_back = false;
                std::optional<DurablyCommittedDependentObjectAdmission> recovered;
                try
                {
                    if (error.transaction_id != prepared_commit.getTransactionID()
                        || storage.getRecoveryRequiredTransactionID() != error.transaction_id)
                    {
                        throw DatabaseSchemaMutationReplayConflictError("Atomic mapped-table recovery latch changed identity");
                    }

                    const auto transaction_ids = storage.listDurableTransactionIDs();
                    if (!std::binary_search(transaction_ids.begin(), transaction_ids.end(), error.transaction_id))
                    {
                        discardUnpreparedDatabaseSchemaMutationStaging(storage, mutation_guard, error.transaction_id);
                        rolled_back = true;
                    }
                    else
                    {
                        auto image = storage.loadTransactionForRecovery(error.transaction_id);
                        const auto & transition = prepared_commit.getRecoveryTransition();
                        const auto expected_bytes = transition.getStagedArtifactBytes();
                        if (image.prepare != transition.getPrepare() || image.staged_artifact_bytes.size() != expected_bytes.size()
                            || !std::equal(
                                image.staged_artifact_bytes.begin(),
                                image.staged_artifact_bytes.end(),
                                expected_bytes.begin(),
                                expected_bytes.end()))
                        {
                            throw DatabaseSchemaMutationReplayConflictError(
                                "Atomic mapped-table recovery image differs from the retained admission transition");
                        }
                        auto recovered_commit = DependentObjectAdmissionCoordinator::recoverPreparedTableCreateDurably(
                            storage, mutation_guard, std::move(prepared_commit), image.commit);
                        rolled_back = !recovered_commit;
                        if (recovered_commit)
                            recovered.emplace(std::move(*recovered_commit));
                    }
                }
                catch (...)
                {
                    std::terminate();
                }

                if (rolled_back)
                    std::rethrow_exception(original);
                return std::move(*recovered);
            }
            catch (...)
            {
                table_name_to_path.erase(table_name);
                tables.erase(table_it);
                tables_lock.unlock();
                throw;
            }
        }();
        durable_confirmed = true;

        auto committed = DependentObjectAdmissionCoordinator::publishDurablyCommittedTableCreate(authority, std::move(durable));
        static_cast<void>(committed);

        table->is_detached = false;
        DatabaseCatalog::instance().publishReservedUUIDMappingNoThrow(table_id.uuid, std::move(database), table);
        if (table->storesDataOnDisk())
            tryCreateSymlink(table);
        if (!table->isSystemStorage() && !DatabaseCatalog::isPredefinedDatabase(database_name))
        {
            for (const auto metric : attached_metrics)
                CurrentMetrics::add(metric);
        }
        tables_lock.unlock();

        guard.impl->schema_lock.unlock();
        publication_complete = true;
        try
        {
            static_cast<void>(authority.scanRetired());
        }
        catch (...)
        {
        }
    }
    catch (...)
    {
        if (durable_confirmed)
            std::terminate();
        throw;
    }
}

void DatabaseAtomic::createStoredObjectWithUDTBindings(
    TableCreateGuard guard,
    ContextPtr query_context,
    const ASTPtr & physical_create_query,
    const StoragePtr & object_storage,
    UDT::PreparedStoredObjectTypeBindingHandoff bindings)
{
    using UDT::AtomicStoredObjectUDTMetadataValidator;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::BoundObjectPhysicalSchema;
    using UDT::DatabaseSchemaMutationIndeterminateDurabilityError;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::discardUnpreparedDatabaseSchemaMutationStaging;
    using UDT::DurablyCommittedStoredObjectUDTPublication;
    using UDT::encodePersistedTypeReferences;
    using UDT::PersistedTypeReferences;
    using UDT::SidecarExpectationRecord;
    using UDT::StoredObjectKind;
    using UDT::StoredObjectUDTPublicationAdmissionProof;
    using UDT::StoredObjectUDTPublicationCoordinator;

    bool startup_attempted = false;
    bool durable_confirmed = false;
    bool publication_complete = false;
    SCOPE_EXIT({
        if (object_storage && !publication_complete && !durable_confirmed)
        {
            if (guard.impl && guard.impl->schema_lock.owns_lock())
                guard.impl->schema_lock.unlock();
            if (startup_attempted)
            {
                try
                {
                    object_storage->shutdown();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Failed to stop an unpublished Atomic mapped stored object");
                }
            }
            try
            {
                /// View/MV/Dictionary drop hooks also unwind constructor-side
                /// dependencies and inner-object ownership when the wrapper
                /// itself stores no data. Generic fresh CREATE invokes drop on
                /// every failed storage for the same reason.
                object_storage->drop();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to clean up an unpublished Atomic mapped stored object");
            }
        }
    });

    if (!guard.impl || guard.impl->owner != this || !guard.impl->schema_lock.owns_lock()
        || guard.impl->schema_lock.mutex() != &udt_schema_mutation_mutex || !guard.impl->authority || !guard.impl->storage
        || !guard.impl->planning_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored-object CREATE guard is invalid");
    if (!query_context || !physical_create_query || !object_storage || !bindings.hasAppliedPhysicalTypeASTs())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored-object CREATE has an incomplete input");

    const auto * create = physical_create_query->as<ASTCreateQuery>();
    const auto storage_id = object_storage->getStorageID();
    const auto object_kind = bindings.getObjectKind();
    const bool is_view = object_kind == StoredObjectKind::View && create && create->is_ordinary_view && object_storage->getName() == "View"
        && object_storage->as<StorageView>();
    const auto * materialized_view = object_storage->as<StorageMaterializedView>();
    const bool is_materialized_view = object_kind == StoredObjectKind::MaterializedView && create && create->is_materialized_view
        && object_storage->getName() == "MaterializedView" && materialized_view;
    const bool is_dictionary
        = object_kind == StoredObjectKind::Dictionary && create && create->is_dictionary && object_storage->isDictionary();
    const bool authorized_materialized_view_surface = is_materialized_view && !create->refresh_strategy
        && materialized_view->hasInnerTable() == create->is_materialized_view_with_inner_table();
    if (!create || (!is_view && !is_materialized_view && !is_dictionary) || create->isTemporary() || create->attach || create->if_not_exists
        || create->replace_view || create->replace_table || create->create_or_replace || !create->cluster.empty()
        || (is_materialized_view && !authorized_materialized_view_surface)
        || (!is_materialized_view && (create->is_populate || create->targets)) || create->refresh_strategy)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mapped stored-object CREATE surface is not authorized");
    if (create->getDatabase() != getDatabaseName() || create->getTable() != storage_id.table_name || create->uuid != storage_id.uuid
        || storage_id.database_name != getDatabaseName() || storage_id.uuid == UUIDHelpers::Nil
        || bindings.getObject().database_uuid != db_uuid || bindings.getObject().object_uuid != storage_id.uuid)
        throw Exception(ErrorCodes::ABORTED, "Atomic stored-object identity changed before admission");

    auto initial_metadata_handle = object_storage->getInMemoryMetadataPtr(query_context, false);
    if (!initial_metadata_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored object has no runtime metadata snapshot");
    StorageMetadataPtr initial_metadata = initial_metadata_handle;
    if (is_materialized_view && materialized_view->hasInnerTable())
    {
        const auto inner_table = materialized_view->getTargetTable();
        const auto inner_metadata = inner_table ? inner_table->getInMemoryMetadataPtr(query_context, false) : nullptr;
        if (!inner_table || !inner_metadata)
            throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView has no complete physical inner table");
        inner_metadata->validateBoundUDTReferences();
        if (inner_metadata->getBoundUDTReferences() || inner_metadata->getPendingUDTColumnAlter()
            || inner_metadata->getColumns().getAllPhysical() != initial_metadata->getColumns().getAllPhysical())
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Mapped MaterializedView inner table must remain physical-only and exactly match its outer runtime schema");
        }
    }

    const PersistedTypeReferences * final_persisted_references = nullptr;
    if (const auto * prepared = bindings.tryGetViewBindings())
    {
        if (!prepared->persisted_references || !prepared->bound_physical_schema || !prepared->sidecar_expectation
            || prepared->persisted_references->object != bindings.getObject() || prepared->persisted_references->object_schema_revision != 1
            || prepared->bound_physical_schema->object != bindings.getObject()
            || prepared->bound_physical_schema->object_schema_revision != 1
            || prepared->physical_schema_fingerprint != prepared->persisted_references->physical_schema_fingerprint
            || prepared->bound_physical_schema->physical_schema_fingerprint != prepared->persisted_references->physical_schema_fingerprint
            || prepared->sidecar_expectation->object != bindings.getObject() || prepared->sidecar_expectation->object_schema_revision != 1
            || prepared->sidecar_expectation->physical_schema_fingerprint != prepared->persisted_references->physical_schema_fingerprint
            || prepared->physical_outputs != initial_metadata->getColumns().getAllPhysical())
        {
            throw Exception(ErrorCodes::ABORTED, "Atomic View runtime outputs differ from their prepared declaration bindings");
        }
        final_persisted_references = std::addressof(*prepared->persisted_references);
    }
    else if (const auto * dictionary_bindings = bindings.tryGetDictionaryBindings())
    {
        if (!dictionary_bindings->persisted_references || !dictionary_bindings->bound_physical_schema
            || !dictionary_bindings->sidecar_expectation || dictionary_bindings->persisted_references->object != bindings.getObject()
            || dictionary_bindings->persisted_references->object_schema_revision != 1
            || dictionary_bindings->bound_physical_schema->object != bindings.getObject()
            || dictionary_bindings->bound_physical_schema->object_schema_revision != 1
            || dictionary_bindings->physical_schema_fingerprint != dictionary_bindings->persisted_references->physical_schema_fingerprint
            || dictionary_bindings->bound_physical_schema->physical_schema_fingerprint
                != dictionary_bindings->persisted_references->physical_schema_fingerprint
            || dictionary_bindings->sidecar_expectation->object != bindings.getObject()
            || dictionary_bindings->sidecar_expectation->object_schema_revision != 1
            || dictionary_bindings->sidecar_expectation->physical_schema_fingerprint
                != dictionary_bindings->persisted_references->physical_schema_fingerprint
            || dictionary_bindings->physical_attributes != initial_metadata->getColumns().getAllPhysical())
        {
            throw Exception(ErrorCodes::ABORTED, "Atomic Dictionary runtime attributes differ from their prepared declaration bindings");
        }
        final_persisted_references = std::addressof(*dictionary_bindings->persisted_references);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored-object CREATE lost its prepared exact bindings");

    /// Move the existing mandatory canonical validation before the mutation
    /// guard. The final ACL decision below uses this same descriptor dictionary
    /// that will be persisted by the CREATE.
    String canonical_sidecar_bytes = encodePersistedTypeReferences(*final_persisted_references);

    auto & authority = *guard.impl->authority;
    auto & storage = *guard.impl->storage;
    StoredObjectUDTPublicationAdmissionProof admission_proof = [&]
    {
        if (const auto * view_bindings = bindings.tryGetViewBindings())
            return udt_lifecycle_adapter->authorizeStoredObjectCreate(
                guard.impl->planning_root.get(),
                storage,
                object_kind,
                *create,
                *view_bindings,
                bindings.usesSelectedOutputClassification());
        if (const auto * dictionary_bindings = bindings.tryGetDictionaryBindings())
            return udt_lifecycle_adapter->authorizeStoredObjectCreate(
                guard.impl->planning_root.get(), storage, *create, *dictionary_bindings);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored-object CREATE lost its prepared exact bindings");
    }();
    checkUsageAccessForFinalPersistedUDTDescriptors(query_context, *final_persisted_references);
    auto object_dependencies = collectMappedObjectDependencies(
        *this,
        guard.impl->planning_root.get(),
        physical_create_query,
        storage_id.getQualifiedName(),
        final_persisted_references->object,
        query_context);

    storage.maintainCheckpointBeforeMutation(guard.impl->planning_root.get());
    auto mutation_guard = storage.issueMutationGuard();
    const UInt64 durable_predecessor = mutation_guard.getDurablePredecessorTransactionID();
    if (durable_predecessor == std::numeric_limits<UInt64>::max())
        throw DatabaseSchemaMutationReplayConflictError("Atomic stored-object transaction ID domain is exhausted");

    BoundObjectPhysicalSchema physical_schema;
    SidecarExpectationRecord expectation;
    if (bindings.tryGetViewBindings())
    {
        auto prepared = std::move(bindings).releaseViewBindings();
        if (!prepared.persisted_references || !prepared.bound_physical_schema || !prepared.sidecar_expectation
            || prepared.physical_outputs != initial_metadata->getColumns().getAllPhysical())
            throw Exception(ErrorCodes::ABORTED, "Atomic View runtime outputs differ from their prepared declaration bindings");
        physical_schema = std::move(*prepared.bound_physical_schema);
        expectation = *prepared.sidecar_expectation;
    }
    else
    {
        auto prepared = std::move(bindings).releaseDictionaryBindings();
        if (!prepared.persisted_references || !prepared.bound_physical_schema || !prepared.sidecar_expectation
            || prepared.physical_attributes != initial_metadata->getColumns().getAllPhysical())
            throw Exception(ErrorCodes::ABORTED, "Atomic Dictionary runtime attributes differ from their prepared declaration bindings");
        physical_schema = std::move(*prepared.bound_physical_schema);
        expectation = *prepared.sidecar_expectation;
    }

    AtomicStoredObjectUDTMetadataValidator metadata_validator(db_uuid, physical_create_query, 1);
    auto prepared_commit = StoredObjectUDTPublicationCoordinator::prepareCreateCommit(
        std::move(guard.impl->planning_root),
        authority,
        storage,
        mutation_guard,
        durable_predecessor + 1,
        std::move(admission_proof),
        std::move(physical_schema),
        getObjectDefinitionFromCreateQuery(physical_create_query),
        std::move(canonical_sidecar_bytes),
        expectation,
        std::move(object_dependencies),
        metadata_validator);
    assertUDTNewDefinitionClosureOperationAllowed(*prepared_commit.getBoundUDTReferences(), AuthorityQuarantineOperationKind::DDL);

    StorageInMemoryMetadata bound_metadata(*initial_metadata);
    bound_metadata.setColumnsAndBoundStoredObjectUDTReferences(
        initial_metadata->getColumns(), prepared_commit.getBoundUDTReferences(), prepared_commit.getExpectationRecord());
    bound_metadata.setBoundUDTVerificationStamp(prepared_commit.getVerificationStamp());
    object_storage->setInMemoryMetadata(bound_metadata);
    auto bound_metadata_handle = object_storage->IStorage::getInMemoryMetadataPtr(nullptr, true);
    StorageMetadataPtr exact_bound_metadata = bound_metadata_handle;
    if (!exact_bound_metadata)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic stored object lost its bound runtime metadata snapshot");
    exact_bound_metadata->validateBoundUDTReferences();

    try
    {
        startup_attempted = true;
        object_storage->startup();

        const String object_name = storage_id.table_name;
        String object_data_path = getTableDataPath(*create);
        auto database = shared_from_this();
        auto attached_metrics = getAttachedCountersForStorage(object_storage);
        createDirectories();

        UniqueLock tables_lock(mutex);
        if (create->getDatabase() != database_name || storage_id.database_name != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed before mapped stored-object publication");
        if (tables.contains(object_name) || table_name_to_path.contains(object_name) || snapshot_detached_tables.contains(object_name))
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS, "Stored object {} already exists or is detached", storage_id.getFullTableName());
        assertDetachedTableNotInUse(storage_id.uuid);
        const auto reserved_mapping = DatabaseCatalog::instance().tryGetByUUID(storage_id.uuid);
        if (!DatabaseCatalog::instance().hasUUIDMapping(storage_id.uuid) || reserved_mapping.first || reserved_mapping.second)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "UUID reservation for {} is no longer empty", storage_id.getNameForLogs());
        auto current_metadata_handle = object_storage->IStorage::getInMemoryMetadataPtr(nullptr, true);
        StorageMetadataPtr current_metadata = current_metadata_handle;
        if (!current_metadata || current_metadata != exact_bound_metadata)
            throw Exception(ErrorCodes::ABORTED, "Atomic stored-object runtime metadata changed before durable admission");

        auto [table_it, object_inserted] = tables.emplace(object_name, object_storage);
        if (!object_inserted)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Stored object {} already exists", storage_id.getFullTableName());
        try
        {
            const auto [path_it, path_inserted] = table_name_to_path.emplace(object_name, std::move(object_data_path));
            static_cast<void>(path_it);
            if (!path_inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped stored-object path already exists");
        }
        catch (...)
        {
            tables.erase(table_it);
            throw;
        }

        DurablyCommittedStoredObjectUDTPublication durable = [&]
        {
            try
            {
                return StoredObjectUDTPublicationCoordinator::commitPreparedCreateDurably(
                    storage, mutation_guard, std::move(prepared_commit));
            }
            catch (const DatabaseSchemaMutationIndeterminateDurabilityError & error)
            {
                const auto original = std::current_exception();
                bool rolled_back = false;
                std::optional<DurablyCommittedStoredObjectUDTPublication> recovered;
                try
                {
                    if (error.transaction_id != prepared_commit.getTransactionID()
                        || storage.getRecoveryRequiredTransactionID() != error.transaction_id)
                        throw DatabaseSchemaMutationReplayConflictError("Atomic stored-object recovery latch changed identity");
                    const auto transaction_ids = storage.listDurableTransactionIDs();
                    if (!std::binary_search(transaction_ids.begin(), transaction_ids.end(), error.transaction_id))
                    {
                        discardUnpreparedDatabaseSchemaMutationStaging(storage, mutation_guard, error.transaction_id);
                        rolled_back = true;
                    }
                    else
                    {
                        auto image = storage.loadTransactionForRecovery(error.transaction_id);
                        const auto & transition = prepared_commit.getRecoveryTransition();
                        const auto expected_bytes = transition.getStagedArtifactBytes();
                        if (image.prepare != transition.getPrepare() || image.staged_artifact_bytes.size() != expected_bytes.size()
                            || !std::equal(
                                image.staged_artifact_bytes.begin(),
                                image.staged_artifact_bytes.end(),
                                expected_bytes.begin(),
                                expected_bytes.end()))
                            throw DatabaseSchemaMutationReplayConflictError(
                                "Atomic stored-object recovery image differs from its retained transition");
                        auto recovered_commit = StoredObjectUDTPublicationCoordinator::recoverPreparedCreateDurably(
                            storage, mutation_guard, std::move(prepared_commit), image.commit);
                        rolled_back = !recovered_commit;
                        if (recovered_commit)
                            recovered.emplace(std::move(*recovered_commit));
                    }
                }
                catch (...)
                {
                    std::terminate();
                }
                if (rolled_back)
                    std::rethrow_exception(original);
                return std::move(*recovered);
            }
            catch (...)
            {
                table_name_to_path.erase(object_name);
                tables.erase(table_it);
                tables_lock.unlock();
                throw;
            }
        }();
        durable_confirmed = true;
        static_cast<void>(StoredObjectUDTPublicationCoordinator::publishDurablyCommittedCreate(authority, std::move(durable)));

        object_storage->is_detached = false;
        DatabaseCatalog::instance().publishReservedUUIDMappingNoThrow(storage_id.uuid, std::move(database), object_storage);
        if (object_storage->storesDataOnDisk())
            tryCreateSymlink(object_storage);
        if (!object_storage->isSystemStorage() && !DatabaseCatalog::isPredefinedDatabase(database_name))
            for (const auto metric : attached_metrics)
                CurrentMetrics::add(metric);
        tables_lock.unlock();

        guard.impl->schema_lock.unlock();
        publication_complete = true;
        try
        {
            static_cast<void>(authority.scanRetired());
        }
        catch (...)
        {
        }
    }
    catch (...)
    {
        if (durable_confirmed)
            std::terminate();
        throw;
    }
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

StoredObjectMetadataLoadDecision
DatabaseAtomic::decideStoredObjectMetadataLoadBeforeParsing(std::string_view canonical_file_object_name) const
{
    std::shared_ptr<const UDT::AtomicAuthorityStartupStatusSnapshot> degraded_status;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        degraded_status = udt_degraded_startup_status;
    }
    if (!degraded_status)
        return {};
    if (degraded_status->hasUnknownDependentObjectScope())
        return {.action = StoredObjectMetadataLoadAction::SkipUnavailable};
    if (const auto * object = degraded_status->findExpectedDependentObject(canonical_file_object_name))
    {
        return {
            .action = StoredObjectMetadataLoadAction::SkipUnavailable,
            .reserved_uuid = object->object_uuid,
        };
    }
    return {};
}

StoredObjectMetadataLoadDecision
DatabaseAtomic::decideStoredObjectMetadataLoadAfterParsing(std::string_view canonical_file_object_name, const ASTCreateQuery & query) const
{
    std::shared_ptr<const UDT::AtomicAuthorityStartupStatusSnapshot> degraded_status;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        degraded_status = udt_degraded_startup_status;
    }
    if (!degraded_status)
        return {};
    if (degraded_status->hasUnknownDependentObjectScope())
        return {.action = StoredObjectMetadataLoadAction::SkipUnavailable};

    const UDT::AtomicAuthorityStartupDependentObjectIdentity * unavailable = nullptr;
    if (const auto * by_file_name = degraded_status->findExpectedDependentObject(canonical_file_object_name))
        unavailable = by_file_name;
    else if (const auto * by_parsed_name = degraded_status->findExpectedDependentObject(query.getTable()))
        unavailable = by_parsed_name;
    else if (const auto * by_uuid = degraded_status->findExpectedDependentObject(query.uuid))
        unavailable = by_uuid;
    if (!unavailable)
        return {};
    return {
        .action = StoredObjectMetadataLoadAction::SkipUnavailable,
        .reserved_uuid = unavailable->object_uuid,
    };
}

bool DatabaseAtomic::forceEagerTableLoadAtStartup(const ASTCreateQuery & query) const
{
    std::lock_guard lock(udt_authority_mutex);
    return udt_table_startup_state && udt_table_startup_state->findExactEntry(query.uuid, query.getTable());
}

void DatabaseAtomic::validateTableMetadataForLoading(const ASTCreateQuery & query, bool permanently_detached) const
{
    bool pending_mapped_object = false;
    {
        std::lock_guard lock(udt_authority_mutex);
        pending_mapped_object = udt_table_startup_state && udt_table_startup_state->findExactEntry(query.uuid, query.getTable());
    }
    if (pending_mapped_object)
    {
        if (permanently_detached)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Mapped user-defined type object {}.{} cannot remain "
                "permanently detached during authority startup",
                getDatabaseName(),
                query.getTable());
        }
        return;
    }
}

void DatabaseAtomic::validateTableMetadataRewriteBeforeLoading(const ASTCreateQuery & query) const
{
    bool pending_mapped_object = false;
    {
        std::lock_guard lock(udt_authority_mutex);
        pending_mapped_object = udt_table_startup_state && udt_table_startup_state->findExactEntry(query.uuid, query.getTable());
    }
    if (pending_mapped_object)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Automatic engine conversion is not supported for mapped "
            "user-defined type object {}.{} during authority startup",
            getDatabaseName(),
            query.getTable());
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

void DatabaseAtomic::attachTable(
    ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::attachTable");
    chassert(relative_table_path != data_path && !relative_table_path.empty());
    cleanupDetachedTablesBeforeAttachWithoutSchemaGuard();

    bool pending_startup = false;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        pending_startup = static_cast<bool>(udt_table_startup_state);
    }
    if (!pending_startup)
    {
        {
            std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
            const auto table_id = table->getStorageID();
            const auto current_database_name = getDatabaseName();
            assertNotLiveMappedMaterializedViewInnerTable(table, "ATTACH");
            if (hasDatabaseOwnedTableExpectationForCrossDatabaseMove(table_id.uuid))
            {
                {
                    std::lock_guard tables_lock(mutex);
                    const auto detached = snapshot_detached_tables.find(name);
                    if (detached == snapshot_detached_tables.end() || detached->second.uuid != table_id.uuid
                        || detached->second.database != current_database_name || detached->second.table != name
                        || detached->second.metadata_path != getObjectMetadataPath(name) || detached->second.is_permanently)
                    {
                        throw Exception(
                            ErrorCodes::ABORTED,
                            "Mapped object {}.{} is not the exact temporarily detached object requested by ATTACH",
                            current_database_name,
                            name);
                    }
                }

                auto authority_image = loadExactMappedObjectAuthorityImage(table_id.uuid, name);
                if (table_id.database_name != current_database_name || table_id.table_name != name
                    || mappedSchemaObjectKindForStorage(*table) != authority_image.image.expectation.object.kind)
                {
                    throw Exception(
                        ErrorCodes::ABORTED,
                        "Mapped object {}.{} ATTACH storage identity differs from its durable installation",
                        current_database_name,
                        name);
                }

                const auto & image = authority_image.image;
                if (image.expectation.object.kind == UDT::SchemaObjectKind::Table)
                {
                    UDT::AtomicTableMetadataValidator validator(db_uuid, authority_image.trusted_create_query, table);
                    validator.validateAndBindStartupMetadata(
                        authority_image.root.get(), image.expectation, image.canonical_metadata_bytes, image.canonical_sidecar_bytes);
                }
                else
                {
                    UDT::AtomicStoredObjectUDTMetadataValidator validator(
                        db_uuid, authority_image.trusted_create_query, image.expectation.object_schema_revision);
                    validator.validateAndBindStartupMetadata(
                        authority_image.root.get(),
                        image.expectation,
                        image.canonical_metadata_bytes,
                        image.canonical_sidecar_bytes,
                        table);
                }

                /// ATTACH is a new operation against the retained mapped
                /// identity. Bind/stamp first, then consult the active
                /// quarantine image before making the storage live again.
                auto rebound_metadata = table->getInMemoryMetadataPtr(nullptr, false);
                assertUDTNewStorageOperationAllowed(rebound_metadata, UDT::AuthorityQuarantineOperationKind::Attach);
            }
            else
                assertUDTTableAllowsOrdinaryMetadataMutation(table, getContext(), "ATTACH");
            attachTableWithoutUDTGuard(name, table, relative_table_path);
        }
        return;
    }

    {
        std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
        /// getDatabaseName() takes the tables mutex. Resolve it before the
        /// authority mutex to preserve the catalog -> authority lock order.
        const auto current_database_name = getDatabaseName();
        const auto table_id = table->getStorageID();
        const UDT::AtomicTableStartupState::Entry * pending_entry = nullptr;
        std::optional<UDT::AtomicAuthority::RootSnapshot> recovered_snapshot;
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (udt_authority_shutdown)
                throw Exception(
                    ErrorCodes::ABORTED,
                    "Cannot attach an Atomic table while its database is "
                    "shutting down");
            if (udt_table_startup_state)
            {
                pending_entry = udt_table_startup_state->findExactEntry(table_id.uuid, name);
                if (pending_entry)
                {
                    if (pending_entry->attached || !udt_authority)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup entry is not bindable");
                    recovered_snapshot.emplace(udt_authority->acquireCurrentRoot());
                }
            }
        }

        const auto transition_private_authority_to_degraded = [&](std::exception_ptr startup_failure)
        {
            /// The snapshot owns a hazard slot in the private authority. Drop
            /// it before draining that authority or shutdown would wait for
            /// this same thread forever.
            recovered_snapshot.reset();
            transitionPendingUDTAuthorityToDegraded(std::move(schema_mutation_lock));
            std::rethrow_exception(startup_failure);
        };

        if (pending_entry)
        {
            try
            {
                if (!*recovered_snapshot)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup has no recovered authority root");
                const auto & image = pending_entry->image;
                auto trusted_create_query
                    = parseMappedTableStartupMetadata(image.canonical_metadata_bytes, current_database_name, image.object_name);
                if (image.expectation.object.kind == UDT::SchemaObjectKind::Table)
                {
                    UDT::AtomicTableMetadataValidator validator(db_uuid, trusted_create_query, table);
                    validator.validateAndBindStartupMetadata(
                        recovered_snapshot->get(), image.expectation, image.canonical_metadata_bytes, image.canonical_sidecar_bytes);
                }
                else
                {
                    UDT::AtomicStoredObjectUDTMetadataValidator validator(
                        db_uuid, trusted_create_query, image.expectation.object_schema_revision);
                    validator.validateAndBindStartupMetadata(
                        recovered_snapshot->get(), image.expectation, image.canonical_metadata_bytes, image.canonical_sidecar_bytes, table);
                }
            }
            catch (const std::bad_alloc &)
            {
                throw;
            }
            catch (const Exception & exception)
            {
                if (UDT::isUDTResourceOrControlExceptionCode(exception.code()))
                    throw;
                transition_private_authority_to_degraded(std::current_exception());
            }
            catch (...)
            {
                transition_private_authority_to_degraded(std::current_exception());
            }
        }
        else
        {
            assertUDTTableAllowsOrdinaryMetadataMutation(table, getContext(), "ATTACH");
        }

        attachTableWithoutUDTGuard(name, table, relative_table_path);
        if (pending_entry)
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (!udt_table_startup_state)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup state disappeared during attach");
            auto * current_entry = udt_table_startup_state->findExactEntry(table_id.uuid, name);
            if (!current_entry || current_entry != pending_entry)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic mapped table startup entry changed during attach");
            udt_table_startup_state->markAttached(*current_entry, table, current_database_name);
        }
    }
}

void DatabaseAtomic::attachTableWithoutUDTGuard(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
    const auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

void DatabaseAtomic::cleanupDetachedTablesBeforeAttachWithoutSchemaGuard()
{
    /// IStorage destruction may re-enter database-owned resources. Select
    /// stale entries under the tables mutex, but release both database locks
    /// before dropping their final StoragePtr references.
    DetachedTables not_in_use;
    {
        std::lock_guard lock(mutex);
        not_in_use = cleanupDetachedTables();
    }
    if (!not_in_use.empty())
    {
        not_in_use.clear();
        LOG_DEBUG(log, "Finished removing not used detached tables before attach");
    }
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    ensurePopulated();
    waitDatabaseStarted();
    std::pair<StoragePtr, DetachedTables> result;
    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        const auto table = getTable(name, getContext());
        assertNotLiveMappedMaterializedViewInnerTable(table, "DETACH");
        assertUDTTableAllowsOrdinaryMetadataMutation(table, getContext(), "DETACH");
        result = detachTableWithoutUDTGuard(name, table, true);
    }

    if (!result.second.empty())
    {
        result.second.clear();
        LOG_DEBUG(log, "Finished removing not used detached tables");
    }

    return result.first;
}

std::pair<StoragePtr, DatabaseAtomic::DetachedTables>
DatabaseAtomic::detachTableWithoutUDTGuard(const String & name, const StoragePtr & expected_table, bool cleanup_detached_tables)
{
    std::pair<StoragePtr, DetachedTables> result;
    {
        std::lock_guard lock(mutex);
        result.first = getTableUnlocked(name);
        if (result.first != expected_table)
            throw Exception(ErrorCodes::ABORTED, "Atomic table changed before detach");
        result.first = DatabaseOrdinary::detachTableUnlocked(name);
        table_name_to_path.erase(name);
        detached_tables.emplace(result.first->getStorageID().uuid, result.first);
        if (cleanup_detached_tables)
            result.second = cleanupDetachedTables();
    }
    return result;
}

void DatabaseAtomic::alterTable(
    ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, bool validate_new_create_query)
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
    {
        DatabaseOrdinary::alterTable(local_context, table_id, metadata, validate_new_create_query);
        return;
    }

    waitDatabaseStarted();
    const auto pending = metadata.getPendingUDTColumnAlter();
    if (pending && pending->getDesiredReferences())
    {
        /// For an initially physical table this performs the standalone,
        /// content-neutral definition-only -> dependent-object-capable transition only after ordinary
        /// ALTER application proved that a logical mapping survives the whole
        /// command batch. Existing mapped tables take the idempotent branch.
        ensureUDTDependentObjectCapabilities();
    }

    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        const auto table = getTable(table_id.table_name, local_context);
        assertNotLiveMappedMaterializedViewInnerTable(table, "ALTER");
        const bool mapped_table = hasDatabaseOwnedUDTTableBinding(table, local_context);
        if (mapped_table || metadata.getBoundUDTReferences() || pending)
        {
            if (mapped_table && !pending)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped Atomic object ALTER has no storage-owned publication handoff");
            if (mapped_table)
            {
                const auto metadata_snapshot = table->getInMemoryMetadataPtr(local_context, false);
                assertUDTNewStorageOperationAllowed(metadata_snapshot, UDT::AuthorityQuarantineOperationKind::DDL);
            }
            static_cast<void>(alterUDTStoredObject(local_context, table_id, table, metadata, validate_new_create_query));
        }
        else
            DatabaseOrdinary::alterTable(local_context, table_id, metadata, validate_new_create_query);
    }
    reclaimRetiredUDTRootsNoThrow();
}

bool DatabaseAtomic::rollbackUDTTableAlter(
    ContextPtr local_context,
    const StorageID & table_id,
    StorageInMemoryMetadata & metadata_to_restore,
    const StorageInMemoryMetadata & committed_metadata)
{
    const auto committed_pending = committed_metadata.getPendingUDTColumnAlter();
    bool committed_mapped = false;
    if (committed_pending)
    {
        const auto completed = committed_pending->getCompletedPublication();
        if (!completed)
            throw Exception(ErrorCodes::ABORTED, "Mapped Atomic table rollback has no completed committed publication");
        committed_mapped = static_cast<bool>(completed->bound_references);
    }
    else
    {
        committed_metadata.validateBoundUDTReferences();
        committed_mapped = static_cast<bool>(committed_metadata.getBoundUDTReferences());
    }

    metadata_to_restore.validateBoundUDTReferences();
    if (metadata_to_restore.getPendingUDTColumnAlter())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped Atomic table rollback target is itself unpublished");
    const bool restore_mapped = static_cast<bool>(metadata_to_restore.getBoundUDTReferences());
    if (!restore_mapped && !committed_mapped)
        return false;

    waitDatabaseStarted();
    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        const auto table = getTable(table_id.table_name, local_context);
        const bool durable_mapped = hasDatabaseOwnedTableExpectationForCrossDatabaseMove(table->getStorageID().uuid);
        if (durable_mapped != committed_mapped)
            throw Exception(ErrorCodes::ABORTED, "Mapped Atomic table rollback boundary no longer matches the committed authority image");

        auto restored_columns = metadata_to_restore.getColumns();
        auto restored = alterUDTStoredObject(local_context, table_id, table, metadata_to_restore, false, true, &committed_metadata);
        try
        {
            if (restored.bound_references)
            {
                if (!restored.expectation)
                    std::terminate();
                metadata_to_restore.setColumnsAndBoundUDTReferences(
                    std::move(restored_columns), std::move(restored.bound_references), *restored.expectation);
                metadata_to_restore.setBoundUDTVerificationStamp(std::move(restored.verification_stamp));
            }
            else
            {
                if (restored.expectation)
                    std::terminate();
                metadata_to_restore.setColumns(std::move(restored_columns));
            }
        }
        catch (...)
        {
            /// The reverse transaction is already durable. Returning an exception
            /// would make the engine converge to the superseded image.
            std::terminate();
        }
    }
    reclaimRetiredUDTRootsNoThrow();
    return true;
}

UDT::CompletedTableColumnTypeAlterPublication DatabaseAtomic::alterUDTStoredObject(
    ContextPtr local_context,
    const StorageID & table_id,
    const StoragePtr & table,
    const StorageInMemoryMetadata & metadata,
    bool validate_new_create_query,
    bool trusted_boundary_rollback,
    const StorageInMemoryMetadata * expected_current_metadata)
{
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationDependentObjectImage;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::AuthorityInventoryRecordKind;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::DependentObjectMutationCoordinator;
    using UDT::DependentObjectMutationKind;
    using UDT::DependentObjectMutationPlanner;
    using UDT::DependentObjectMutationRequest;
    using UDT::PersistedTypeReferences;
    using UDT::rebaseBoundStoredObjectTypeReferences;
    using UDT::rebaseBoundTableColumnTypeReferences;
    using UDT::SchemaObjectID;
    using UDT::SchemaObjectKind;

    const auto actual_table_id = table->getStorageID();
    if (actual_table_id.database_name != getDatabaseName() || actual_table_id.table_name != table_id.table_name
        || actual_table_id.uuid != table_id.uuid || actual_table_id.uuid == UUIDHelpers::Nil)
    {
        throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Mapped Atomic object identity changed before ALTER");
    }
    const auto object_kind = mappedSchemaObjectKindForStorage(*table);
    const String engine_name = table->getName();
    const bool supported_table = object_kind == SchemaObjectKind::Table
        && ((engine_name == "Memory" && !table->storesDataOnDisk())
            || (table->isMergeTree() && table->storesDataOnDisk() && !table->isSharedStorage() && !engine_name.starts_with("Replicated")
                && !engine_name.starts_with("Shared")));
    const auto * ordinary_view = table->as<StorageView>();
    const auto * materialized_view = table->as<StorageMaterializedView>();
    const bool supported_view = object_kind == SchemaObjectKind::View
        && ((engine_name == "View" && ordinary_view)
            || (engine_name == "MaterializedView" && materialized_view && !materialized_view->isRefreshable()));
    const bool supported_dictionary
        = object_kind == SchemaObjectKind::Dictionary && engine_name == "Dictionary" && table->as<StorageDictionary>();
    if (!supported_table && !supported_view && !supported_dictionary)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Mapped Atomic ALTER does not support storage engine {} for object kind {}",
            engine_name,
            static_cast<unsigned>(object_kind));
    }
    if (supported_view && materialized_view && materialized_view->hasInnerTable())
    {
        const auto inner_table = materialized_view->getTargetTable();
        const auto inner_metadata = inner_table ? inner_table->getInMemoryMetadataPtr(local_context, false) : nullptr;
        if (!inner_table || !inner_metadata)
            throw Exception(ErrorCodes::ABORTED, "Mapped MaterializedView lost its physical inner table before ALTER");
        inner_metadata->validateBoundUDTReferences();
        if (inner_metadata->getBoundUDTReferences() || inner_metadata->getPendingUDTColumnAlter()
            || inner_metadata->getColumns().getAllPhysical() != metadata.getColumns().getAllPhysical())
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "Mapped MaterializedView ALTER cannot change or assign authority identity to its physical-only inner table");
        }
    }

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> planning_root;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown || udt_table_startup_state || !udt_authority || !udt_mutation_storage
            || active_udt_authority.load(std::memory_order_acquire) != udt_authority.get())
            throw Exception(ErrorCodes::ABORTED, "Mapped object ALTER requires one active, fully recovered Atomic authority");
        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        planning_root.emplace(authority->acquireCurrentRoot());
    }
    if (!*planning_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped object ALTER has no authority root");

    const SchemaObjectID object{
        .kind = object_kind,
        .database_uuid = db_uuid,
        .object_uuid = actual_table_id.uuid,
    };
    if (object.kind == SchemaObjectKind::Table)
    {
        if (isAtomicMaterializedViewInnerTableName(actual_table_id.table_name))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Atomic MaterializedView physical inner table {} cannot acquire a separate user-defined type authority identity",
                actual_table_id.getNameForLogs());
        }
        for (auto iterator = getTablesIterator(local_context, {}, false); iterator->isValid(); iterator->next())
        {
            const auto owner = iterator->table();
            const auto * owner_materialized_view = owner ? owner->as<StorageMaterializedView>() : nullptr;
            if (owner_materialized_view && owner_materialized_view->hasInnerTable()
                && owner_materialized_view->getTargetTableId().uuid == object.object_uuid)
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "The physical inner table of MaterializedView {} cannot acquire a separate user-defined type authority identity",
                    owner->getStorageID().getNameForLogs());
            }
        }
    }
    const auto pending = metadata.getPendingUDTColumnAlter();
    const bool initial_admission = planning_root->get().findExpectationRecord(object) == nullptr;
    std::optional<AtomicDatabaseSchemaMutationDependentObjectImage> before_image;
    String before_object_name;
    String before_canonical_metadata;
    UInt64 before_object_schema_revision = 0;
    if (initial_admission)
    {
        if (object.kind != SchemaObjectKind::Table)
            throw Exception(ErrorCodes::ABORTED, "Only a physical Table can enter mapped ALTER admission");
        const bool has_trusted_restore_binding
            = trusted_boundary_rollback && metadata.getBoundUDTReferences() && metadata.getBoundUDTExpectation();
        if ((!pending || !pending->getDesiredReferences()) && !has_trusted_restore_binding)
            throw Exception(ErrorCodes::ABORTED, "Physical-to-mapped Table ALTER has no final logical binding");
        if (planning_root->get().getSchemaObjectDependencyGraph().containsNode(object)
            || planning_root->get().pinAuthorityInventory()->find({
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = object.object_uuid,
            }))
        {
            throw Exception(ErrorCodes::ABORTED, "Physical-to-mapped Table ALTER identity is partially present in the authority root");
        }
        auto live_metadata = table->getInMemoryMetadataPtr(local_context, false);
        if (!live_metadata)
            throw Exception(ErrorCodes::ABORTED, "Physical-to-mapped Table ALTER has no live metadata snapshot");
        live_metadata->validateBoundUDTReferences();
        if (!trusted_boundary_rollback
            && (live_metadata->getBoundUDTReferences() || live_metadata->getBoundUDTExpectation()
                || live_metadata->getPendingUDTColumnAlter()))
        {
            throw Exception(ErrorCodes::ABORTED, "Physical-to-mapped Table ALTER live metadata is not physical-only");
        }
        before_object_name = table_id.table_name;
        before_canonical_metadata = readMetadataFile(getDisk(), getObjectMetadataPath(table_id.table_name));
        before_object_schema_revision = 1;
    }
    else
    {
        const auto planning_inventory = planning_root->get().pinAuthorityInventory();
        const auto planning_graph = planning_root->get().pinSchemaObjectDependencyGraph();
        if (!planning_inventory || !planning_graph)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped object ALTER authority root has no inventory or dependency graph");
        auto reconciliation = storage->readAndReconcileAuthorityRecords(*planning_inventory, *planning_graph);
        before_image.emplace(findExactDependentObjectImage(std::move(reconciliation), object));
        if (before_image->object_name != table_id.table_name)
            throw Exception(ErrorCodes::ABORTED, "Mapped object ALTER name differs from its durable installation mapping");
        before_object_name = before_image->object_name;
        before_canonical_metadata = before_image->canonical_metadata_bytes;
        before_object_schema_revision = before_image->expectation.object_schema_revision;
    }

    if (expected_current_metadata)
    {
        ParserCreateQuery expected_parser;
        ASTPtr expected_ast = parseQuery(
            expected_parser,
            before_canonical_metadata.data(),
            before_canonical_metadata.data() + before_canonical_metadata.size(),
            "mapped Atomic object rollback metadata",
            0,
            local_context->getSettingsRef()[Setting::max_parser_depth],
            local_context->getSettingsRef()[Setting::max_parser_backtracks]);
        applyMetadataChangesToCreateQuery(expected_ast, *expected_current_metadata, local_context, false);
        if (getObjectDefinitionFromCreateQuery(expected_ast) != before_canonical_metadata)
            throw Exception(ErrorCodes::ABORTED, "Mapped Atomic object rollback no longer targets the committed metadata image");
    }

    std::optional<PersistedTypeReferences> desired_references;
    if (pending)
    {
        if (pending->getObject() != object || pending->getBeforeObjectSchemaRevision() != before_object_schema_revision
            || pending->getAfterPhysicalColumns() != metadata.getColumns().getAllPhysical())
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped object ALTER plan is stale or belongs to another object");
        }
        desired_references = pending->getDesiredReferences();
    }
    else if (trusted_boundary_rollback)
    {
        metadata.validateBoundUDTReferences();
        const auto & restore_references = metadata.getBoundUDTReferences();
        const auto & restore_expectation = metadata.getBoundUDTExpectation();
        if (restore_references || restore_expectation)
        {
            if (object.kind != SchemaObjectKind::Table || !restore_references || !restore_expectation
                || restore_references->getObject() != object)
                throw Exception(ErrorCodes::ABORTED, "Trusted mapped-table rollback has incomplete restore provenance");
            desired_references = rebaseBoundTableColumnTypeReferences(
                metadata.getColumns().getAllPhysical(), *restore_references, *restore_expectation, before_object_schema_revision);
        }
    }
    else
    {
        metadata.validateBoundUDTReferences();
        const auto & retained_references = metadata.getBoundUDTReferences();
        const auto & retained_expectation = metadata.getBoundUDTExpectation();
        if (!retained_references || !retained_expectation || retained_references->getObject() != object)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "A mapped object can lose logical provenance only through its prepared ALTER/physicalization path");
        }
        if (object.kind == SchemaObjectKind::Table)
        {
            desired_references = rebaseBoundTableColumnTypeReferences(
                metadata.getColumns().getAllPhysical(), *retained_references, *retained_expectation, before_object_schema_revision);
        }
        else
            desired_references = rebaseBoundStoredObjectTypeReferences(*retained_references, *retained_expectation);
    }

    ParserCreateQuery parser;
    ASTPtr after_ast = parseQuery(
        parser,
        before_canonical_metadata.data(),
        before_canonical_metadata.data() + before_canonical_metadata.size(),
        "mapped Atomic object metadata",
        0,
        local_context->getSettingsRef()[Setting::max_parser_depth],
        local_context->getSettingsRef()[Setting::max_parser_backtracks]);
    auto & after_create = after_ast->as<ASTCreateQuery &>();
    if (after_create.uuid != table_id.uuid)
        throw Exception(ErrorCodes::ABORTED, "Mapped object ALTER metadata UUID differs from the live storage");
    const bool metadata_kind_matches = (object.kind == SchemaObjectKind::Table && !after_create.isView() && !after_create.is_dictionary)
        || (object.kind == SchemaObjectKind::View && after_create.isView() && !after_create.is_dictionary)
        || (object.kind == SchemaObjectKind::Dictionary && after_create.is_dictionary);
    if (!metadata_kind_matches)
        throw Exception(ErrorCodes::ABORTED, "Mapped object ALTER metadata kind differs from its stable storage identity");
    applyMetadataChangesToCreateQuery(after_ast, metadata, local_context, validate_new_create_query);
    String after_canonical_metadata = getObjectDefinitionFromCreateQuery(after_ast);
    if (validate_new_create_query)
    {
        const size_t max_query_size = local_context->getSettingsRef()[Setting::max_query_size];
        if (max_query_size && after_canonical_metadata.size() > max_query_size)
        {
            throw Exception(
                ErrorCodes::QUERY_IS_TOO_LARGE,
                "The resulting metadata of table {} ({} bytes) would exceed max_query_size ({})",
                table_id.getNameForLogs(),
                after_canonical_metadata.size(),
                max_query_size);
        }
    }
    auto ref_dependencies = getDependenciesFromCreateQuery(
        local_context->getGlobalContext(), table_id.getQualifiedName(), after_ast, local_context->getCurrentDatabase());
    auto loading_dependencies
        = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), after_ast);
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(
        table_id.getQualifiedName(), ref_dependencies.dependencies, loading_dependencies);
    std::vector<SchemaObjectID> after_object_dependencies;
    if (desired_references)
    {
        after_object_dependencies
            = collectMappedObjectDependencies(*this, planning_root->get(), after_ast, table_id.getQualifiedName(), object, local_context);
    }

    storage->maintainCheckpointBeforeMutation(planning_root->get());
    auto mutation_guard = storage->issueMutationGuard();
    const UInt64 predecessor = mutation_guard.getDurablePredecessorTransactionID();
    if (predecessor == std::numeric_limits<UInt64>::max())
        throw DatabaseSchemaMutationReplayConflictError("Atomic schema transaction ID domain is exhausted");
    DependentObjectMutationRequest request;
    request.kind = initial_admission ? DependentObjectMutationKind::AlterAdmission : DependentObjectMutationKind::Alter;
    request.object = object;
    request.transaction_id = predecessor + 1;
    request.expected_database_catalog_epoch = planning_root->get().getDatabaseCatalogEpoch();
    if (initial_admission)
    {
        request.physical_before_object_name = std::move(before_object_name);
        request.physical_before_canonical_metadata_bytes = std::move(before_canonical_metadata);
    }
    else
        request.before_image = std::move(*before_image);
    request.physical_columns = metadata.getColumns().getAllPhysical();
    request.after_canonical_metadata_bytes = std::move(after_canonical_metadata);
    request.after_persisted_references = std::move(desired_references);
    request.after_object_dependencies = std::move(after_object_dependencies);
    auto planned = DependentObjectMutationPlanner::plan(planning_root->get(), std::move(request));
    const auto planned_kind = planned.getKind();

    if (static_cast<bool>(planned.getBoundUDTReferences()) != static_cast<bool>(planned.getSidecarExpectation())
        || static_cast<bool>(planned.getBoundUDTReferences()) != static_cast<bool>(planned.getVerificationStamp()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped object ALTER planner produced an incomplete binding package");
    if (planned.getBoundUDTReferences())
        assertUDTNewDefinitionClosureOperationAllowed(*planned.getBoundUDTReferences(), AuthorityQuarantineOperationKind::DDL);
    auto prepared = DependentObjectMutationCoordinator::prepareCommit(
        std::move(*planning_root), *authority, *storage, mutation_guard, std::move(planned));
    auto published = commitMappedTableMutationWithRecovery(*authority, *storage, mutation_guard, prepared);
    if (published.kind != planned_kind || static_cast<bool>(published.bound_references) != static_cast<bool>(published.expectation)
        || static_cast<bool>(published.bound_references) != static_cast<bool>(published.verification_stamp))
        std::terminate();

    try
    {
        if (pending)
            pending->completePublication(published.bound_references, published.expectation, published.verification_stamp);
        DatabaseCatalog::instance().updateDependencies(
            table_id,
            ref_dependencies.dependencies,
            loading_dependencies,
            ref_dependencies.mv_from_dependency ? TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()} : TableNamesSet{});
    }
    catch (...)
    {
        std::terminate();
    }
    return {
        .bound_references = std::move(published.bound_references),
        .expectation = std::move(published.expectation),
        .verification_stamp = std::move(published.verification_stamp),
    };
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::dropTable");
    waitDatabaseStarted();
    auto table = tryGetTable(table_name, local_context);
    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(table_name));
    bool defer_mapped_inner_table_drop = false;
    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        assertNotLiveMappedMaterializedViewInnerTable(table, "DROP");
        if (hasDatabaseOwnedUDTTableBinding(table, local_context))
        {
            const auto metadata_snapshot = table->getInMemoryMetadataPtr(local_context, false);
            assertUDTNewStorageOperationAllowed(metadata_snapshot, UDT::AuthorityQuarantineOperationKind::DDL);
            const auto * materialized_view = table->as<StorageMaterializedView>();
            defer_mapped_inner_table_drop = materialized_view && materialized_view->hasInnerTable();
        }
    }

    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread). A mapped
    /// inner-table MaterializedView is the exception: deleting its physical
    /// child before the outer authority transaction commits could leave a
    /// still-published logical object without storage when that transaction
    /// fails. Detach/enqueue the outer object first; its normal background
    /// drop then removes the physical-only child after the schema lock is
    /// released. SYNC still waits for that outer cleanup task.
    if (!defer_mapped_inner_table_drop)
        table->dropInnerTableIfAny(sync, local_context);

    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        const auto current_table = getTable(table_name, local_context);
        if (current_table != table)
            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Table {}.{} was replaced while being dropped",
                backQuote(getDatabaseName()),
                backQuote(table_name));
        /// Repeat under the final mutation lock: a physical MV can become
        /// mapped after the early diagnostic check, and its child must not
        /// cross that publication boundary into an ordinary DROP.
        assertNotLiveMappedMaterializedViewInnerTable(current_table, "DROP");
        if (hasDatabaseOwnedUDTTableBinding(current_table, local_context))
            dropUDTTable(local_context, table_name, current_table, sync);
        else
            dropTableImplWithoutUDTGuard(local_context, table_name, sync);
    }
    reclaimRetiredUDTRootsNoThrow();
}

void DatabaseAtomic::dropTableImpl(ContextPtr local_context, const String & table_name, bool sync)
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
    {
        dropTableImplWithoutUDTGuard(local_context, table_name, sync);
        return;
    }

    waitDatabaseStarted();
    {
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        const auto table = getTable(table_name, local_context);
        assertNotLiveMappedMaterializedViewInnerTable(table, "DROP");
        if (hasDatabaseOwnedUDTTableBinding(table, local_context))
            dropUDTTable(local_context, table_name, table, sync);
        else
            dropTableImplWithoutUDTGuard(local_context, table_name, sync);
    }
    reclaimRetiredUDTRootsNoThrow();
}

void DatabaseAtomic::dropUDTTable(ContextPtr, const String & table_name, const StoragePtr & table, bool sync)
{
    using UDT::AtomicAuthority;
    using UDT::AtomicDatabaseSchemaMutationStorage;
    using UDT::AuthorityQuarantineOperationKind;
    using UDT::DatabaseSchemaMutationReplayConflictError;
    using UDT::DependentObjectMutationCoordinator;
    using UDT::DependentObjectMutationKind;
    using UDT::DependentObjectMutationPlanner;
    using UDT::DependentObjectMutationRequest;
    using UDT::SchemaObjectID;
    using UDT::SchemaObjectKind;

    const auto metadata_snapshot = table->getInMemoryMetadataPtr(nullptr, false);
    assertUDTNewStorageOperationAllowed(metadata_snapshot, AuthorityQuarantineOperationKind::DDL);

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    std::optional<AtomicAuthority::RootSnapshot> planning_root;
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown || udt_table_startup_state || !udt_authority || !udt_mutation_storage
            || active_udt_authority.load(std::memory_order_acquire) != udt_authority.get())
            throw Exception(ErrorCodes::ABORTED, "Mapped table DROP requires one active, fully recovered Atomic authority");
        authority = udt_authority.get();
        storage = udt_mutation_storage.get();
        planning_root.emplace(authority->acquireCurrentRoot());
    }
    if (!*planning_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table DROP has no authority root");

    const SchemaObjectID object{
        .kind = mappedSchemaObjectKindForStorage(*table),
        .database_uuid = db_uuid,
        .object_uuid = table->getStorageID().uuid,
    };
    if (object.kind == SchemaObjectKind::Table && isAtomicMaterializedViewInnerTableName(table_name))
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Atomic MaterializedView physical inner table {} unexpectedly has a separate user-defined type authority identity",
            table->getStorageID().getNameForLogs());
    }
    const auto planning_inventory = planning_root->get().pinAuthorityInventory();
    const auto planning_graph = planning_root->get().pinSchemaObjectDependencyGraph();
    if (!planning_inventory || !planning_graph)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table DROP authority root has no inventory or dependency graph");
    auto reconciliation = storage->readAndReconcileAuthorityRecords(*planning_inventory, *planning_graph);
    auto before_image = findExactDependentObjectImage(std::move(reconciliation), object);
    if (before_image.object_name != table_name)
        throw Exception(ErrorCodes::ABORTED, "Mapped table DROP name differs from its durable installation mapping");

    storage->maintainCheckpointBeforeMutation(planning_root->get());
    auto mutation_guard = storage->issueMutationGuard();
    const UInt64 predecessor = mutation_guard.getDurablePredecessorTransactionID();
    if (predecessor == std::numeric_limits<UInt64>::max())
        throw DatabaseSchemaMutationReplayConflictError("Atomic schema transaction ID domain is exhausted");
    DependentObjectMutationRequest request;
    request.kind = DependentObjectMutationKind::Drop;
    request.object = object;
    request.transaction_id = predecessor + 1;
    request.expected_database_catalog_epoch = planning_root->get().getDatabaseCatalogEpoch();
    request.before_image = std::move(before_image);
    auto planned = DependentObjectMutationPlanner::plan(planning_root->get(), std::move(request));
    auto prepared = DependentObjectMutationCoordinator::prepareCommit(
        std::move(*planning_root), *authority, *storage, mutation_guard, std::move(planned));

    auto published = commitMappedTableMutationWithRecovery(*authority, *storage, mutation_guard, prepared);
    if (published.kind != DependentObjectMutationKind::Drop || published.bound_references || published.expectation
        || published.verification_stamp)
        std::terminate();

    const auto table_id = table->getStorageID();
    const String dropped_metadata_path = DatabaseCatalog::instance().getPathForDroppedMetadata(table_id);
    try
    {
        {
            std::lock_guard tables_lock(mutex);
            if (getTableUnlocked(table_name) != table)
                std::terminate();
            const auto detached = DatabaseOrdinary::detachTableUnlocked(table_name);
            if (detached != table)
                std::terminate();
            table_name_to_path.erase(table_name);
            snapshot_detached_tables.erase(table_name);
        }
        if (table->storesDataOnDisk())
            tryRemoveSymlink(table_name);
        DatabaseCatalog::instance().enqueueDroppedTableCleanup(table_id, table, getDisk(), dropped_metadata_path, sync);
    }
    catch (...)
    {
        std::terminate();
    }
}

void DatabaseAtomic::dropTableImplWithoutUDTGuard(ContextPtr local_context, const String & table_name, bool sync)
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
            txn->commit(); /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// We apply changes in ZooKeeper before applying changes in local metadata
        /// file to reduce probability of failures between these operations (it's
        /// more likely to lost connection, than to fail before applying local
        /// changes).
        /// TODO better detection and recovery

        db_disk->replaceFile(table_metadata_path,
                             table_metadata_path_drop); /// Mark table as dropped
        DatabaseOrdinary::detachTableUnlocked(table_name); /// Should never throw
        table_name_to_path.erase(table_name);
        snapshot_detached_tables.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data
    /// in background. Cleanup is performed outside of database to allow easily
    /// DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, db_disk, table_metadata_path_drop, sync);
}

void DatabaseAtomic::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary) TSA_NO_THREAD_SAFETY_ANALYSIS /// TSA does not support conditional locking
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::renameTable");
    createDirectories();
    waitDatabaseStarted();

    if (typeid(*this) != typeid(to_database) && typeid_cast<DatabaseOrdinary *>(&to_database))
    {
        /// The generic path takes the table-exclusive lock first and then the
        /// source schema guard. Delegating before locking here avoids inverting
        /// ALTER's table -> schema order.
        DatabaseOnDisk::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
        return;
    }

    bool reclaim_retired_udt_roots = false;
    SCOPE_EXIT({
        if (reclaim_retired_udt_roots)
            reclaimRetiredUDTRootsNoThrow();
    });

    /// A cross-database move must be serialized with admission in both
    /// databases. In particular, a lazy StorageTableProxy may expose only a
    /// physical column cache, so storage-local bound references are not the
    /// authority for deciding whether a table may leave its database.
    auto * target_atomic_database = dynamic_cast<DatabaseAtomic *>(&to_database);
    if (target_atomic_database && target_atomic_database != this)
    {
        target_atomic_database->createDirectories();
        target_atomic_database->waitDatabaseStarted();
    }

    /// Deferred population may attach tables and enter the UDT schema boundary,
    /// so it must complete before either schema-mutation mutex is acquired.
    ensurePopulated();
    if (target_atomic_database && target_atomic_database != this)
        target_atomic_database->ensurePopulated();

    std::unique_lock<std::mutex> source_schema_mutation_lock;
    std::unique_lock<std::mutex> target_schema_mutation_lock;
    if (this == &to_database)
    {
        source_schema_mutation_lock = std::unique_lock<std::mutex>(udt_schema_mutation_mutex);
    }
    else
    {
        source_schema_mutation_lock = std::unique_lock<std::mutex>(udt_schema_mutation_mutex, std::defer_lock);
        if (target_atomic_database)
        {
            target_schema_mutation_lock = std::unique_lock<std::mutex>(target_atomic_database->udt_schema_mutation_mutex, std::defer_lock);
            std::lock(source_schema_mutation_lock, target_schema_mutation_lock);
        }
        else
        {
            source_schema_mutation_lock.lock();
        }
    }

    if (typeid(*this) != typeid(to_database))
    {
        if (!allowMoveTableToOtherDatabaseEngine(to_database))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Moving tables between databases of different engines is "
                "not supported");
    }

    std::string message;
    if (exchange && !supportsAtomicRename(&message))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "RENAME EXCHANGE is not supported because exchanging files "
            "is not supported by the OS ({})",
            message);

    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;
    if (inside_database && table_name == to_table_name)
        return;

    /// The schema guard(s) are already held and the database table mutexes are
    /// not. Resolve every database-owned UDT decision here: degraded ownership
    /// inspection may need getDatabaseName(), which also takes the table mutex.
    /// After locking below, exact StoragePtr checks prove that these decisions
    /// still describe the catalog entries being renamed.
    const auto expected_table = getTable(table_name, local_context);
    assertNotLiveMappedMaterializedViewInnerTable(expected_table, "RENAME");
    if (dictionary && !expected_table->isDictionary())
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE "
            "DICTIONARY) for tables");
    }
    const bool expected_table_mapped = hasDatabaseOwnedUDTTableBinding(expected_table, local_context);

    StoragePtr expected_other_table;
    bool expected_other_table_mapped = false;
    if (exchange)
    {
        expected_other_table = other_db.getTable(to_table_name, local_context);
        other_db.assertNotLiveMappedMaterializedViewInnerTable(expected_other_table, "RENAME EXCHANGE");
        if (dictionary && !expected_other_table->isDictionary())
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE "
                "DICTIONARY) for tables");
        }
        expected_other_table_mapped = other_db.hasDatabaseOwnedUDTTableBinding(expected_other_table, local_context);
    }

    const bool target_owns_source_table
        = !inside_database && other_db.hasDatabaseOwnedTableExpectationForCrossDatabaseMove(expected_table->getStorageID().uuid);
    const bool source_owns_target_table
        = !inside_database && exchange && hasDatabaseOwnedTableExpectationForCrossDatabaseMove(expected_other_table->getStorageID().uuid);
    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        /// Path can be not set for DDL dictionaries, but it does not matter for
        /// StorageDictionary.
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        chassert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        /// This path bypasses detachTableUnlocked, so clear stale async-load names
        /// here too, otherwise getAllTableNames keeps suggesting the old name
        /// (#91777).
        db.eraseAsyncLoadState(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_)
                      TSA_REQUIRES(db.mutex)
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
    if (table != expected_table)
        throw Exception(ErrorCodes::ABORTED, "Atomic table {} changed while preparing RENAME", table_name);
    bool mapped_table = false;

    if (!inside_database)
    {
        UDT::assertTableCanLeaveAtomicDatabase(expected_table_mapped, table->getStorageID().getNameForLogs());
        UDT::assertTableCanEnterAtomicDatabase(target_owns_source_table, table->getStorageID().getNameForLogs(), other_db.database_name);
    }
    else
    {
        mapped_table = expected_table_mapped;
        reclaim_retired_udt_roots = mapped_table;
        if (mapped_table && exchange)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "RENAME EXCHANGE is not supported for an Atomic table with durable user-defined type bindings");
        }
    }

    StorageID old_table_id = table->getStorageID();
    StorageID new_table_id = {other_db.database_name, to_table_name, old_table_id.uuid};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (other_table != expected_other_table)
            throw Exception(ErrorCodes::ABORTED, "Atomic table {} changed while preparing RENAME EXCHANGE", to_table_name);
        other_table_new_id = {database_name, table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
        if (!inside_database)
        {
            UDT::assertTableCanLeaveAtomicDatabase(expected_other_table_mapped, other_table->getStorageID().getNameForLogs());
            UDT::assertTableCanEnterAtomicDatabase(source_owns_target_table, other_table->getStorageID().getNameForLogs(), database_name);
        }
        else
        {
            if (expected_other_table_mapped)
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "RENAME EXCHANGE is not supported for an Atomic table with durable user-defined type bindings");
            }
        }
    }

    /// Table renaming actually begins here
    auto txn = local_context->getZooKeeperMetadataTransaction();
    if (txn && !local_context->isInternalSubquery())
        txn->commit(); /// Commit point (a sort of) for Replicated database

    auto db_disk = getDisk();

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery
    std::optional<StorageInMemoryMetadata> mapped_after_metadata;
    if (mapped_table)
    {
        using UDT::AtomicAuthority;
        using UDT::AtomicDatabaseSchemaMutationStorage;
        using UDT::AuthorityQuarantineOperationKind;
        using UDT::DatabaseSchemaMutationReplayConflictError;
        using UDT::DependentObjectMutationCoordinator;
        using UDT::DependentObjectMutationKind;
        using UDT::DependentObjectMutationPlanner;
        using UDT::DependentObjectMutationRequest;
        using UDT::SchemaObjectID;
        using UDT::SchemaObjectKind;

        const auto object_kind = mappedSchemaObjectKindForStorage(*table);
        if (object_kind == SchemaObjectKind::Table && isAtomicMaterializedViewInnerTableName(to_table_name))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "A mapped Atomic Table cannot be renamed into the reserved MaterializedView inner-table namespace");
        }
        const String engine_name = table->getName();
        const bool supported_engine = object_kind != SchemaObjectKind::Table || (engine_name == "Memory" && !table->storesDataOnDisk())
            || (table->isMergeTree() && table->storesDataOnDisk() && !table->isSharedStorage() && !engine_name.starts_with("Replicated")
                && !engine_name.starts_with("Shared"));
        if (!supported_engine)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Mapped Atomic table RENAME supports only Memory and non-replicated, non-shared MergeTree-family tables");
        }

        AtomicAuthority * authority = nullptr;
        AtomicDatabaseSchemaMutationStorage * storage = nullptr;
        std::optional<AtomicAuthority::RootSnapshot> planning_root;
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (udt_authority_shutdown || udt_table_startup_state || !udt_authority || !udt_mutation_storage
                || active_udt_authority.load(std::memory_order_acquire) != udt_authority.get())
            {
                throw Exception(ErrorCodes::ABORTED, "Mapped table RENAME requires one active, fully recovered Atomic authority");
            }
            authority = udt_authority.get();
            storage = udt_mutation_storage.get();
            planning_root.emplace(authority->acquireCurrentRoot());
        }
        if (!*planning_root)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table RENAME has no authority root");

        const SchemaObjectID object{
            .kind = object_kind,
            .database_uuid = db_uuid,
            .object_uuid = old_table_id.uuid,
        };
        /// The database table mutex is already held here. In particular,
        /// `StorageMaterializedView::getInMemoryMetadataPtr` resolves its target
        /// through the catalog and would try to take this mutex recursively.
        /// Durable UDT bindings belong to the outer storage metadata, so read
        /// that exact snapshot without invoking storage-specific decoration.
        auto current_metadata_handle = table->IStorage::getInMemoryMetadataPtr(local_context, false);
        StorageMetadataPtr current_metadata = current_metadata_handle;
        if (!current_metadata)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table RENAME has no metadata snapshot");
        current_metadata->validateBoundUDTReferences();
        if (!current_metadata->getBoundUDTReferences() || !current_metadata->getBoundUDTExpectation()
            || current_metadata->getBoundUDTReferences()->getObject() != object)
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped table RENAME metadata lost its durable binding package");
        }
        assertUDTNewStorageOperationAllowed(current_metadata, AuthorityQuarantineOperationKind::DDL);

        const auto planning_inventory = planning_root->get().pinAuthorityInventory();
        const auto planning_graph = planning_root->get().pinSchemaObjectDependencyGraph();
        if (!planning_inventory || !planning_graph)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table RENAME authority root has no inventory or dependency graph");
        auto reconciliation = storage->readAndReconcileAuthorityRecords(*planning_inventory, *planning_graph);
        auto before_image = findExactDependentObjectImage(std::move(reconciliation), object);
        if (before_image.object_name != table_name || before_image.expectation != *current_metadata->getBoundUDTExpectation())
        {
            throw Exception(ErrorCodes::ABORTED, "Mapped table RENAME live metadata differs from its durable image");
        }

        storage->maintainCheckpointBeforeMutation(planning_root->get());
        auto mutation_guard = storage->issueMutationGuard();
        const UInt64 predecessor = mutation_guard.getDurablePredecessorTransactionID();
        if (predecessor == std::numeric_limits<UInt64>::max())
            throw DatabaseSchemaMutationReplayConflictError("Atomic schema transaction ID domain is exhausted");
        DependentObjectMutationRequest request;
        request.kind = DependentObjectMutationKind::Rename;
        request.object = object;
        request.transaction_id = predecessor + 1;
        request.expected_database_catalog_epoch = planning_root->get().getDatabaseCatalogEpoch();
        request.before_image = std::move(before_image);
        request.after_object_name = to_table_name;
        request.physical_columns = current_metadata->getColumns().getAllPhysical();
        auto planned = DependentObjectMutationPlanner::plan(planning_root->get(), std::move(request));
        if (!planned.getBoundUDTReferences() || !planned.getSidecarExpectation())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped table RENAME planner produced no rebound metadata package");
        mapped_after_metadata.emplace(*current_metadata);
        if (object_kind == SchemaObjectKind::Table)
        {
            mapped_after_metadata->setColumnsAndBoundUDTReferences(
                current_metadata->getColumns(), planned.getBoundUDTReferences(), *planned.getSidecarExpectation());
        }
        else
        {
            mapped_after_metadata->setColumnsAndBoundStoredObjectUDTReferences(
                current_metadata->getColumns(), planned.getBoundUDTReferences(), *planned.getSidecarExpectation());
        }
        mapped_after_metadata->setBoundUDTVerificationStamp(planned.getVerificationStamp());
        auto prepared = DependentObjectMutationCoordinator::prepareCommit(
            std::move(*planning_root), *authority, *storage, mutation_guard, std::move(planned));
        auto published = commitMappedTableMutationWithRecovery(*authority, *storage, mutation_guard, prepared);
        if (published.kind != DependentObjectMutationKind::Rename || !published.bound_references || !published.expectation
            || published.bound_references != mapped_after_metadata->getBoundUDTReferences()
            || *published.expectation != *mapped_after_metadata->getBoundUDTExpectation()
            || published.verification_stamp != mapped_after_metadata->getBoundUDTVerificationStamp())
        {
            std::terminate();
        }
    }
    else if (exchange)
        db_disk->renameExchange(old_metadata_path, new_metadata_path);
    else
        db_disk->moveFile(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not
    /// throw (if they do, it's a logical error)
    try
    {
        table_data_path = detach(*this, table_name, table->storesDataOnDisk());
        if (exchange)
            other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

        /// The mapped root is already published. Install its matching metadata
        /// before engine callbacks: `StorageDictionary` refreshes its config
        /// synchronously during rename and must observe the same expectation.
        if (mapped_after_metadata)
            table->setInMemoryMetadata(*mapped_after_metadata);
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
    catch (...)
    {
        if (mapped_table)
            std::terminate();
        throw;
    }
}

void DatabaseAtomic::commitCreateTable(
    const ASTCreateQuery & query,
    const StoragePtr & table,
    const String & table_metadata_tmp_path,
    const String & table_metadata_path,
    ContextPtr query_context)
{
    /// CREATE commits the new outer object's own durable metadata. Wrappers
    /// such as Alias may expose their target's live metadata through the
    /// virtual lookup, but that target binding is neither owned nor persisted
    /// by the wrapper and must not select the mapped-table commit protocol.
    const auto metadata = table->IStorage::getInMemoryMetadataPtr(query_context, true);
    if (!metadata)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic table CREATE has no metadata snapshot");
    metadata->validateBoundUDTReferences();
    if (metadata->getBoundUDTReferences() || metadata->getBoundUDTExpectation())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Mapped user-defined type table {} cannot use the ordinary "
            "Atomic CREATE commit path",
            table->getStorageID().getNameForLogs());
    }

    auto db_disk = getDisk();

    createDirectories();
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    std::unique_lock<std::mutex> restore_schema_lock;
    try
    {
        if (udt_authority_mode != AuthorityMode::Unsupported && query_context && query_context->isUnderRestore())
        {
            /// The preflight lease alone is deliberately not publication
            /// authority. Re-enter schema serialization at the final Atomic
            /// metadata/catalog commit, prove that this RESTORE still owns a
            /// live lease, and recheck that no UDT authority image appeared.
            restore_schema_lock = std::unique_lock(udt_schema_mutation_mutex);
            if (!udt_restore_publication_leases.load(std::memory_order_acquire))
                throw Exception(ErrorCodes::ABORTED, "Atomic RESTORE publication lost its database-owned preflight lease");
            std::lock_guard authority_lock(udt_authority_mutex);
            if (active_udt_authority.load(std::memory_order_acquire) || udt_authority || udt_degraded_startup_status
                || udt_table_startup_state || udt_verification_runtime || udt_verification_scheduler
                || (udt_mutation_storage && udt_mutation_storage->hasDurableAuthorityMarker()))
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "RESTORE into an active or durable Atomic user-defined type authority is not supported because backup manifests omit "
                    "its complete authority and database policy state");
            }
        }

        std::lock_guard lock{mutex};
        if (query.getDatabase() != database_name)
            throw Exception(
                ErrorCodes::UNKNOWN_DATABASE,
                "Database was renamed to `{}`, cannot create table in `{}`",
                database_name,
                query.getDatabase());
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        chassert(DatabaseCatalog::instance().hasUUIDMapping(query.uuid));

        auto txn = query_context->getZooKeeperMetadataTransaction();
        if (txn && !query_context->isInternalSubquery())
            txn->commit(); /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following
        /// renameNoReplace(...)
        /// TODO better detection and recovery

        /// It throws if `table_metadata_path` already exists (it's possible if
        /// table was detached)
        db_disk->moveFile(table_metadata_tmp_path,
                          table_metadata_path); /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table); /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
    if (restore_schema_lock.owns_lock())
        restore_schema_lock.unlock();
    if (table->storesDataOnDisk())
        tryCreateSymlink(table);
}

void DatabaseAtomic::commitAlterTable(
    const StorageID & table_id,
    const String & table_metadata_tmp_path,
    const String & table_metadata_path,
    const String & /*statement*/,
    ContextPtr query_context)
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
        txn->commit(); /// Commit point (a sort of) for Replicated database

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    check_file_exists = db_disk->renameExchangeIfSupported(table_metadata_tmp_path, table_metadata_path);
    if (!check_file_exists)
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
}

void DatabaseAtomic::assertDetachedTableNotInUse(const UUID & uuid)
{
    /// Without this check the following race is possible since table RWLocks are
    /// not used:
    /// 1. INSERT INTO table ...;
    /// 2. DETACH TABLE table; (INSERT still in progress, it holds StoragePtr)
    /// 3. ATTACH TABLE table; (new instance of Storage with the same UUID is
    /// created, instances share data on disk)
    /// 4. INSERT INTO table ...; (both Storage instances writes data without any
    /// synchronization) To avoid it, we remember UUIDs of detached tables and
    /// does not allow ATTACH table with such UUID until detached instance still
    /// in use.
    if (detached_tables.contains(uuid))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS,
            "Cannot attach table with UUID {}, "
            "because it was detached but still used by some query. Retry later.",
            uuid);
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

DatabaseTablesIteratorPtr DatabaseAtomic::getTablesIterator(
    ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    auto base_iter = DatabaseOrdinary::getTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

std::vector<std::pair<ASTPtr, StoragePtr>>
DatabaseAtomic::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    auto selected = DatabaseOrdinary::getTablesForBackup(filter, local_context);
    if (udt_authority_mode == AuthorityMode::Unsupported)
        return selected;

    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (active_udt_authority.load(std::memory_order_acquire)
            || (udt_mutation_storage && udt_mutation_storage->hasDurableAuthorityMarker()))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "BACKUP of an active or durable Atomic user-defined type authority is not supported because backup manifests omit its "
                "complete authority and database policy state");
        }
    }
    for (const auto & [create_query, table] : selected)
    {
        static_cast<void>(create_query);
        if (table && hasDatabaseOwnedUDTTableBinding(table, local_context))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "BACKUP of mapped user-defined type table {} is not supported because backup manifests omit its sidecar and authority "
                "state",
                table->getStorageID().getNameForLogs());
        }
    }
    return selected;
}

std::shared_ptr<void> DatabaseAtomic::acquireUDTBackupLease(const std::vector<StoragePtr> & selected_tables, ContextPtr local_context) const
{
    if (udt_authority_mode == AuthorityMode::Unsupported)
        return {};

    waitDatabaseStarted();
    std::unique_lock schema_mutation_lock(udt_schema_mutation_mutex);
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (active_udt_authority.load(std::memory_order_acquire)
            || (udt_mutation_storage && udt_mutation_storage->hasDurableAuthorityMarker()))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "BACKUP of an active or durable Atomic user-defined type authority is not supported because backup manifests omit its "
                "complete authority and database policy state");
        }
    }
    for (const auto & table : selected_tables)
    {
        if (!table)
            continue;

        const auto table_id = table->getStorageID();
        {
            std::lock_guard tables_lock(mutex);
            const auto table_it = tables.find(table_id.table_name);
            if (table_id.database_name != database_name || table_it == tables.end() || table_it->second != table)
            {
                throw Exception(
                    ErrorCodes::ABORTED,
                    "Atomic table {} changed between BACKUP metadata collection and schema lease acquisition",
                    table_id.getNameForLogs());
            }
        }

        if (hasDatabaseOwnedUDTTableBinding(table, local_context))
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "BACKUP of mapped user-defined type table {} is not supported because backup manifests omit its sidecar and authority "
                "state",
                table_id.getNameForLogs());
        }
    }

    struct Lease final
    {
        explicit Lease(std::unique_lock<std::mutex> lock_)
            : lock(std::move(lock_))
        {
        }
        std::unique_lock<std::mutex> lock;
    };
    return std::make_shared<Lease>(std::move(schema_mutation_lock));
}

void DatabaseAtomic::createTableRestoredFromBackup(
    const ASTPtr & create_table_query,
    ContextMutablePtr local_context,
    std::shared_ptr<IRestoreCoordination> restore_coordination,
    UInt64 timeout_ms)
{
    bool owns_restore_publication_lease = false;
    if (udt_authority_mode != AuthorityMode::Unsupported)
    {
        if (!local_context || !local_context->isUnderRestore())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic RESTORE publication requires an exact restore-owned query context");
        std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (active_udt_authority.load(std::memory_order_acquire) || udt_authority || udt_degraded_startup_status
                || udt_table_startup_state || udt_verification_runtime || udt_verification_scheduler
                || (udt_mutation_storage && udt_mutation_storage->hasDurableAuthorityMarker()))
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "RESTORE into an active or durable Atomic user-defined type authority is not supported because backup manifests omit "
                    "its complete authority and database policy state");
            }
        }

        const UInt64 previous_leases = udt_restore_publication_leases.fetch_add(1, std::memory_order_acq_rel);
        if (previous_leases >= maximum_concurrent_udt_restore_publication_leases)
        {
            static_cast<void>(udt_restore_publication_leases.fetch_sub(1, std::memory_order_acq_rel));
            throw Exception(
                ErrorCodes::ABORTED,
                "Too many concurrent Atomic RESTORE publications are in flight (maximum {})",
                maximum_concurrent_udt_restore_publication_leases);
        }
        owns_restore_publication_lease = true;
    }

    SCOPE_EXIT({
        if (owns_restore_publication_lease)
        {
            const UInt64 previous_leases = udt_restore_publication_leases.fetch_sub(1, std::memory_order_acq_rel);
            if (!previous_leases)
                std::terminate();
        }
    });
    DatabaseOrdinary::createTableRestoredFromBackup(
        create_table_query, std::move(local_context), std::move(restore_coordination), timeout_ms);
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

void DatabaseAtomic::markUDTTableStartupSucceeded(UUID table_uuid, std::string_view table_name, const StoragePtr & table)
{
    const auto current_database_name = getDatabaseName();
    std::lock_guard authority_lock(udt_authority_mutex);
    if (udt_authority_shutdown)
        return;
    if (!udt_table_startup_state)
        return;
    auto * entry = udt_table_startup_state->findExactEntry(table_uuid, table_name);
    if (entry)
        udt_table_startup_state->markStarted(*entry, table, current_database_name);
}

void DatabaseAtomic::onTableStartupCompleted(const QualifiedTableName & name, const StoragePtr & table)
{
    {
        std::lock_guard authority_lock(udt_authority_mutex);
        if (udt_authority_shutdown || !udt_table_startup_state || !udt_table_startup_state->by_name.contains(name.table))
            return;
    }

    std::lock_guard schema_mutation_lock(udt_schema_mutation_mutex);
    const auto table_id = table->getStorageID();
    markUDTTableStartupSucceeded(table_id.uuid, name.table, table);
}

void DatabaseAtomic::handlePendingUDTTableLoadOrStartupFailure(
    const QualifiedTableName & name, AsyncTableLoadingFailurePhase phase, std::exception_ptr failure)
{
    switch (phase)
    {
        case AsyncTableLoadingFailurePhase::LoadExecution:
        case AsyncTableLoadingFailurePhase::LoadDependency:
        case AsyncTableLoadingFailurePhase::StartupExecution:
        case AsyncTableLoadingFailurePhase::StartupDependency: break;
    }

    /// This callback runs only after an engine load/startup job or one of its
    /// dependencies failed. Such a failure says nothing about the canonical UDT
    /// image. Exact binding corruption transitions synchronously inside the
    /// schema-serialized Atomic attach validator before it can reach this hook.
    /// Therefore every phase here keeps the private root retryable.
    std::lock_guard authority_lock(udt_authority_mutex);
    if (!udt_authority_shutdown && udt_table_startup_state && udt_table_startup_state->by_name.contains(name.table)
        && !udt_table_startup_state->retryable_failure)
    {
        udt_table_startup_state->retryable_failure = std::move(failure);
    }
}

void DatabaseAtomic::onAsyncTableLoadingFailed(
    const QualifiedTableName & name, AsyncTableLoadingFailurePhase phase, std::exception_ptr failure)
{
    handlePendingUDTTableLoadOrStartupFailure(name, phase, std::move(failure));
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
        /// not_in_use destroyed here (after lock released) — StoragePtrs freed
        /// without holding mutex

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
            LOG_INFO(
                log,
                "Still waiting for detached table {} to be no longer in use "
                "(use_count={}, elapsed ~{}s)",
                toString(uuid),
                use_count,
                iterations / 10);

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

    throw Exception(
        ErrorCodes::UNFINISHED,
        "Did not finish waiting for detached table {} to be no longer in use "
        "because the server is shutting down",
        uuid);
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
