#include <Databases/UDT/AuthorityAutomaticRepair.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Interpreters/DatabaseCatalog.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::UDT
{

struct OwnedAuditObservations
{
    std::vector<String> bytes;
    std::vector<AuthorityRepairObservation> observations;
};

struct OwnedRepairCandidates
{
    std::vector<String> bytes;
    std::vector<String> references;
    std::vector<AuthorityRepairCandidate> candidates;
};

class AuthorityAutomaticRepairContinuation::Impl final
{
public:
    enum class SourcePhase : UInt8
    {
        NotStarted,
        LocalWAL,
        PrepareReplicatedAuthority,
        ReplicatedAuthority,
        PrepareVerifiedBackup,
        VerifiedBackup,
        OrderCandidates,
        Complete,
    };

    void reset(const AuthorityRootGraphIdentity & root_, const AuthorityQuarantinePlan::Ptr & quarantine_)
    {
        root = root_;
        quarantine = quarantine_;
        preflight_leaf = 0;
        observation_count = 0;
        byte_observation_count = 0;
        observation_control_bytes = 0;
        audit_buffers_ready = false;
        observation_leaf = 0;
        observed_bytes = 0;
        observations = {};
        audit_build.reset();
        completed_audit.reset();
        audit_manifest.reset();
        repair_targets = std::vector<AuthorityRepairTarget>{};
        plan_build.reset();
        repair_plan.reset();
        candidates = {};
        ordered_candidates = {};
        selected_targets = std::vector<bool>{};
        candidate_by_target = std::vector<size_t>{};
        ordering_target_index = 0;
        candidate_input_bytes = 0;
        candidate_retained_plan_bytes = 0;
        source_control_bytes = 0;
        wal_discovery.reset();
        wal_transaction_ids = std::vector<UInt64>{};
        wal_transaction_index = 0;
        wal_transactions_examined = 0;
        wal_artifacts_examined = 0;
        wal_bytes_examined = 0;
        source_phase = SourcePhase::NotStarted;
        external_missing_targets = std::vector<AuthorityRepairTarget>{};
        missing_scan_index = 0;
        external_target_offset = 0;
    }

    void clear() noexcept
    {
        root.reset();
        quarantine.reset();
        preflight_leaf = 0;
        observation_count = 0;
        byte_observation_count = 0;
        observation_control_bytes = 0;
        audit_buffers_ready = false;
        observation_leaf = 0;
        observed_bytes = 0;
        observations = {};
        audit_build.reset();
        completed_audit.reset();
        audit_manifest.reset();
        repair_targets = std::vector<AuthorityRepairTarget>{};
        plan_build.reset();
        repair_plan.reset();
        candidates = {};
        ordered_candidates = {};
        selected_targets = std::vector<bool>{};
        candidate_by_target = std::vector<size_t>{};
        ordering_target_index = 0;
        candidate_input_bytes = 0;
        candidate_retained_plan_bytes = 0;
        source_control_bytes = 0;
        wal_discovery.reset();
        wal_transaction_ids = std::vector<UInt64>{};
        wal_transaction_index = 0;
        wal_transactions_examined = 0;
        wal_artifacts_examined = 0;
        wal_bytes_examined = 0;
        source_phase = SourcePhase::NotStarted;
        external_missing_targets = std::vector<AuthorityRepairTarget>{};
        missing_scan_index = 0;
        external_target_offset = 0;
    }

    std::mutex mutex;
    std::optional<AuthorityRootGraphIdentity> root;
    AuthorityQuarantinePlan::Ptr quarantine;
    size_t preflight_leaf = 0;
    UInt64 observation_count = 0;
    UInt64 byte_observation_count = 0;
    UInt64 observation_control_bytes = 0;
    bool audit_buffers_ready = false;
    size_t observation_leaf = 0;
    UInt64 observed_bytes = 0;
    OwnedAuditObservations observations;
    std::unique_ptr<AuthorityRepairAuditBuildContinuation> audit_build;
    AuthorityRepairAudit::Ptr completed_audit;
    std::optional<Digest> audit_manifest;
    std::vector<AuthorityRepairTarget> repair_targets;
    std::unique_ptr<AuthorityRepairPlanBuildContinuation> plan_build;
    AuthorityRepairPlan::Ptr repair_plan;
    OwnedRepairCandidates candidates;
    OwnedRepairCandidates ordered_candidates;
    std::vector<bool> selected_targets;
    std::vector<size_t> candidate_by_target;
    size_t ordering_target_index = 0;
    UInt64 candidate_input_bytes = 0;
    UInt64 candidate_retained_plan_bytes = 0;
    UInt64 source_control_bytes = 0;
    std::shared_ptr<AtomicDatabaseSchemaMutationDurableTransactionDiscovery> wal_discovery;
    std::vector<UInt64> wal_transaction_ids;
    size_t wal_transaction_index = 0;
    UInt64 wal_transactions_examined = 0;
    UInt64 wal_artifacts_examined = 0;
    UInt64 wal_bytes_examined = 0;
    SourcePhase source_phase = SourcePhase::NotStarted;
    std::vector<AuthorityRepairTarget> external_missing_targets;
    size_t missing_scan_index = 0;
    size_t external_target_offset = 0;
};

AuthorityAutomaticRepairContinuation::AuthorityAutomaticRepairContinuation()
    : impl(std::make_unique<Impl>())
{
}

AuthorityAutomaticRepairContinuation::~AuthorityAutomaticRepairContinuation() = default;

class AuthorityAutomaticRepairAccess final
{
public:
    static AtomicDatabaseSchemaMutationStorage::RepairAuditTargetRead readTarget(
        AtomicDatabaseSchemaMutationStorage & storage,
        const AuthorityRoot & root,
        const AuthorityInventoryLeaf & leaf,
        UInt64 maximum_retained_bytes)
    {
        return storage.readAuthorityRepairAuditTarget(root, leaf, maximum_retained_bytes);
    }

    static std::optional<std::vector<UInt64>> resumeWALDiscovery(
        AtomicDatabaseSchemaMutationStorage & storage,
        AtomicDatabaseSchemaMutationDurableTransactionDiscovery & continuation,
        const AuthorityRootGraphIdentity & root,
        UInt64 maximum_transactions,
        UInt64 maximum_control_bytes,
        const AuthorityVerificationPassBudget & pass_budget)
    {
        return storage.resumeDurableTransactionIDDiscoveryForAuthorityRepair(
            continuation, root, maximum_transactions, maximum_control_bytes, pass_budget);
    }

    static std::optional<AtomicDatabaseSchemaMutationRecoveryTransaction> loadCommittedWALTransaction(
        AtomicDatabaseSchemaMutationStorage & storage,
        UInt64 transaction_id,
        UInt64 maximum_total_staged_artifact_bytes,
        UInt64 maximum_staged_artifacts,
        UInt64 maximum_control_bytes)
    {
        return storage.loadCommittedTransactionForAuthorityRepair(
            transaction_id, maximum_total_staged_artifact_bytes, maximum_staged_artifacts, maximum_control_bytes);
    }

    static bool isExactlyTemporarilyDetached(DB::DatabaseAtomic & database, const SchemaObjectID & object, std::string_view object_name)
        TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        /// attempt retains the concrete database schema lock throughout audit;
        /// this narrow friend seam only exposes the lock-required exact check.
        return database.isExactTemporarilyDetachedUDTObject(object, object_name);
    }
};

namespace
{

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        throw AuthorityRepairPlanError(AuthorityRepairPlanError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        throw AuthorityRepairPlanError(AuthorityRepairPlanError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 inventory_key_canonical_bytes = sizeof(UInt16) + sizeof(UInt8) + canonical_uuid_bytes;
constexpr UInt64 target_view_base_canonical_bytes
    = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 candidate_view_base_canonical_bytes = 2 * sizeof(UInt8) + schema_object_identity_canonical_bytes
    + inventory_key_canonical_bytes + sizeof(UInt64) + sizeof(Digest) + 2 * sizeof(UInt64);
constexpr UInt64 selection_base_canonical_bytes = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes
    + sizeof(UInt64) + 2 * sizeof(Digest) + sizeof(UInt8) + 2 * sizeof(UInt64);
constexpr UInt64 repair_plan_base_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + 3 * sizeof(Digest) + 2 * sizeof(UInt64);
constexpr UInt64 minimum_wal_source_in_flight_control_bytes = database_schema_wal_prepare_minimum_decode_control_bytes
    + sizeof(AtomicDatabaseSchemaMutationRecoveryTransaction) - sizeof(DatabaseSchemaWALPrepare) + sizeof(String);

UInt64 fixedPlanInputBytes(UInt64 target_count)
{
    return checkedMultiply(target_count, target_view_base_canonical_bytes, "automatic repair target input bytes overflow UInt64");
}

UInt64 fixedPlanRetainedBytes(UInt64 target_count)
{
    return checkedAdd(
        repair_plan_base_canonical_bytes,
        checkedMultiply(target_count, selection_base_canonical_bytes, "automatic repair retained plan bytes overflow UInt64"),
        "automatic repair retained plan bytes overflow UInt64");
}

UInt64 sourceControlReservation(UInt64 target_count, UInt64 maximum_wal_transactions)
{
    constexpr UInt64 candidate_bundle_item_bytes = 2 * sizeof(String) + sizeof(AuthorityRepairCandidate);
    const UInt64 per_target = checkedAdd(
        checkedMultiply(2, sizeof(AuthorityRepairTarget), "automatic repair source controls overflow UInt64"),
        checkedAdd(
            checkedMultiply(2, candidate_bundle_item_bytes, "automatic repair source controls overflow UInt64"),
            sizeof(UInt8) + sizeof(size_t),
            "automatic repair source controls overflow UInt64"),
        "automatic repair source controls overflow UInt64");
    return checkedAdd(
        checkedMultiply(target_count, per_target, "automatic repair source controls overflow UInt64"),
        checkedAdd(
            atomic_database_schema_mutation_discovery_fixed_control_bytes,
            checkedMultiply(
                maximum_wal_transactions,
                atomic_database_schema_mutation_discovery_control_bytes_per_transaction,
                "automatic repair WAL discovery controls overflow UInt64"),
            "automatic repair WAL discovery controls overflow UInt64"),
        "automatic repair source controls overflow UInt64");
}

void checkRepairRunBudget(const AuthorityAutomaticRepairLimits & limits, std::string_view operation)
{
    if (limits.cancellation.stop_requested())
        throw AuthorityRepairAuditError(AuthorityRepairAuditError::Code::ExecutionBudgetExceeded, String(operation) + " was cancelled");
    if (limits.monotonic_deadline && std::chrono::steady_clock::now() >= *limits.monotonic_deadline)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::ExecutionBudgetExceeded, String(operation) + " exceeded its wall-time budget");
    if (limits.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *limits.thread_cpu_deadline_nanoseconds)
    {
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::ExecutionBudgetExceeded, String(operation) + " exceeded its CPU-time budget");
    }
}

template <typename T>
std::optional<T> earlierDeadline(std::optional<T> lhs, std::optional<T> rhs)
{
    if (!lhs)
        return rhs;
    if (!rhs)
        return lhs;
    return std::min(*lhs, *rhs);
}

AuthorityExactRepairLimits effectiveExecutionLimits(const AuthorityAutomaticRepairLimits & limits)
{
    auto result = limits.execution;
    if (limits.cancellation.stop_possible())
        result.verification_executor.cancellation = limits.cancellation;
    result.verification_executor.monotonic_deadline
        = earlierDeadline(result.verification_executor.monotonic_deadline, limits.monotonic_deadline);
    result.verification_executor.thread_cpu_deadline_nanoseconds
        = earlierDeadline(result.verification_executor.thread_cpu_deadline_nanoseconds, limits.thread_cpu_deadline_nanoseconds);
    return result;
}

bool isOrdinaryDependentObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

bool storageMatchesObjectKind(const IStorage & storage, SchemaObjectKind kind) noexcept
{
    if (kind == SchemaObjectKind::Table)
        return !storage.isView() && !storage.isDictionary();
    if (kind == SchemaObjectKind::View)
        return storage.isView();
    if (kind == SchemaObjectKind::Dictionary)
        return storage.isDictionary();
    return false;
}

void appendBytesObservation(
    OwnedAuditObservations & output, AuthorityRepairAuditArtifactKind kind, const AuthorityInventoryKey & key, std::optional<String> bytes)
{
    AuthorityRepairObservation observation{
        .artifact_kind = kind,
        .authority_key = key,
        .state = bytes ? AuthorityRepairObservationState::Present : AuthorityRepairObservationState::Missing,
        .artifact_bytes = {},
        .object = {},
        .object_schema_revision = 0,
        .physical_schema_fingerprint = {},
    };
    if (bytes)
    {
        output.bytes.push_back(std::move(*bytes));
        observation.artifact_bytes = output.bytes.back();
    }
    output.observations.push_back(std::move(observation));
}

AuthorityRepairObservation observeLiveObject(
    DB::DatabaseAtomic & database,
    const SidecarExpectationRecord & expectation,
    const AuthorityInventoryKey & key,
    const AtomicDatabaseSchemaMutationStorage::RepairAuditTargetRead & durable)
{
    AuthorityRepairObservation result{
        .artifact_kind = AuthorityRepairAuditArtifactKind::StoredObjectImage,
        .authority_key = key,
        .state = AuthorityRepairObservationState::Missing,
        .artifact_bytes = {},
        .object = {},
        .object_schema_revision = 0,
        .physical_schema_fingerprint = {},
    };

    if (isOrdinaryDependentObjectKind(expectation.object.kind))
    {
        const auto [mapped_database, table] = DatabaseCatalog::instance().tryGetByUUID(expectation.object.object_uuid);
        if (mapped_database && mapped_database.get() == std::addressof(database) && table
            && storageMatchesObjectKind(*table, expectation.object.kind))
        {
            try
            {
                const auto storage_id = table->getStorageID();
                if (storage_id.uuid != expectation.object.object_uuid || storage_id.database_name != database.getDatabaseName()
                    || storage_id.table_name.empty())
                    return result;
                const auto metadata = table->getInMemoryMetadataPtr(nullptr, false);
                if (!metadata)
                    return result;
                metadata->validateBoundUDTReferences();
                const auto & references = metadata->getBoundUDTReferences();
                if (!references)
                    return result;
                result.state = AuthorityRepairObservationState::Present;
                result.object = references->getObject();
                result.object_schema_revision = references->getObjectSchemaRevision();
                result.physical_schema_fingerprint = references->getPhysicalSchemaFingerprint();
            }
            catch (const TableColumnTypeBindingError &)
            {
                /// Invalid live provenance is an independently observed
                /// StoredObjectImage mismatch. It must become an explicit,
                /// unrepairable audit finding rather than aborting the audit
                /// before the exact quarantine closure can be re-anchored.
            }
            return result;
        }

        if (!durable.installation_record_bytes)
            return result;
        try
        {
            const auto installation = decodeDependentObjectMetadataInstallationRecord(*durable.installation_record_bytes);
            if (installation.object != expectation.object || installation.object_schema_revision != expectation.object_schema_revision
                || !expectation.installation_record_hash
                || computeDependentObjectMetadataInstallationRecordHash(installation) != *expectation.installation_record_hash
                || !AuthorityAutomaticRepairAccess::isExactlyTemporarilyDetached(database, expectation.object, installation.object_name))
                return result;
            result.state = AuthorityRepairObservationState::Present;
            result.object = expectation.object;
            result.object_schema_revision = expectation.object_schema_revision;
            result.physical_schema_fingerprint = expectation.physical_schema_fingerprint;
            return result;
        }
        catch (const DependentObjectMetadataInstallationRecordError &)
        {
            return result;
        }
    }

    if (expectation.object.kind != SchemaObjectKind::SyntheticTestObject || !durable.metadata_bytes || !durable.persisted_references_bytes)
        return result;
    try
    {
        const auto validated
            = validateSyntheticDependentObjectMetadata(expectation, *durable.metadata_bytes, *durable.persisted_references_bytes);
        result.state = AuthorityRepairObservationState::Present;
        result.object = validated.object;
        result.object_schema_revision = validated.object_schema_revision;
        result.physical_schema_fingerprint = validated.physical_schema_fingerprint;
    }
    catch (const SyntheticObjectMetadataError &)
    {
    }
    return result;
}

const OwnedAuditObservations & collectAuditObservations(
    DB::DatabaseAtomic & database,
    AtomicDatabaseSchemaMutationStorage & storage,
    const AuthorityRoot & root,
    const AuthorityAutomaticRepairLimits & limits,
    AuthorityAutomaticRepairContinuation::Impl & progress)
{
    const auto inventory = root.pinAuthorityInventory();
    if (!inventory || inventory->getSummary() != root.getInventorySummary())
        throw AuthorityRepairAuditError(AuthorityRepairAuditError::Code::InvalidRoot, "automatic repair cannot pin its inventory");

    const UInt64 leaf_count = inventory->getSummary().leaf_count;
    if (toUInt64(progress.preflight_leaf) > leaf_count || toUInt64(progress.observation_leaf) > leaf_count)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::InvalidRoot, "automatic repair audit continuation has an invalid leaf offset");
    while (toUInt64(progress.preflight_leaf) < leaf_count)
    {
        checkRepairRunBudget(limits, "automatic repair audit preflight");
        const auto * leaf_ptr = inventory->getLeafByCanonicalIndex(toUInt64(progress.preflight_leaf));
        if (!leaf_ptr)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::InventoryMismatch,
                "automatic repair audit cannot resume its canonical inventory preflight");
        const auto & leaf = *leaf_ptr;
        UInt64 leaf_observations = 1;
        UInt64 leaf_byte_observations = 1;
        if (leaf.key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation)
        {
            const auto * expectation = root.findExpectationRecord(leaf.key.object_uuid);
            if (!expectation || expectation->object.object_uuid != leaf.key.object_uuid
                || expectation->object_schema_revision != leaf.object_revision)
                throw AuthorityRepairAuditError(
                    AuthorityRepairAuditError::Code::RecordStoreMismatch, "automatic repair expectation differs from its inventory leaf");
            leaf_observations = expectation->installation_record_hash ? 5 : 3;
            leaf_byte_observations = expectation->installation_record_hash ? 4 : 2;
        }
        if (progress.observation_count > limits.audit.maximum_observations
            || leaf_observations > limits.audit.maximum_observations - progress.observation_count)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::LimitExceeded,
                "automatic repair audit observation set exceeds its limit before storage reads");
        progress.observation_count += leaf_observations;
        progress.byte_observation_count = checkedAdd(
            progress.byte_observation_count, leaf_byte_observations, "automatic repair audit byte-observation count overflows UInt64");
        ++progress.preflight_leaf;
    }
    if (progress.observation_count > limits.audit.maximum_observations)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::LimitExceeded,
            "automatic repair audit continuation exceeds its current observation-count limit");
    if (!std::in_range<size_t>(progress.observation_count) || !std::in_range<size_t>(progress.byte_observation_count))
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::LimitExceeded, "automatic repair audit observation set exceeds size_t");
    if (progress.observation_count > std::numeric_limits<UInt64>::max() / sizeof(AuthorityRepairObservation)
        || progress.byte_observation_count > std::numeric_limits<UInt64>::max() / sizeof(String))
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::ArithmeticOverflow, "automatic repair audit container charge overflows UInt64");
    const UInt64 observation_control_bytes = progress.observation_count * sizeof(AuthorityRepairObservation);
    const UInt64 string_control_bytes = progress.byte_observation_count * sizeof(String);
    if (string_control_bytes > std::numeric_limits<UInt64>::max() - observation_control_bytes)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::ArithmeticOverflow, "automatic repair audit control charge overflows UInt64");
    progress.observation_control_bytes = observation_control_bytes + string_control_bytes;
    if (progress.observation_control_bytes > limits.maximum_audit_input_retained_bytes)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::LimitExceeded,
            "automatic repair audit containers exceed the retained-input limit before allocation");
    if (progress.observed_bytes > limits.audit.maximum_total_observed_bytes
        || progress.observed_bytes > limits.maximum_audit_input_retained_bytes - progress.observation_control_bytes)
    {
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::LimitExceeded, "automatic repair retained audit continuation exceeds its current byte limits");
    }

    if (!progress.audit_buffers_ready)
    {
        checkRepairRunBudget(limits, "automatic repair audit buffer allocation");
        progress.observations.bytes.reserve(static_cast<size_t>(progress.byte_observation_count));
        progress.observations.observations.reserve(static_cast<size_t>(progress.observation_count));
        progress.audit_buffers_ready = true;
        checkRepairRunBudget(limits, "automatic repair audit buffer allocation");
    }
    while (toUInt64(progress.observation_leaf) < leaf_count)
    {
        checkRepairRunBudget(limits, "automatic repair audit storage scan");
        if (progress.observed_bytes > limits.audit.maximum_total_observed_bytes
            || progress.observed_bytes > limits.maximum_audit_input_retained_bytes - progress.observation_control_bytes)
        {
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::LimitExceeded,
                "automatic repair retained audit continuation exceeds its current byte limits");
        }
        const auto * leaf_ptr = inventory->getLeafByCanonicalIndex(toUInt64(progress.observation_leaf));
        if (!leaf_ptr)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::InventoryMismatch, "automatic repair audit cannot resume its canonical inventory scan");
        const auto & leaf = *leaf_ptr;
        const UInt64 retained_remaining
            = limits.maximum_audit_input_retained_bytes - progress.observation_control_bytes - progress.observed_bytes;
        const UInt64 observed_remaining = limits.audit.maximum_total_observed_bytes - progress.observed_bytes;
        auto durable = AuthorityAutomaticRepairAccess::readTarget(storage, root, leaf, std::min(retained_remaining, observed_remaining));
        UInt64 target_bytes = 0;
        for (const auto * bytes :
             {std::addressof(durable.authority_record_bytes),
              std::addressof(durable.installation_record_bytes),
              std::addressof(durable.persisted_references_bytes),
              std::addressof(durable.metadata_bytes)})
        {
            if (*bytes)
                target_bytes = checkedAdd(target_bytes, toUInt64((*bytes)->size()), "automatic repair audit input bytes overflow UInt64");
        }
        if (target_bytes > retained_remaining || target_bytes > observed_remaining)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::LimitExceeded,
                "automatic repair audit target exceeds its prospective aggregate byte limits");
        progress.observed_bytes += target_bytes;
        if (leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
        {
            appendBytesObservation(
                progress.observations,
                AuthorityRepairAuditArtifactKind::DefinitionRecord,
                leaf.key,
                std::move(durable.authority_record_bytes));
            ++progress.observation_leaf;
            checkRepairRunBudget(limits, "automatic repair audit storage scan");
            continue;
        }

        const auto * expectation = root.findExpectationRecord(leaf.key.object_uuid);
        if (!expectation || expectation->object_schema_revision != leaf.object_revision)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::RecordStoreMismatch, "automatic repair expectation differs from its inventory leaf");
        auto live_object = observeLiveObject(database, *expectation, leaf.key, durable);
        appendBytesObservation(
            progress.observations,
            AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
            leaf.key,
            std::move(durable.authority_record_bytes));
        appendBytesObservation(
            progress.observations,
            AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
            leaf.key,
            std::move(durable.persisted_references_bytes));
        if (expectation->installation_record_hash)
        {
            appendBytesObservation(
                progress.observations,
                AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
                leaf.key,
                std::move(durable.installation_record_bytes));
            appendBytesObservation(
                progress.observations,
                AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
                leaf.key,
                std::move(durable.metadata_bytes));
        }
        progress.observations.observations.push_back(std::move(live_object));
        ++progress.observation_leaf;
        checkRepairRunBudget(limits, "automatic repair audit storage scan");
    }
    if (progress.observations.observations.size() != progress.observation_count
        || progress.observations.bytes.size() > progress.byte_observation_count)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::NonCanonicalObservationSet,
            "automatic repair audit collection did not materialize its exact preflight set");
    return progress.observations;
}

bool artifactIdentityLess(
    const AuthorityInventoryKey & lhs_key,
    AuthorityRepairArtifactKind lhs_kind,
    const AuthorityInventoryKey & rhs_key,
    AuthorityRepairArtifactKind rhs_kind) noexcept
{
    if (authorityInventoryKeyLess(lhs_key, rhs_key))
        return true;
    if (authorityInventoryKeyLess(rhs_key, lhs_key))
        return false;
    return static_cast<UInt8>(lhs_kind) < static_cast<UInt8>(rhs_kind);
}

std::optional<AuthorityRepairArtifactKind> toRepairArtifactKind(DatabaseSchemaWALStagedArtifactKind kind) noexcept
{
    switch (kind)
    {
        case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord: return AuthorityRepairArtifactKind::DefinitionRecord;
        case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord: return AuthorityRepairArtifactKind::SidecarExpectationRecord;
        case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar:
            return AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar;
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata:
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord: return std::nullopt;
    }
    return std::nullopt;
}

bool exactCandidateBytes(const AuthorityRepairTarget & target, DatabaseSchemaWALStagedArtifactKind kind, std::string_view bytes)
{
    try
    {
        switch (target.artifact_kind)
        {
            case AuthorityRepairArtifactKind::DefinitionRecord: {
                if (kind != DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord)
                    return false;
                const auto record = decodeRecord(bytes);
                return record.identity.database_uuid == target.object.database_uuid
                    && record.identity.type_uuid == target.object.object_uuid && record.identity.revision == target.object_revision
                    && computeRecordHash(record) == target.expected_canonical_hash;
            }
            case AuthorityRepairArtifactKind::SidecarExpectationRecord: {
                if (kind != DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord)
                    return false;
                const auto expectation = decodeSidecarExpectationRecord(bytes);
                return expectation.object == target.object && expectation.object_schema_revision == target.object_revision
                    && expectation.physical_schema_fingerprint == target.physical_schema_fingerprint
                    && computeSidecarExpectationRecordHash(expectation) == target.expected_canonical_hash;
            }
            case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: {
                if (kind != DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar)
                    return false;
                const auto references = decodePersistedTypeReferences(bytes);
                return references.object == target.object && references.object_schema_revision == target.object_revision
                    && references.physical_schema_fingerprint == target.physical_schema_fingerprint
                    && computePersistedTypeReferencesSidecarHash(references) == target.expected_canonical_hash;
            }
        }
    }
    catch (const RecordError &)
    {
    }
    catch (const SidecarExpectationRecordError &)
    {
    }
    catch (const PersistedTypeReferencesError &)
    {
    }
    return false;
}

[[maybe_unused]] bool repairCandidateLess(const AuthorityRepairCandidate & lhs, const AuthorityRepairCandidate & rhs) noexcept
{
    if (artifactIdentityLess(lhs.authority_key, lhs.artifact_kind, rhs.authority_key, rhs.artifact_kind))
        return true;
    if (artifactIdentityLess(rhs.authority_key, rhs.artifact_kind, lhs.authority_key, lhs.artifact_kind))
        return false;
    return static_cast<UInt8>(lhs.source) < static_cast<UInt8>(rhs.source);
}

[[maybe_unused]] void sortRepairCandidates(OwnedRepairCandidates & candidates)
{
    std::sort(candidates.candidates.begin(), candidates.candidates.end(), repairCandidateLess);
}

struct TargetMatch
{
    const AuthorityRepairTarget * target = nullptr;
    size_t index = 0;
};

std::optional<TargetMatch>
findTarget(std::span<const AuthorityRepairTarget> targets, const AuthorityInventoryKey & key, AuthorityRepairArtifactKind kind)
{
    const auto it = std::lower_bound(
        targets.begin(),
        targets.end(),
        std::pair{key, kind},
        [](const AuthorityRepairTarget & target, const auto & identity)
        { return artifactIdentityLess(target.authority_key, target.artifact_kind, identity.first, identity.second); });
    if (it == targets.end() || it->authority_key != key || it->artifact_kind != kind)
        return std::nullopt;
    return TargetMatch{.target = std::addressof(*it), .index = static_cast<size_t>(it - targets.begin())};
}

void resumeLocalWALCandidates(
    AtomicDatabaseSchemaMutationStorage & storage,
    const AuthorityRootGraphIdentity & root,
    std::span<const AuthorityRepairTarget> targets,
    const AuthorityAutomaticRepairLimits & limits,
    AuthorityAutomaticRepairContinuation::Impl & progress)
{
    using SourcePhase = AuthorityAutomaticRepairContinuation::Impl::SourcePhase;
    if (progress.source_phase == SourcePhase::NotStarted)
    {
        const UInt64 target_count = toUInt64(targets.size());
        if (target_count == 0 || target_count > limits.plan.maximum_targets || target_count > limits.plan.maximum_candidates
            || progress.candidate_input_bytes != fixedPlanInputBytes(target_count)
            || progress.candidate_retained_plan_bytes != fixedPlanRetainedBytes(target_count)
            || progress.source_control_bytes != sourceControlReservation(target_count, limits.maximum_local_wal_transactions)
            || progress.source_control_bytes > limits.maximum_source_control_bytes)
        {
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded,
                "automatic repair source state was not prospectively admitted before allocation");
        }
        if (!progress.wal_discovery)
        {
            progress.candidates.bytes.reserve(targets.size());
            progress.candidates.references.reserve(targets.size());
            progress.candidates.candidates.reserve(targets.size());
            progress.selected_targets.assign(targets.size(), false);
            progress.candidate_by_target.assign(targets.size(), std::numeric_limits<size_t>::max());
            progress.wal_discovery = std::make_shared<AtomicDatabaseSchemaMutationDurableTransactionDiscovery>();
        }
        const UInt64 wal_discovery_controls = checkedAdd(
            atomic_database_schema_mutation_discovery_fixed_control_bytes,
            checkedMultiply(
                limits.maximum_local_wal_transactions,
                atomic_database_schema_mutation_discovery_control_bytes_per_transaction,
                "automatic repair WAL discovery controls overflow UInt64"),
            "automatic repair WAL discovery controls overflow UInt64");
        if (wal_discovery_controls > progress.source_control_bytes)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::ArithmeticOverflow,
                "automatic repair WAL discovery controls exceed their admitted source state");
        const UInt64 retained_controls_without_wal_discovery = progress.source_control_bytes - wal_discovery_controls;
        const AuthorityVerificationPassBudget discovery_budget{
            .cancellation = limits.cancellation,
            .monotonic_deadline = limits.monotonic_deadline,
            .thread_cpu_deadline_nanoseconds = limits.thread_cpu_deadline_nanoseconds,
            .maximum_work_items = limits.maximum_local_wal_discovery_work_items_per_pass,
        };
        std::optional<std::vector<UInt64>> transaction_ids;
        while (!transaction_ids)
        {
            checkRepairRunBudget(limits, "automatic repair WAL namespace scan");
            transaction_ids = AuthorityAutomaticRepairAccess::resumeWALDiscovery(
                storage,
                *progress.wal_discovery,
                root,
                limits.maximum_local_wal_transactions,
                limits.maximum_source_control_bytes - retained_controls_without_wal_discovery,
                discovery_budget);
        }
        progress.wal_transaction_ids = std::move(*transaction_ids);
        progress.wal_discovery.reset();
        if (progress.wal_transaction_ids.capacity() > limits.maximum_local_wal_transactions)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair WAL namespace exceeded its admitted control capacity");
        progress.source_phase = SourcePhase::LocalWAL;
        checkRepairRunBudget(limits, "automatic repair WAL namespace scan");
    }
    if (progress.source_phase != SourcePhase::LocalWAL)
        return;
    if (progress.wal_transactions_examined > limits.maximum_local_wal_transactions
        || progress.wal_artifacts_examined > limits.maximum_local_wal_artifacts_examined
        || progress.wal_bytes_examined > limits.maximum_local_wal_bytes_examined
        || progress.candidate_input_bytes > limits.plan.maximum_total_input_bytes
        || progress.candidate_retained_plan_bytes > limits.plan.maximum_retained_plan_bytes
        || progress.source_control_bytes > limits.maximum_source_control_bytes)
    {
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair retained WAL continuation exceeds its current limits");
    }

    while (progress.wal_transaction_index < progress.wal_transaction_ids.size())
    {
        checkRepairRunBudget(limits, "automatic repair WAL scan");
        if (progress.wal_transactions_examined >= limits.maximum_local_wal_transactions)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded,
                "automatic repair WAL continuation exceeds its prospective transaction-count limit");
        const UInt64 transaction_id
            = progress.wal_transaction_ids[progress.wal_transaction_ids.size() - 1 - progress.wal_transaction_index];
        if (progress.wal_artifacts_examined >= limits.maximum_local_wal_artifacts_examined
            || progress.wal_bytes_examined >= limits.maximum_local_wal_bytes_examined
            || progress.source_control_bytes >= limits.maximum_source_control_bytes)
        {
            progress.wal_transaction_index = progress.wal_transaction_ids.size();
            break;
        }
        auto transaction = AuthorityAutomaticRepairAccess::loadCommittedWALTransaction(
            storage,
            transaction_id,
            limits.maximum_local_wal_bytes_examined - progress.wal_bytes_examined,
            limits.maximum_local_wal_artifacts_examined - progress.wal_artifacts_examined,
            limits.maximum_source_control_bytes - progress.source_control_bytes);
        ++progress.wal_transactions_examined;
        ++progress.wal_transaction_index;
        if (!transaction)
        {
            checkRepairRunBudget(limits, "automatic repair WAL scan");
            continue;
        }
        progress.wal_artifacts_examined = checkedAdd(
            progress.wal_artifacts_examined,
            toUInt64(transaction->prepare.staged_artifacts.size()),
            "automatic repair WAL artifact count overflows UInt64");
        if (progress.wal_artifacts_examined > limits.maximum_local_wal_artifacts_examined)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair WAL scan exceeds its artifact limit");
        for (const auto & bytes : transaction->staged_artifact_bytes)
        {
            progress.wal_bytes_examined
                = checkedAdd(progress.wal_bytes_examined, toUInt64(bytes.size()), "automatic repair WAL byte count overflows UInt64");
            if (progress.wal_bytes_examined > limits.maximum_local_wal_bytes_examined)
                throw AuthorityRepairPlanError(
                    AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair WAL scan exceeds its byte limit");
        }
        if (transaction->commit && transaction->recovery_decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
        {
            if (transaction->prepare.staged_artifacts.size() != transaction->staged_artifact_bytes.size())
                throw AuthorityRepairPlanError(
                    AuthorityRepairPlanError::Code::InvalidCandidateSet,
                    "automatic repair WAL transaction has an incomplete staged manifest");
            for (size_t ordinal = 0; ordinal < transaction->prepare.staged_artifacts.size(); ++ordinal)
            {
                const auto & ref = transaction->prepare.staged_artifacts[ordinal];
                const auto kind = toRepairArtifactKind(ref.kind);
                if (!kind)
                    continue;
                const AuthorityInventoryKey key{
                    .record_kind = *kind == AuthorityRepairArtifactKind::DefinitionRecord
                        ? AuthorityInventoryRecordKind::TypeDefinition
                        : AuthorityInventoryRecordKind::SidecarExpectation,
                    .object_uuid = ref.object.object_uuid,
                };
                const auto match = findTarget(targets, key, *kind);
                if (!match || progress.selected_targets[match->index] || match->target->object != ref.object
                    || match->target->object_revision != ref.revision)
                    continue;
                auto & bytes = transaction->staged_artifact_bytes[ordinal];
                if (!exactCandidateBytes(*match->target, ref.kind, bytes))
                    continue;
                String source_reference = "local-schema-wal/" + std::to_string(transaction_id) + "/" + std::to_string(ordinal);
                if (source_reference.size() > limits.plan.maximum_source_reference_bytes)
                    throw AuthorityRepairPlanError(
                        AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair WAL source reference exceeds its byte limit");
                const UInt64 retained = checkedAdd(
                    toUInt64(bytes.size()), toUInt64(source_reference.size()), "automatic repair WAL candidate bytes overflow UInt64");
                const UInt64 next_input = checkedAdd(
                    progress.candidate_input_bytes,
                    checkedAdd(candidate_view_base_canonical_bytes, retained, "automatic repair candidate input bytes overflow UInt64"),
                    "automatic repair candidate input bytes overflow UInt64");
                const UInt64 next_retained
                    = checkedAdd(progress.candidate_retained_plan_bytes, retained, "automatic repair retained plan bytes overflow UInt64");
                if (next_input > limits.plan.maximum_total_input_bytes || next_retained > limits.plan.maximum_retained_plan_bytes)
                    throw AuthorityRepairPlanError(
                        AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair candidates exceed their input byte limit");
                progress.candidate_input_bytes = next_input;
                progress.candidate_retained_plan_bytes = next_retained;
                progress.selected_targets[match->index] = true;
                progress.candidates.bytes.push_back(std::move(bytes));
                progress.candidates.references.push_back(std::move(source_reference));
                progress.candidates.candidates.push_back({
                    .source = AuthorityRepairSource::LocalSchemaWAL,
                    .artifact_kind = *kind,
                    .object = match->target->object,
                    .authority_key = match->target->authority_key,
                    .object_revision = match->target->object_revision,
                    .physical_schema_fingerprint = match->target->physical_schema_fingerprint,
                    .canonical_bytes = progress.candidates.bytes.back(),
                    .source_reference = progress.candidates.references.back(),
                });
                progress.candidate_by_target[match->index] = progress.candidates.candidates.size() - 1;
            }
        }
        if (progress.candidates.candidates.size() == targets.size())
            break;
        checkRepairRunBudget(limits, "automatic repair WAL scan");
    }
    progress.source_phase
        = progress.candidates.candidates.size() == targets.size() ? SourcePhase::OrderCandidates : SourcePhase::PrepareReplicatedAuthority;
    progress.external_missing_targets.clear();
    progress.external_missing_targets.reserve(targets.size());
    progress.missing_scan_index = 0;
    progress.external_target_offset = 0;
}

void resumeCollectMissingTargets(
    std::span<const AuthorityRepairTarget> targets,
    const AuthorityAutomaticRepairLimits & limits,
    AuthorityAutomaticRepairContinuation::Impl & progress)
{
    while (progress.missing_scan_index < targets.size())
    {
        checkRepairRunBudget(limits, "automatic repair missing-target scan");
        if (progress.missing_scan_index >= progress.selected_targets.size())
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair source continuation lost its exact target-selection bitmap");
        if (!progress.selected_targets[progress.missing_scan_index])
            progress.external_missing_targets.push_back(targets[progress.missing_scan_index]);
        ++progress.missing_scan_index;
    }
}

std::optional<DatabaseSchemaWALStagedArtifactKind> stagedKindForRepairArtifact(AuthorityRepairArtifactKind kind) noexcept
{
    switch (kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord: return DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord;
        case AuthorityRepairArtifactKind::SidecarExpectationRecord: return DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord;
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar:
            return DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar;
    }
    return std::nullopt;
}

bool appendExactExternalSource(
    OwnedRepairCandidates & output,
    const AuthorityRootGraphIdentity & root,
    std::span<const AuthorityRepairTarget> all_targets,
    std::span<const AuthorityRepairTarget> missing_targets,
    const std::shared_ptr<const IExactAuthorityRepairSource> & source,
    AuthorityRepairSource expected_source,
    const AuthorityAutomaticRepairLimits & limits,
    UInt64 & input_bytes,
    UInt64 & retained_plan_bytes,
    UInt64 retained_source_controls,
    std::vector<bool> & selected_targets,
    std::vector<size_t> & candidate_by_target)
{
    if (!source || missing_targets.empty())
        return false;
    if (source->getSource() != expected_source)
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::InvalidConfiguration, "automatic repair external adapter advertises the wrong source tier");

    if (input_bytes > limits.plan.maximum_total_input_bytes || retained_plan_bytes > limits.plan.maximum_retained_plan_bytes
        || retained_source_controls > limits.maximum_source_control_bytes || output.candidates.size() > limits.plan.maximum_candidates)
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair candidates exceed their input byte limit");
    const UInt64 remaining_candidate_slots = limits.plan.maximum_candidates - toUInt64(output.candidates.size());
    const UInt64 remaining_input = limits.plan.maximum_total_input_bytes - input_bytes;
    const UInt64 remaining_retained = limits.plan.maximum_retained_plan_bytes - retained_plan_bytes;
    const UInt64 remaining_control = limits.maximum_source_control_bytes - retained_source_controls;
    UInt64 candidate_cap = std::min<UInt64>(toUInt64(missing_targets.size()), remaining_candidate_slots);
    candidate_cap = std::min(candidate_cap, remaining_input / candidate_view_base_canonical_bytes);
    candidate_cap = std::min(candidate_cap, remaining_control / sizeof(AuthorityExactRepairSourceCandidate));
    if (candidate_cap == 0)
        return false;
    const UInt64 fixed_input_reservation = checkedMultiply(
        candidate_cap, candidate_view_base_canonical_bytes, "automatic repair external candidate input bytes overflow UInt64");
    AuthorityExactRepairSourceLimits source_limits{
        .maximum_candidates = candidate_cap,
        .maximum_total_candidate_bytes
        = std::min({limits.maximum_external_source_bytes_per_call, remaining_input - fixed_input_reservation, remaining_retained}),
        .maximum_source_reference_bytes = limits.plan.maximum_source_reference_bytes,
        .maximum_control_bytes = remaining_control,
        .cancellation = limits.cancellation,
        .monotonic_deadline = limits.monotonic_deadline,
        .thread_cpu_deadline_nanoseconds = limits.thread_cpu_deadline_nanoseconds,
    };
    if (source_limits.maximum_total_candidate_bytes == 0)
        return false;

    checkRepairRunBudget(limits, "automatic repair external source scan");
    auto owned = source->collect(root, missing_targets, source_limits);
    if (owned.size() > source_limits.maximum_candidates)
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::InvalidCandidateSet, "automatic repair external adapter exceeded its candidate-count contract");
    if (checkedMultiply(
            toUInt64(owned.size()),
            sizeof(AuthorityExactRepairSourceCandidate),
            "automatic repair external source controls overflow UInt64")
        > source_limits.maximum_control_bytes)
    {
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::InvalidCandidateSet, "automatic repair external adapter exceeded its control-memory contract");
    }
    UInt64 source_bytes = 0;
    size_t preceding_target_index = 0;
    bool have_preceding_target = false;
    for (const auto & candidate : owned)
    {
        const auto it = std::lower_bound(
            missing_targets.begin(),
            missing_targets.end(),
            std::pair{candidate.authority_key, candidate.artifact_kind},
            [](const AuthorityRepairTarget & target, const auto & identity)
            { return artifactIdentityLess(target.authority_key, target.artifact_kind, identity.first, identity.second); });
        if (it == missing_targets.end() || it->authority_key != candidate.authority_key || it->artifact_kind != candidate.artifact_kind
            || it->object != candidate.object || it->object_revision != candidate.object_revision
            || it->physical_schema_fingerprint != candidate.physical_schema_fingerprint
            || candidate.source_reference.size() > source_limits.maximum_source_reference_bytes)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair external adapter returned a candidate outside its exact requested target set");
        const size_t index = static_cast<size_t>(it - missing_targets.begin());
        if (have_preceding_target && index <= preceding_target_index)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair external adapter returned noncanonical or duplicate target candidates");
        preceding_target_index = index;
        have_preceding_target = true;
        source_bytes
            = checkedAdd(source_bytes, toUInt64(candidate.canonical_bytes.size()), "automatic repair source bytes overflow UInt64");
        source_bytes
            = checkedAdd(source_bytes, toUInt64(candidate.source_reference.size()), "automatic repair source provenance overflows UInt64");
        if (source_bytes > source_limits.maximum_total_candidate_bytes)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair external adapter exceeded its candidate-byte contract");
        const auto staged_kind = stagedKindForRepairArtifact(candidate.artifact_kind);
        if (!staged_kind || !exactCandidateBytes(*it, *staged_kind, candidate.canonical_bytes))
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet, "automatic repair external adapter returned a non-exact candidate");
    }

    const UInt64 next_input = checkedAdd(
        input_bytes,
        checkedAdd(
            checkedMultiply(
                toUInt64(owned.size()),
                candidate_view_base_canonical_bytes,
                "automatic repair external candidate input bytes overflow UInt64"),
            source_bytes,
            "automatic repair external candidate input bytes overflow UInt64"),
        "automatic repair external candidate input bytes overflow UInt64");
    const UInt64 next_retained
        = checkedAdd(retained_plan_bytes, source_bytes, "automatic repair external retained plan bytes overflow UInt64");
    if (next_input > limits.plan.maximum_total_input_bytes || next_retained > limits.plan.maximum_retained_plan_bytes)
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair external candidates exceed the prospective plan limits");

    for (auto & candidate : owned)
    {
        const auto full_match = findTarget(all_targets, candidate.authority_key, candidate.artifact_kind);
        if (!full_match || full_match->target->object != candidate.object
            || full_match->target->object_revision != candidate.object_revision || full_match->index >= selected_targets.size()
            || full_match->index >= candidate_by_target.size() || selected_targets[full_match->index])
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::InvalidCandidateSet,
                "automatic repair external adapter changed its exact candidate mapping");
        output.bytes.push_back(std::move(candidate.canonical_bytes));
        output.references.push_back(std::move(candidate.source_reference));
        output.candidates.push_back({
            .source = expected_source,
            .artifact_kind = candidate.artifact_kind,
            .object = candidate.object,
            .authority_key = candidate.authority_key,
            .object_revision = candidate.object_revision,
            .physical_schema_fingerprint = candidate.physical_schema_fingerprint,
            .canonical_bytes = output.bytes.back(),
            .source_reference = output.references.back(),
        });
        selected_targets[full_match->index] = true;
        candidate_by_target[full_match->index] = output.candidates.size() - 1;
    }
    input_bytes = next_input;
    retained_plan_bytes = next_retained;
    return !owned.empty();
}

const OwnedRepairCandidates & collectRepairCandidates(
    AtomicDatabaseSchemaMutationStorage & storage,
    const AuthorityRootGraphIdentity & root,
    std::span<const AuthorityRepairTarget> targets,
    const AuthorityAutomaticRepairLimits & limits,
    AuthorityAutomaticRepairResult & result,
    AuthorityAutomaticRepairContinuation::Impl & progress)
{
    using SourcePhase = AuthorityAutomaticRepairContinuation::Impl::SourcePhase;
    resumeLocalWALCandidates(storage, root, targets, limits, progress);
    while (progress.source_phase != SourcePhase::Complete && progress.source_phase != SourcePhase::OrderCandidates)
    {
        const bool preparing = progress.source_phase == SourcePhase::PrepareReplicatedAuthority
            || progress.source_phase == SourcePhase::PrepareVerifiedBackup;
        if (preparing)
        {
            resumeCollectMissingTargets(targets, limits, progress);
            progress.external_target_offset = 0;
            progress.source_phase = progress.source_phase == SourcePhase::PrepareReplicatedAuthority ? SourcePhase::ReplicatedAuthority
                                                                                                     : SourcePhase::VerifiedBackup;
            if (progress.external_missing_targets.empty())
            {
                progress.source_phase = SourcePhase::OrderCandidates;
                break;
            }
        }

        const bool replicated = progress.source_phase == SourcePhase::ReplicatedAuthority;
        const auto & source = replicated ? limits.replicated_authority_source : limits.verified_backup_source;
        const AuthorityRepairSource expected_source
            = replicated ? AuthorityRepairSource::ReplicatedAuthority : AuthorityRepairSource::VerifiedBackup;
        while (progress.external_target_offset < progress.external_missing_targets.size())
        {
            checkRepairRunBudget(limits, "automatic repair external source scan");
            const size_t end = std::min(
                progress.external_missing_targets.size(),
                progress.external_target_offset + static_cast<size_t>(limits.maximum_external_source_targets_per_call));
            const std::span<const AuthorityRepairTarget> chunk(
                progress.external_missing_targets.data() + progress.external_target_offset, end - progress.external_target_offset);
            static_cast<void>(appendExactExternalSource(
                progress.candidates,
                root,
                targets,
                chunk,
                source,
                expected_source,
                limits,
                progress.candidate_input_bytes,
                progress.candidate_retained_plan_bytes,
                progress.source_control_bytes,
                progress.selected_targets,
                progress.candidate_by_target));
            progress.external_target_offset = end;
            checkRepairRunBudget(limits, "automatic repair external source scan");
        }
        if (replicated)
        {
            progress.external_missing_targets.clear();
            progress.external_missing_targets.reserve(targets.size());
            progress.missing_scan_index = 0;
            progress.external_target_offset = 0;
            progress.source_phase = SourcePhase::PrepareVerifiedBackup;
        }
        else
        {
            progress.source_phase = SourcePhase::OrderCandidates;
        }
        checkRepairRunBudget(limits, "automatic repair source scan");
    }

    if (progress.source_phase == SourcePhase::OrderCandidates)
    {
        if (progress.ordered_candidates.candidates.empty() && progress.ordering_target_index == 0)
        {
            progress.ordered_candidates.bytes.reserve(targets.size());
            progress.ordered_candidates.references.reserve(targets.size());
            progress.ordered_candidates.candidates.reserve(targets.size());
        }
        while (progress.ordering_target_index < targets.size())
        {
            checkRepairRunBudget(limits, "automatic repair candidate ordering");
            const size_t target_index = progress.ordering_target_index;
            if (target_index >= progress.selected_targets.size() || target_index >= progress.candidate_by_target.size())
                throw AuthorityRepairPlanError(
                    AuthorityRepairPlanError::Code::InvalidCandidateSet, "automatic repair candidate ordering lost its target mapping");
            if (progress.selected_targets[target_index])
            {
                const size_t source_index = progress.candidate_by_target[target_index];
                if (source_index >= progress.candidates.candidates.size() || source_index >= progress.candidates.bytes.size()
                    || source_index >= progress.candidates.references.size())
                    throw AuthorityRepairPlanError(
                        AuthorityRepairPlanError::Code::InvalidCandidateSet,
                        "automatic repair candidate ordering has an invalid source cursor");
                const auto source = progress.candidates.candidates[source_index];
                progress.ordered_candidates.bytes.push_back(std::move(progress.candidates.bytes[source_index]));
                progress.ordered_candidates.references.push_back(std::move(progress.candidates.references[source_index]));
                progress.ordered_candidates.candidates.push_back({
                    .source = source.source,
                    .artifact_kind = source.artifact_kind,
                    .object = source.object,
                    .authority_key = source.authority_key,
                    .object_revision = source.object_revision,
                    .physical_schema_fingerprint = source.physical_schema_fingerprint,
                    .canonical_bytes = progress.ordered_candidates.bytes.back(),
                    .source_reference = progress.ordered_candidates.references.back(),
                });
                progress.candidate_by_target[target_index] = progress.ordered_candidates.candidates.size() - 1;
            }
            ++progress.ordering_target_index;
            checkRepairRunBudget(limits, "automatic repair candidate ordering");
        }
        progress.candidates = std::move(progress.ordered_candidates);
        progress.ordered_candidates = {};
        progress.ordering_target_index = 0;
        progress.source_phase = SourcePhase::Complete;
    }
    result.local_wal_transactions_examined = progress.wal_transactions_examined;
    result.local_wal_artifacts_examined = progress.wal_artifacts_examined;
    result.local_wal_bytes_examined = progress.wal_bytes_examined;
    return progress.candidates;
}

void validateLimits(const AuthorityAutomaticRepairLimits & limits)
{
    constexpr AuthorityRepairPlanLimits plan_maxima;
    if (limits.maximum_local_wal_transactions == 0 || limits.maximum_local_wal_discovery_work_items_per_pass == 0
        || limits.maximum_local_wal_discovery_work_items_per_pass
            > AuthorityAutomaticRepairLimits{}.maximum_local_wal_discovery_work_items_per_pass
        || limits.maximum_local_wal_artifacts_examined == 0 || limits.maximum_local_wal_bytes_examined == 0
        || limits.maximum_audit_input_retained_bytes == 0
        || limits.maximum_source_control_bytes < minimum_wal_source_in_flight_control_bytes
        || limits.maximum_source_control_bytes > AuthorityAutomaticRepairLimits{}.maximum_source_control_bytes
        || limits.maximum_external_source_targets_per_call == 0
        || limits.maximum_external_source_targets_per_call > plan_maxima.maximum_candidates
        || limits.maximum_external_source_bytes_per_call == 0
        || limits.maximum_external_source_bytes_per_call > plan_maxima.maximum_total_input_bytes)
        throw AuthorityRepairPlanError(
            AuthorityRepairPlanError::Code::InvalidConfiguration, "automatic repair limits must be finite and nonzero");
}

}

AuthorityAutomaticRepairResult
AuthorityAutomaticRepair::attempt(DB::DatabaseAtomic & database, const AuthorityAutomaticRepairLimits & limits)
{
    validateLimits(limits);
    checkRepairRunBudget(limits, "automatic repair");
    database.waitDatabaseStarted();
    AuthorityAutomaticRepairResult result;
    std::unique_lock schema_lock(database.udt_schema_mutation_mutex);
    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    AuthorityVerificationRuntimeState * runtime = nullptr;
    {
        std::lock_guard authority_lock(database.udt_authority_mutex);
        authority = database.udt_authority.get();
        storage = database.udt_mutation_storage.get();
        runtime = database.udt_verification_runtime.get();
        if (database.udt_authority_mode != DB::DatabaseAtomic::AuthorityMode::Enabled || database.udt_authority_shutdown
            || database.udt_table_startup_state || !authority || !storage || !runtime
            || database.active_udt_authority.load(std::memory_order_acquire) != authority
            || database.active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::InvalidRoot, "automatic repair components are not active and consistent");
    }

    auto runtime_snapshot = runtime->acquireSnapshot();
    if (runtime_snapshot.isFailClosed())
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::QuarantineFailure, "automatic repair cannot recover an unbounded fail-closed runtime");
    const auto published_quarantine = runtime_snapshot.getQuarantine();
    if (!published_quarantine)
        return result;

    auto read_root = authority->acquireCurrentRoot();
    auto audit_root = authority->acquireCurrentRoot();
    if (!read_root || !audit_root || read_root->getAuthorityState() != audit_root->getAuthorityState())
        throw AuthorityRepairAuditError(AuthorityRepairAuditError::Code::InvalidRoot, "automatic repair cannot pin one exact root");
    const auto durable_state = storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != read_root->getAuthorityState() || storage->getRecoveryRequiredTransactionID())
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::InvalidRoot, "automatic repair root differs from the durable schema head");
    const auto & root_state = read_root->getAuthorityState();
    const AuthorityRootGraphIdentity exact_root_identity{
        .authority_root = {
            .database_uuid = root_state.database_uuid,
            .database_catalog_epoch = root_state.database_catalog_epoch,
            .authority_anchor = root_state.anchor_hash,
        },
        .schema_graph_root = root_state.schema_graph_root,
    };
    if (published_quarantine->getRoot() != exact_root_identity)
        throw AuthorityRepairAuditError(
            AuthorityRepairAuditError::Code::QuarantineFailure,
            "automatic repair quarantine is not anchored to the current exact authority root");

    auto execution_limits = effectiveExecutionLimits(limits);
    if (execution_limits.reverification_continuation && execution_limits.reverification_continuation->isActiveFor(*published_quarantine))
    {
        checkRepairRunBudget(limits, "automatic repair re-verification resume");
        result.execution = AuthorityRepairCoordinator::resumeReverificationAndRelease(database, std::move(schema_lock), execution_limits);
        result.status = result.execution->status == AuthorityExactRepairStatus::ReverifiedAndReleased
            ? AuthorityAutomaticRepairStatus::ReverifiedAndReleased
            : AuthorityAutomaticRepairStatus::RepairedQuarantineRetained;
        return result;
    }

    auto automatic_continuation = limits.continuation;
    if (!automatic_continuation)
        automatic_continuation = std::make_shared<AuthorityAutomaticRepairContinuation>();
    auto & progress = *automatic_continuation->impl;
    std::unique_lock progress_lock(progress.mutex);
    if (!progress.root || *progress.root != exact_root_identity || progress.quarantine != published_quarantine)
        progress.reset(exact_root_identity, published_quarantine);
    if (!progress.completed_audit)
    {
        const auto & observations = collectAuditObservations(database, *storage, read_root.get(), limits, progress);
        auto audit_limits = limits.audit;
        if (limits.cancellation.stop_possible())
            audit_limits.cancellation = limits.cancellation;
        audit_limits.monotonic_deadline = earlierDeadline(audit_limits.monotonic_deadline, limits.monotonic_deadline);
        audit_limits.thread_cpu_deadline_nanoseconds
            = earlierDeadline(audit_limits.thread_cpu_deadline_nanoseconds, limits.thread_cpu_deadline_nanoseconds);
        if (!progress.audit_build)
            progress.audit_build = std::make_unique<AuthorityRepairAuditBuildContinuation>();
        progress.completed_audit = AuthorityRepairAudit::resume(
            *progress.audit_build, std::move(audit_root), observations.observations, audit_limits, published_quarantine);
        progress.audit_build.reset();
        progress.observations = {};
        progress.preflight_leaf = 0;
        progress.observation_count = 0;
        progress.byte_observation_count = 0;
        progress.observation_control_bytes = 0;
        progress.audit_buffers_ready = false;
        progress.observation_leaf = 0;
        progress.observed_bytes = 0;
    }
    const auto & audit = progress.completed_audit;
    checkRepairRunBudget(limits, "automatic repair audit");
    result.audited_artifacts = audit->getStatistics().observed_artifacts;
    result.damaged_artifacts = audit->getDamagedArtifactCount();
    if (!audit->hasDamage())
    {
        progress.clear();
        progress_lock.unlock();
        result.execution = AuthorityRepairCoordinator::resumeReverificationAndRelease(database, std::move(schema_lock), execution_limits);
        result.status = result.execution->status == AuthorityExactRepairStatus::ReverifiedAndReleased
            ? AuthorityAutomaticRepairStatus::ReverifiedAndReleased
            : AuthorityAutomaticRepairStatus::RepairedQuarantineRetained;
        return result;
    }

    const auto audited_quarantine = audit->pinQuarantinePlan();
    const bool same_quarantine = audited_quarantine && audited_quarantine->getRoot() == published_quarantine->getRoot()
        && audited_quarantine->getFailingSeeds().size() == published_quarantine->getFailingSeeds().size()
        && std::equal(audited_quarantine->getFailingSeeds().begin(),
                      audited_quarantine->getFailingSeeds().end(),
                      published_quarantine->getFailingSeeds().begin())
        && audited_quarantine->getQuarantinedObjects().size() == published_quarantine->getQuarantinedObjects().size()
        && std::equal(audited_quarantine->getQuarantinedObjects().begin(),
                      audited_quarantine->getQuarantinedObjects().end(),
                      published_quarantine->getQuarantinedObjects().begin());
    if (!same_quarantine)
    {
        runtime->publishAutomaticRepairAuditQuarantine(read_root.get(), audited_quarantine);
        progress.quarantine = audited_quarantine;
    }
    checkRepairRunBudget(limits, "automatic repair quarantine publication");
    if (!audit->hasCompleteRepairTargetSet())
    {
        progress.clear();
        result.status = AuthorityAutomaticRepairStatus::AuditUnrepairable;
        return result;
    }
    const auto repair_targets = audit->getCompleteRepairTargets();
    if (!progress.audit_manifest)
    {
        const UInt64 target_count = toUInt64(repair_targets.size());
        if (target_count == 0 || target_count > limits.plan.maximum_targets || target_count > limits.plan.maximum_candidates)
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded,
                "automatic repair target count exceeds the source/planner limits before allocation");
        const UInt64 fixed_input = fixedPlanInputBytes(target_count);
        const UInt64 fixed_retained = fixedPlanRetainedBytes(target_count);
        const UInt64 source_controls = sourceControlReservation(target_count, limits.maximum_local_wal_transactions);
        const UInt64 source_peak_controls = checkedAdd(
            source_controls, minimum_wal_source_in_flight_control_bytes, "automatic repair minimum in-flight WAL controls overflow UInt64");
        if (fixed_input > limits.plan.maximum_total_input_bytes || fixed_retained > limits.plan.maximum_retained_plan_bytes
            || source_peak_controls > limits.maximum_source_control_bytes)
        {
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded,
                "automatic repair fixed source/planner state exceeds its prospective limits");
        }
        progress.audit_manifest = audit->getDamagedArtifactManifestDigest();
        progress.repair_targets.assign(repair_targets.begin(), repair_targets.end());
        progress.candidate_input_bytes = fixed_input;
        progress.candidate_retained_plan_bytes = fixed_retained;
        progress.source_control_bytes = source_controls;
    }
    else
    {
        const auto same_target = [](const AuthorityRepairTarget & lhs, const AuthorityRepairTarget & rhs)
        {
            return lhs.artifact_kind == rhs.artifact_kind && lhs.object == rhs.object && lhs.authority_key == rhs.authority_key
                && lhs.object_revision == rhs.object_revision && lhs.expected_canonical_hash == rhs.expected_canonical_hash
                && lhs.physical_schema_fingerprint == rhs.physical_schema_fingerprint;
        };
        const UInt64 target_count = toUInt64(repair_targets.size());
        const UInt64 source_controls = sourceControlReservation(target_count, limits.maximum_local_wal_transactions);
        const UInt64 source_peak_controls = checkedAdd(
            source_controls, minimum_wal_source_in_flight_control_bytes, "automatic repair minimum in-flight WAL controls overflow UInt64");
        if (*progress.audit_manifest != audit->getDamagedArtifactManifestDigest() || progress.repair_targets.size() != repair_targets.size()
            || !std::equal(progress.repair_targets.begin(), progress.repair_targets.end(), repair_targets.begin(), same_target))
        {
            throw AuthorityRepairAuditError(
                AuthorityRepairAuditError::Code::NonCanonicalObservationSet,
                "automatic repair resumed audit changed its exact damage manifest");
        }
        if (target_count > limits.plan.maximum_targets || target_count > limits.plan.maximum_candidates
            || progress.candidate_input_bytes < fixedPlanInputBytes(target_count)
            || progress.candidate_input_bytes > limits.plan.maximum_total_input_bytes
            || progress.candidate_retained_plan_bytes < fixedPlanRetainedBytes(target_count)
            || progress.candidate_retained_plan_bytes > limits.plan.maximum_retained_plan_bytes
            || progress.source_control_bytes != source_controls || source_peak_controls > limits.maximum_source_control_bytes)
        {
            throw AuthorityRepairPlanError(
                AuthorityRepairPlanError::Code::LimitExceeded, "automatic repair continuation exceeds its current source/planner limits");
        }
    }

    const auto & candidates = collectRepairCandidates(*storage, audit->getRoot(), progress.repair_targets, limits, result, progress);
    try
    {
        if (!progress.repair_plan)
        {
            if (!progress.plan_build)
                progress.plan_build = std::make_unique<AuthorityRepairPlanBuildContinuation>();
            while (!progress.repair_plan)
            {
                checkRepairRunBudget(limits, "automatic repair planning");
                progress.repair_plan
                    = AuthorityRepairPlan::resumeExactCandidateSet(*progress.plan_build, *audit, candidates.candidates, 1, limits.plan);
                checkRepairRunBudget(limits, "automatic repair planning");
            }
            progress.plan_build.reset();
        }
        checkRepairRunBudget(limits, "automatic repair planning");
    }
    catch (const AuthorityRepairPlanError & error)
    {
        if (error.code != AuthorityRepairPlanError::Code::ExactSourceMissing)
            throw;
        progress.plan_build.reset();
        progress.external_missing_targets.clear();
        progress.external_missing_targets.reserve(progress.repair_targets.size());
        progress.missing_scan_index = 0;
        progress.external_target_offset = 0;
        progress.source_phase = AuthorityAutomaticRepairContinuation::Impl::SourcePhase::PrepareReplicatedAuthority;
        result.status = AuthorityAutomaticRepairStatus::ExactSourceUnavailable;
        return result;
    }

    auto repair_plan = progress.repair_plan;
    progress.clear();
    progress_lock.unlock();
    result.execution = AuthorityRepairCoordinator::executeAndRelease(database, *repair_plan, std::move(schema_lock), execution_limits);
    result.status = result.execution->status == AuthorityExactRepairStatus::RepairedAndReleased
        ? AuthorityAutomaticRepairStatus::RepairedAndReleased
        : AuthorityAutomaticRepairStatus::RepairedQuarantineRetained;
    return result;
}

AuthorityAutomaticRepairResult
AuthorityAutomaticRepair::attemptLocalSchemaWAL(DB::DatabaseAtomic & database, const AuthorityAutomaticRepairLimits & limits)
{
    return attempt(database, limits);
}

}
