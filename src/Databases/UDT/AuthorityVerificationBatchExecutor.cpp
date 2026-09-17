#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AtomicStoredObjectUDTMetadataValidator.h>
#include <Databases/UDT/AtomicTableMetadataValidator.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <limits>
#include <new>
#include <optional>
#include <utility>
#include <vector>

namespace DB::UDT
{

class AuthorityVerificationBatchExecutorAccess final
{
public:
    static bool isExactlyTemporarilyDetached(DB::DatabaseAtomic & database, const SchemaObjectID & object, std::string_view object_name)
        TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        /// executeTrusted validates and retains the concrete database schema
        /// lock before any target reaches this narrow friend-access seam.
        return database.isExactTemporarilyDetachedUDTObject(object, object_name);
    }
};

namespace
{

using ExecutorError = AuthorityVerificationBatchExecutorError;

[[noreturn]] void fail(ExecutorError::Code code, std::string_view message)
{
    throw ExecutorError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(ExecutorError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(ExecutorError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool costFits(const AuthorityVerificationTargetCost & actual, const AuthorityVerificationTargetCost & declared) noexcept
{
    return actual.canonical_bytes <= declared.canonical_bytes && actual.work_units <= declared.work_units
        && actual.transient_bytes <= declared.transient_bytes && actual.io_bytes <= declared.io_bytes;
}

bool executionBudgetExpired(const AuthorityVerificationBatchExecutorLimits & limits)
{
    if (limits.cancellation.stop_requested())
        return true;
    if (limits.monotonic_deadline && std::chrono::steady_clock::now() >= *limits.monotonic_deadline)
        return true;
    return limits.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *limits.thread_cpu_deadline_nanoseconds;
}

AuthorityVerificationTargetCost aggregateCosts(const AuthorityVerificationTargetCost & lhs, const AuthorityVerificationTargetCost & rhs)
{
    return {
        .canonical_bytes = checkedAdd(lhs.canonical_bytes, rhs.canonical_bytes, "verification canonical cost overflows UInt64"),
        .work_units = checkedAdd(lhs.work_units, rhs.work_units, "verification work cost overflows UInt64"),
        .transient_bytes = std::max(lhs.transient_bytes, rhs.transient_bytes),
        .io_bytes = checkedAdd(lhs.io_bytes, rhs.io_bytes, "verification I/O cost overflows UInt64"),
    };
}

AuthorityRootIdentity validateAndIdentifyRoot(const AuthorityRoot & authority)
{
    const auto & state = authority.getAuthorityState();
    const auto inventory = authority.pinAuthorityInventory();
    const UInt64 capability_mask = authority.getPersistentCapabilityMask();
    if (!inventory || state.database_uuid == UUIDHelpers::Nil || state.database_catalog_epoch == 0 || isZeroDigest(state.anchor_hash)
        || state.database_uuid != authority.getDatabaseUUID() || inventory->getSummary() != authority.getInventorySummary()
        || inventory->getSummary().leaf_count != state.leaf_count || inventory->getSummary().merkle_radix_root != state.inventory_root)
        fail(ExecutorError::Code::InvalidRoot, "verification executor received an invalid or inconsistently anchored root");
    if (capability_mask != definition_authority_capability_mask && capability_mask != dependent_object_authority_capability_mask)
        fail(ExecutorError::Code::InvalidRoot, "verification executor received an unsupported authority capability root");
    return {
        .database_uuid = state.database_uuid,
        .database_catalog_epoch = state.database_catalog_epoch,
        .authority_anchor = state.anchor_hash,
    };
}

class TargetBudget final
{
public:
    TargetBudget(const AuthorityVerificationTargetCost & declared_, const AuthorityVerificationTargetCost & source_cost)
        : declared(declared_)
        , charged(source_cost)
        , valid(costFits(source_cost, declared_))
    {
    }

    bool isValid() const noexcept { return valid; }

    bool charge(UInt64 canonical_bytes, UInt64 work_units, UInt64 io_bytes = 0) noexcept
    {
        if (!valid || !fits(charged.canonical_bytes, canonical_bytes, declared.canonical_bytes)
            || !fits(charged.work_units, work_units, declared.work_units) || !fits(charged.io_bytes, io_bytes, declared.io_bytes))
            return false;
        charged.canonical_bytes += canonical_bytes;
        charged.work_units += work_units;
        charged.io_bytes += io_bytes;
        return true;
    }

    bool chargeTransientPeak(UInt64 transient_bytes) noexcept
    {
        if (!valid || transient_bytes > declared.transient_bytes)
            return false;
        charged.transient_bytes = std::max(charged.transient_bytes, transient_bytes);
        return true;
    }

    UInt64 remainingCanonicalBytes() const noexcept { return declared.canonical_bytes - charged.canonical_bytes; }
    UInt64 remainingWorkUnits() const noexcept { return declared.work_units - charged.work_units; }
    const AuthorityVerificationTargetCost & getCharged() const noexcept { return charged; }

private:
    static bool fits(UInt64 current, UInt64 amount, UInt64 maximum) noexcept { return current <= maximum && amount <= maximum - current; }

    const AuthorityVerificationTargetCost & declared;
    AuthorityVerificationTargetCost charged;
    bool valid;
};

UInt64 definitionScratchBound(UInt64 bytes)
{
    UInt64 result = sizeof(Record);
    result = checkedAdd(
        result, checkedMultiply(bytes, 2, "definition string scratch overflows UInt64"), "definition scratch overflows UInt64");
    result = checkedAdd(
        result,
        checkedMultiply(
            checkedAdd(bytes, 1, "definition parameter count overflows UInt64") / 2,
            sizeof(Parameter),
            "definition parameter scratch overflows UInt64"),
        "definition scratch overflows UInt64");
    constexpr UInt64 minimum_dependency_bytes = sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest);
    return checkedAdd(
        result,
        checkedMultiply(
            checkedAdd(bytes, minimum_dependency_bytes - 1, "definition dependency count overflows UInt64") / minimum_dependency_bytes,
            sizeof(DefinitionDependency),
            "definition dependency scratch overflows UInt64"),
        "definition scratch overflows UInt64");
}

struct SidecarDecodeBudget
{
    PersistedTypeReferencesLimits limits;
};

SidecarDecodeBudget makeSidecarDecodeBudget(const PersistedTypeReferencesLimits & configured, UInt64 bytes)
{
    SidecarDecodeBudget result{.limits = configured};
    result.limits.maximum_sidecar_bytes = std::min(configured.maximum_sidecar_bytes, std::max<UInt64>(bytes, 1));
    const UInt64 byte_bounded_items = std::max<UInt64>(bytes, 1);
    result.limits.maximum_descriptors = std::min(configured.maximum_descriptors, byte_bounded_items);
    result.limits.maximum_occurrence_paths = std::min(configured.maximum_occurrence_paths, byte_bounded_items);

    return result;
}

ASTPtr parseTrustedTableMetadata(std::string_view canonical_metadata_bytes, std::string_view database_name, std::string_view table_name)
{
    constexpr UInt64 maximum_metadata_bytes = 16ULL << 20;
    if (canonical_metadata_bytes.size() > maximum_metadata_bytes)
        throw AtomicTableMetadataValidationError(
            AtomicTableMetadataValidationError::Code::LimitExceeded, "Atomic authority verification table metadata exceeds its byte limit");

    ASTPtr ast;
    try
    {
        ParserCreateQuery parser;
        ast = parseQuery(
            parser,
            canonical_metadata_bytes.data(),
            canonical_metadata_bytes.data() + canonical_metadata_bytes.size(),
            "Atomic authority verification table metadata",
            maximum_metadata_bytes,
            256,
            100'000);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const std::exception &)
    {
        throw AtomicTableMetadataValidationError(
            AtomicTableMetadataValidationError::Code::InvalidMetadata,
            "Atomic authority verification table metadata is not one complete CREATE-family query");
    }

    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw AtomicTableMetadataValidationError(
            AtomicTableMetadataValidationError::Code::InvalidMetadata,
            "Atomic authority verification table metadata is not a CREATE query");
    create->setDatabase(String(database_name));
    create->setTable(String(table_name));
    return ast;
}

using TrustedTargetInput = AuthorityVerificationTargetArtifactView;

std::optional<std::string_view> asView(const std::optional<String> & value) noexcept
{
    if (!value)
        return std::nullopt;
    return std::string_view(*value);
}

struct CurrentObjectImage
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest physical_schema_fingerprint{};
};

void chargeEstimated(AuthorityVerificationTargetCost & cost, UInt64 canonical_bytes, UInt64 work_units, UInt64 transient_peak = 0)
{
    cost.canonical_bytes = checkedAdd(cost.canonical_bytes, canonical_bytes, "estimated verification canonical cost overflows UInt64");
    cost.work_units = checkedAdd(cost.work_units, work_units, "estimated verification work cost overflows UInt64");
    cost.transient_bytes = std::max(cost.transient_bytes, transient_peak);
}

UInt64 definitionStructuralWork(const Record & record)
{
    return checkedAdd(
        checkedAdd(4, toUInt64(record.parameters.size()), "definition structural work overflows UInt64"),
        toUInt64(record.dependencies.size()),
        "definition structural work overflows UInt64");
}

AuthorityVerificationTargetCost finishEstimatedCost(AuthorityVerificationTargetCost cost) noexcept
{
    /// Schedule targets use zero to mean an invalid/unaccounted dimension.
    /// A missing file still consumes a probe and can legitimately read/hash no
    /// bytes, so retain a one-unit declaration without attributing it later.
    cost.canonical_bytes = std::max<UInt64>(cost.canonical_bytes, 1);
    cost.work_units = std::max<UInt64>(cost.work_units, 1);
    cost.transient_bytes = std::max<UInt64>(cost.transient_bytes, 1);
    cost.io_bytes = std::max<UInt64>(cost.io_bytes, 1);
    return cost;
}

bool isOrdinaryDependentObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

AuthorityVerificationTargetCost estimateDefinitionCost(const TrustedTargetInput & input, const AuthorityIntegrityVerifierLimits & limits)
{
    AuthorityVerificationTargetCost cost = input.source_cost;
    if (!input.authority_record)
        return finishEstimatedCost(cost);
    const UInt64 bytes = toUInt64(input.authority_record->size());
    const UInt64 peak = checkedAdd(input.retained_bytes, definitionScratchBound(bytes), "estimated definition peak overflows UInt64");
    UInt64 structural_work = 1;
    try
    {
        structural_work = definitionStructuralWork(decodeRecord(*input.authority_record, limits.definition_record));
    }
    catch (const RecordError &)
    {
        /// A malformed record is still a terminal, bounded target. Byte parsing
        /// is covered by canonical/transient and scheduler CPU/wall budgets.
    }
    chargeEstimated(cost, bytes, structural_work, peak);
    return finishEstimatedCost(cost);
}

AuthorityVerificationTargetCost estimateDependentObjectCost(
    const AuthorityRoot & authority,
    const AuthorityInventoryLeaf & leaf,
    const TrustedTargetInput & input,
    const AuthorityIntegrityVerifierLimits & configured_limits)
{
    AuthorityVerificationTargetCost cost = input.source_cost;
    if (!input.authority_record)
        return finishEstimatedCost(cost);

    const UInt64 expectation_bytes = toUInt64(input.authority_record->size());
    const UInt64 expectation_peak = checkedAdd(
        input.retained_bytes,
        checkedAdd(
            sizeof(SidecarExpectationRecord),
            checkedMultiply(expectation_bytes, 2, "estimated expectation scratch overflows UInt64"),
            "estimated expectation scratch overflows UInt64"),
        "estimated expectation peak overflows UInt64");
    chargeEstimated(cost, 0, 0, expectation_peak);

    SidecarExpectationRecord expectation;
    try
    {
        expectation = decodeSidecarExpectationRecord(*input.authority_record);
    }
    catch (const SidecarExpectationRecordError &)
    {
        return finishEstimatedCost(cost);
    }
    const auto * rooted = authority.findExpectationRecord(expectation.object);
    if (!rooted || *rooted != expectation || expectation.object.object_uuid != leaf.key.object_uuid
        || expectation.object_schema_revision != leaf.object_revision
        || computeSidecarExpectationRecordHash(expectation) != leaf.canonical_record_hash)
        return finishEstimatedCost(cost);

    std::optional<DependentObjectMetadataInstallationRecord> installation;
    if (isOrdinaryDependentObjectKind(expectation.object.kind))
    {
        if (!input.installation_record || !expectation.installation_record_hash)
            return finishEstimatedCost(cost);
        const UInt64 installation_bytes = toUInt64(input.installation_record->size());
        const UInt64 installation_peak = checkedAdd(
            input.retained_bytes,
            checkedAdd(
                sizeof(SidecarExpectationRecord),
                checkedAdd(
                    sizeof(DependentObjectMetadataInstallationRecord),
                    checkedMultiply(installation_bytes, 2, "estimated installation scratch overflows UInt64"),
                    "estimated installation scratch overflows UInt64"),
                "estimated installation scratch overflows UInt64"),
            "estimated installation peak overflows UInt64");
        chargeEstimated(cost, 0, 0, installation_peak);
        try
        {
            installation = decodeDependentObjectMetadataInstallationRecord(*input.installation_record);
            if (installation->object != expectation.object || installation->object_schema_revision != expectation.object_schema_revision
                || computeDependentObjectMetadataInstallationRecordHash(*installation) != *expectation.installation_record_hash)
                return finishEstimatedCost(cost);
        }
        catch (const DependentObjectMetadataInstallationRecordError &)
        {
            return finishEstimatedCost(cost);
        }
    }
    else if (
        expectation.object.kind != SchemaObjectKind::SyntheticTestObject || expectation.installation_record_hash
        || input.installation_record)
    {
        return finishEstimatedCost(cost);
    }

    if (!input.persisted_references || !input.metadata)
        return finishEstimatedCost(cost);
    const UInt64 sidecar_bytes = toUInt64(input.persisted_references->size());
    const auto sidecar_decode = makeSidecarDecodeBudget(configured_limits.persisted_references, sidecar_bytes);
    /// Sidecar decode, trusted metadata parsing and integrity verification are
    /// sequential phases. The exact admission-verifier ceiling is therefore
    /// their common peak rather than an additive byte-expansion estimate.
    chargeEstimated(cost, 0, 0, configured_limits.maximum_transient_bytes);

    PersistedTypeReferences references;
    try
    {
        references = decodePersistedTypeReferences(*input.persisted_references, sidecar_decode.limits);
    }
    catch (const PersistedTypeReferencesError &)
    {
        return finishEstimatedCost(cost);
    }

    AuthorityIntegrityVerificationStatistics verifier_statistics;
    try
    {
        verifier_statistics = verifyAuthorityObjectIntegrity(
                                  authority,
                                  expectation,
                                  references,
                                  *input.persisted_references,
                                  expectation.object_schema_revision,
                                  expectation.physical_schema_fingerprint,
                                  configured_limits)
                                  .statistics;
    }
    catch (const AuthorityIntegrityVerifierError & error)
    {
        if (error.code == AuthorityIntegrityVerifierError::Code::InvalidConfiguration)
            fail(ExecutorError::Code::InvalidConfiguration, "verification cost estimator limits are invalid");
        verifier_statistics = error.statistics;
    }
    chargeEstimated(
        cost, verifier_statistics.canonical_bytes_hashed, verifier_statistics.work_units, verifier_statistics.peak_transient_bytes);
    return finishEstimatedCost(cost);
}

struct CurrentObjectImageResult
{
    enum class State : UInt8
    {
        Complete,
        Damaged,
        LimitExceeded,
        Failed,
    };

    State state = State::Damaged;
    std::optional<CurrentObjectImage> image = std::nullopt;
    bool live_storage_observed = false;
    StoragePtr live_storage{};
    std::optional<IStorage::AlterLockHolder> live_alter_lock = std::nullopt;
    TableLockHolder live_table_lock{};
};

struct TargetExecutionResult
{
    std::optional<AuthorityVerificationTargetDisposition> disposition = std::nullopt;
    AuthorityVerificationTargetCost charged_cost{};
};

TargetExecutionResult verifyDefinition(
    const AuthorityRoot & authority,
    const ScheduledAuthorityVerificationTarget & target,
    const TrustedTargetInput & input,
    const AuthorityIntegrityVerifierLimits & limits)
{
    TargetBudget budget(target.cost, input.source_cost);
    if (!budget.isValid())
        return {.charged_cost = budget.getCharged()};
    if (!input.authority_record)
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};

    const UInt64 bytes = toUInt64(input.authority_record->size());
    const UInt64 peak = checkedAdd(input.retained_bytes, definitionScratchBound(bytes), "definition peak overflows UInt64");
    if (!budget.charge(bytes, 0) || !budget.chargeTransientPeak(peak))
        return {.charged_cost = budget.getCharged()};

    try
    {
        const Record observed = decodeRecord(*input.authority_record, limits.definition_record);
        if (!budget.charge(0, definitionStructuralWork(observed)))
            return {.charged_cost = budget.getCharged()};
        const Record * rooted = authority.findDefinitionRecord({
            .database_uuid = authority.getDatabaseUUID(),
            .type_uuid = target.leaf.key.object_uuid,
            .revision = target.leaf.object_revision,
        });
        const bool clean = rooted && observed == *rooted && observed.identity.database_uuid == authority.getDatabaseUUID()
            && observed.identity.type_uuid == target.leaf.key.object_uuid && observed.identity.revision == target.leaf.object_revision
            && computeRecordHash(observed, limits.definition_record) == target.leaf.canonical_record_hash;
        return {
            clean ? AuthorityVerificationTargetDisposition::Verified : AuthorityVerificationTargetDisposition::Damaged,
            budget.getCharged()};
    }
    catch (const RecordError & error)
    {
        if (error.code == RecordError::Code::LimitExceeded)
            return {.charged_cost = budget.getCharged()};
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    }
}

CurrentObjectImageResult deriveCurrentObjectImage(
    DatabaseAtomic & database,
    ContextPtr database_context,
    std::string_view database_name,
    const SidecarExpectationRecord & expectation,
    const TrustedTargetInput & input,
    const PersistedTypeReferencesLimits & sidecar_limits,
    const std::optional<DependentObjectMetadataInstallationRecord> & installation)
{
    if (!input.metadata || !input.persisted_references)
        return {.state = CurrentObjectImageResult::State::Damaged};

    try
    {
        if (expectation.object.kind == SchemaObjectKind::SyntheticTestObject)
        {
            const auto validated
                = validateSyntheticDependentObjectMetadata(expectation, *input.metadata, *input.persisted_references, {}, sidecar_limits);
            return {
                .state = CurrentObjectImageResult::State::Complete,
                .image = CurrentObjectImage{
                    .object = validated.object,
                    .object_schema_revision = validated.object_schema_revision,
                    .physical_schema_fingerprint = validated.physical_schema_fingerprint,
                },
                .live_storage_observed = false,
            };
        }
        if (!isOrdinaryDependentObjectKind(expectation.object.kind) || !installation)
            return {.state = CurrentObjectImageResult::State::Damaged};
        if (computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, *input.metadata)
            != installation->metadata_artifact_hash)
            return {.state = CurrentObjectImageResult::State::Damaged};

        const String & table_name = installation->object_name;
        auto table = database.tryGetTable(table_name, database_context);
        if (!table)
        {
            if (AuthorityVerificationBatchExecutorAccess::isExactlyTemporarilyDetached(database, expectation.object, table_name))
            {
                /// The inactive image is still fully authenticated: the
                /// rooted expectation binds this exact installation record,
                /// the installation binds the canonical metadata bytes, and
                /// the detached registry proves that no live storage replaced
                /// the UUID/name while the schema lock is held.  The common
                /// verifier below still checks the complete sidecar.  No live
                /// verification stamp is published for this branch; ATTACH
                /// performs its own full parse/rebind before activation.
                return {
                    .state = CurrentObjectImageResult::State::Complete,
                    .image = CurrentObjectImage{
                        .object = expectation.object,
                        .object_schema_revision = expectation.object_schema_revision,
                        .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
                    },
                    .live_storage_observed = false,
                };
            }
            return {.state = CurrentObjectImageResult::State::Damaged};
        }
        if (table->getStorageID().uuid != expectation.object.object_uuid || table->getStorageID().table_name != table_name)
            return {.state = CurrentObjectImageResult::State::Damaged};
        if ((expectation.object.kind == SchemaObjectKind::Table && (table->isView() || table->isDictionary()))
            || (expectation.object.kind == SchemaObjectKind::View && !table->isView())
            || (expectation.object.kind == SchemaObjectKind::Dictionary && !table->isDictionary()))
            return {.state = CurrentObjectImageResult::State::Damaged};

        /// The trusted batch already owns database schema serialization, so it
        /// must never wait for an ALTER that may itself be waiting to enter that
        /// serialization boundary. A zero-time alter-lock probe distinguishes
        /// the durable-root -> live-metadata handoff from stable corruption:
        /// retry while an engine ALTER is still publishing, otherwise retain
        /// the lock through live validation and stamp publication.
        auto live_alter_lock = table->tryLockForAlter(Poco::Timespan{0});
        if (!live_alter_lock)
            return {.state = CurrentObjectImageResult::State::Failed};
        auto live_table_lock = table->tryLockForShare("UDTAuthorityVerification", Poco::Timespan{0});
        if (!live_table_lock)
            return {.state = CurrentObjectImageResult::State::Failed};
        const auto trusted_create = parseTrustedTableMetadata(*input.metadata, database_name, table_name);
        SchemaObjectID validated_object;
        UInt64 validated_revision = 0;
        Digest validated_fingerprint{};
        if (expectation.object.kind == SchemaObjectKind::Table)
        {
            AtomicTableMetadataValidatorLimits validator_limits;
            validator_limits.persisted_references = sidecar_limits;
            AtomicTableMetadataValidator validator(expectation.object.database_uuid, trusted_create, table, std::move(validator_limits));
            const auto validated = validator.validateAndCanonicalize(expectation, *input.metadata, *input.persisted_references);
            validated_object = validated.getObject();
            validated_revision = validated.getObjectSchemaRevision();
            validated_fingerprint = validated.getPhysicalSchemaFingerprint();
        }
        else
        {
            AtomicStoredObjectUDTMetadataValidatorLimits validator_limits;
            validator_limits.persisted_references = sidecar_limits;
            AtomicStoredObjectUDTMetadataValidator validator(
                expectation.object.database_uuid, trusted_create, expectation.object_schema_revision, std::move(validator_limits));
            const auto validated = validator.validateCurrentMetadata(expectation, *input.metadata, *input.persisted_references);
            validated_object = validated.object;
            validated_revision = validated.object_schema_revision;
            validated_fingerprint = validated.physical_schema_fingerprint;
        }
        const auto live_metadata = table->getInMemoryMetadataPtr(nullptr, false);
        if (!live_metadata)
            return {.state = CurrentObjectImageResult::State::Damaged};
        live_metadata->validateBoundUDTReferences();
        const auto & live_expectation = live_metadata->getBoundUDTExpectation();
        const auto & live_references = live_metadata->getBoundUDTReferences();
        if (!live_expectation || !live_references || *live_expectation != expectation || live_references->getObject() != validated_object
            || live_references->getObjectSchemaRevision() != validated_revision
            || live_references->getSidecarHash() != expectation.sidecar_hash
            || live_references->getPhysicalSchemaFingerprint() != validated_fingerprint)
            return {.state = CurrentObjectImageResult::State::Damaged};
        return {
            .state = CurrentObjectImageResult::State::Complete,
            .image = CurrentObjectImage{
                .object = live_references->getObject(),
                .object_schema_revision = live_references->getObjectSchemaRevision(),
                .physical_schema_fingerprint = live_references->getPhysicalSchemaFingerprint(),
            },
            .live_storage_observed = true,
            .live_storage = std::move(table),
            .live_alter_lock = std::move(live_alter_lock),
            .live_table_lock = std::move(live_table_lock),
        };
    }
    catch (const SyntheticObjectMetadataError & error)
    {
        if (error.code == SyntheticObjectMetadataError::Code::LimitExceeded)
            return {.state = CurrentObjectImageResult::State::LimitExceeded};
        if (error.code == SyntheticObjectMetadataError::Code::InvalidConfiguration)
            fail(ExecutorError::Code::InvalidConfiguration, "verification synthetic-metadata limits are invalid");
        return {.state = CurrentObjectImageResult::State::Damaged};
    }
    catch (const AtomicTableMetadataValidationError & error)
    {
        if (error.code == AtomicTableMetadataValidationError::Code::LimitExceeded)
            return {.state = CurrentObjectImageResult::State::LimitExceeded};
        return {.state = CurrentObjectImageResult::State::Damaged};
    }
    catch (const StoredObjectUDTPublicationPackageError & error)
    {
        if (error.code == StoredObjectUDTPublicationPackageError::Code::InvalidConfiguration)
            fail(ExecutorError::Code::InvalidConfiguration, "verification stored-object metadata limits are invalid");
        if (error.code == StoredObjectUDTPublicationPackageError::Code::LimitExceeded)
            return {.state = CurrentObjectImageResult::State::LimitExceeded};
        return {.state = CurrentObjectImageResult::State::Damaged};
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::InvalidConfiguration)
            fail(ExecutorError::Code::InvalidConfiguration, "verification live binding limits are invalid");
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            return {.state = CurrentObjectImageResult::State::LimitExceeded};
        return {.state = CurrentObjectImageResult::State::Damaged};
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const std::exception &)
    {
        return {.state = CurrentObjectImageResult::State::Failed};
    }
}

void publishVerifiedLiveObjectStamp(
    const AuthorityRoot & authority,
    const SidecarExpectationRecord & expectation,
    const VerifiedAuthorityObjectIntegrity & verified,
    const CurrentObjectImageResult & current)
{
    if (!current.live_storage_observed)
        return;
    if (!current.image || !current.live_storage)
        fail(ExecutorError::Code::InvalidTrustedBatch, "verification live-object adapter returned incomplete publication evidence");
    if (!current.live_alter_lock || !current.live_alter_lock->owns_lock())
        fail(ExecutorError::Code::InvalidTrustedBatch, "verification live-object adapter lost its engine ALTER serialization");
    if (!current.live_table_lock)
        fail(ExecutorError::Code::InvalidTrustedBatch, "verification live-object adapter lost its table lifetime serialization");

    auto live_metadata = current.live_storage->getInMemoryMetadataPtr(nullptr, false);
    if (!live_metadata)
        fail(ExecutorError::Code::InvalidTrustedBatch, "verified live object lost its metadata before stamp publication");
    live_metadata->validateBoundUDTReferences();
    const auto & bound_references = live_metadata->getBoundUDTReferences();
    const auto & live_expectation = live_metadata->getBoundUDTExpectation();
    if (!bound_references || !live_expectation || *live_expectation != expectation || bound_references->getObject() != current.image->object
        || bound_references->getObjectSchemaRevision() != current.image->object_schema_revision
        || bound_references->getSidecarHash() != expectation.sidecar_hash
        || bound_references->getPhysicalSchemaFingerprint() != current.image->physical_schema_fingerprint)
        fail(ExecutorError::Code::InvalidTrustedBatch, "verified live object changed before stamp publication");

    const auto required_definitions = collectAuthorityVerificationRequiredDefinitions(*bound_references);
    const auto & state = authority.getAuthorityState();
    auto stamp = AuthorityVerificationStamp::create(
        verified,
        required_definitions,
        {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        });
    StorageInMemoryMetadata replacement(*live_metadata);
    replacement.setBoundUDTVerificationStamp(std::move(stamp));
    current.live_storage->setInMemoryMetadata(replacement);
}

TargetExecutionResult verifyDependentObject(
    DatabaseAtomic & database,
    ContextPtr database_context,
    std::string_view database_name,
    const AuthorityRoot & authority,
    const ScheduledAuthorityVerificationTarget & target,
    const TrustedTargetInput & input,
    const AuthorityIntegrityVerifierLimits & configured_limits)
{
    TargetBudget budget(target.cost, input.source_cost);
    if (!budget.isValid())
        return {.charged_cost = budget.getCharged()};
    if (!input.authority_record)
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};

    const UInt64 expectation_bytes = toUInt64(input.authority_record->size());
    const UInt64 expectation_peak = checkedAdd(
        input.retained_bytes,
        checkedAdd(
            sizeof(SidecarExpectationRecord),
            checkedMultiply(expectation_bytes, 2, "expectation scratch overflows UInt64"),
            "expectation scratch overflows UInt64"),
        "expectation peak overflows UInt64");
    if (!budget.chargeTransientPeak(expectation_peak))
        return {.charged_cost = budget.getCharged()};

    SidecarExpectationRecord observed_expectation;
    try
    {
        observed_expectation = decodeSidecarExpectationRecord(*input.authority_record);
    }
    catch (const SidecarExpectationRecordError &)
    {
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    }
    const auto * rooted = authority.findExpectationRecord(observed_expectation.object);
    if (!rooted || observed_expectation != *rooted || observed_expectation.object.object_uuid != target.leaf.key.object_uuid
        || observed_expectation.object_schema_revision != target.leaf.object_revision
        || computeSidecarExpectationRecordHash(observed_expectation) != target.leaf.canonical_record_hash)
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};

    std::optional<DependentObjectMetadataInstallationRecord> installation;
    if (isOrdinaryDependentObjectKind(observed_expectation.object.kind))
    {
        if (!input.installation_record || !observed_expectation.installation_record_hash)
            return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
        const UInt64 installation_bytes = toUInt64(input.installation_record->size());
        const UInt64 installation_peak = checkedAdd(
            input.retained_bytes,
            checkedAdd(
                sizeof(SidecarExpectationRecord),
                checkedAdd(
                    sizeof(DependentObjectMetadataInstallationRecord),
                    checkedMultiply(installation_bytes, 2, "installation scratch overflows UInt64"),
                    "installation scratch overflows UInt64"),
                "installation scratch overflows UInt64"),
            "installation peak overflows UInt64");
        if (!budget.chargeTransientPeak(installation_peak))
            return {.charged_cost = budget.getCharged()};
        try
        {
            installation = decodeDependentObjectMetadataInstallationRecord(*input.installation_record);
            if (installation->object != observed_expectation.object
                || installation->object_schema_revision != observed_expectation.object_schema_revision
                || computeDependentObjectMetadataInstallationRecordHash(*installation) != *observed_expectation.installation_record_hash)
                return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
        }
        catch (const DependentObjectMetadataInstallationRecordError & error)
        {
            if (error.code == DependentObjectMetadataInstallationRecordError::Code::InvalidConfiguration)
                fail(ExecutorError::Code::InvalidConfiguration, "verification installation-record limits are invalid");
            return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
        }
    }
    else if (
        observed_expectation.object.kind != SchemaObjectKind::SyntheticTestObject || observed_expectation.installation_record_hash
        || input.installation_record)
    {
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    }

    if (!input.persisted_references || !input.metadata)
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    const UInt64 sidecar_bytes = toUInt64(input.persisted_references->size());
    const auto sidecar_decode = makeSidecarDecodeBudget(configured_limits.persisted_references, sidecar_bytes);
    if (!budget.chargeTransientPeak(configured_limits.maximum_transient_bytes))
        return {.charged_cost = budget.getCharged()};

    PersistedTypeReferences references;
    try
    {
        references = decodePersistedTypeReferences(*input.persisted_references, sidecar_decode.limits);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            return {.charged_cost = budget.getCharged()};
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    }

    const auto current = deriveCurrentObjectImage(
        database, database_context, database_name, observed_expectation, input, sidecar_decode.limits, installation);
    switch (current.state)
    {
        case CurrentObjectImageResult::State::Complete:
            if (!current.image)
                fail(ExecutorError::Code::InvalidTrustedBatch, "verification current-object adapter returned an incomplete success");
            break;
        case CurrentObjectImageResult::State::Damaged: return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
        case CurrentObjectImageResult::State::LimitExceeded:
        case CurrentObjectImageResult::State::Failed: return {.charged_cost = budget.getCharged()};
    }

    const UInt64 remaining_canonical = budget.remainingCanonicalBytes();
    const UInt64 remaining_work = budget.remainingWorkUnits();
    const UInt64 remaining_transient = target.cost.transient_bytes;
    if (remaining_canonical == 0 || remaining_work == 0 || remaining_transient == 0)
        return {.charged_cost = budget.getCharged()};

    AuthorityIntegrityVerifierLimits verifier_limits = configured_limits;
    verifier_limits.persisted_references = sidecar_decode.limits;
    verifier_limits.maximum_canonical_bytes_hashed = std::min(verifier_limits.maximum_canonical_bytes_hashed, remaining_canonical);
    verifier_limits.maximum_work_units = std::min(verifier_limits.maximum_work_units, remaining_work);
    verifier_limits.maximum_transient_bytes = std::min(verifier_limits.maximum_transient_bytes, remaining_transient);
    const auto charge_verifier = [&](const AuthorityIntegrityVerificationStatistics & statistics)
    {
        return budget.charge(statistics.canonical_bytes_hashed, statistics.work_units)
            && budget.chargeTransientPeak(statistics.peak_transient_bytes);
    };

    try
    {
        const auto verified = verifyAuthorityObjectIntegrity(
            authority,
            observed_expectation,
            references,
            *input.persisted_references,
            current.image->object_schema_revision,
            current.image->physical_schema_fingerprint,
            verifier_limits);
        if (!charge_verifier(verified.statistics))
            return {.charged_cost = budget.getCharged()};
        if (verified.object != current.image->object || verified.object != observed_expectation.object
            || verified.database_uuid != authority.getDatabaseUUID()
            || verified.database_catalog_epoch != authority.getDatabaseCatalogEpoch()
            || verified.authority_anchor != authority.getAuthorityState().anchor_hash)
            return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
        publishVerifiedLiveObjectStamp(authority, observed_expectation, verified, current);
        return {AuthorityVerificationTargetDisposition::Verified, budget.getCharged()};
    }
    catch (const AuthorityIntegrityVerifierError & error)
    {
        if (!charge_verifier(error.statistics))
            return {.charged_cost = budget.getCharged()};
        if (error.code == AuthorityIntegrityVerifierError::Code::InvalidConfiguration)
            fail(ExecutorError::Code::InvalidConfiguration, "verification integrity limits are invalid");
        if (error.code == AuthorityIntegrityVerifierError::Code::LimitExceeded)
            return {.charged_cost = budget.getCharged()};
        return {AuthorityVerificationTargetDisposition::Damaged, budget.getCharged()};
    }
}

TargetExecutionResult verifyTarget(
    DatabaseAtomic & database,
    ContextPtr database_context,
    std::string_view database_name,
    const AuthorityRoot & authority,
    const ScheduledAuthorityVerificationTarget & target,
    const TrustedTargetInput & input,
    const AuthorityIntegrityVerifierLimits & limits)
{
    switch (target.leaf.key.record_kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return verifyDefinition(authority, target, input, limits);
        case AuthorityInventoryRecordKind::SidecarExpectation:
            return verifyDependentObject(database, database_context, database_name, authority, target, input, limits);
    }
    fail(ExecutorError::Code::InvalidPlan, "verification plan contains an unknown inventory record kind");
}

}

AuthorityVerificationBatchExecutorError::AuthorityVerificationBatchExecutorError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityVerificationTargetCost AuthorityVerificationBatchExecutor::estimateTrustedTargetCost(
    const AuthorityRoot & authority,
    const AuthorityInventoryLeaf & leaf,
    const AuthorityVerificationTargetArtifactView & input,
    const AuthorityVerificationBatchExecutorLimits & limits)
{
    if (limits.maximum_terminal_targets == 0 || limits.maximum_terminal_targets > 1'024)
        fail(ExecutorError::Code::InvalidConfiguration, "verification executor terminal-target limit is invalid");
    try
    {
        validateAuthorityIntegrityVerifierLimits(limits.object_verifier);
    }
    catch (const AuthorityIntegrityVerifierError &)
    {
        fail(ExecutorError::Code::InvalidConfiguration, "verification cost estimator limits are invalid");
    }
    static_cast<void>(validateAndIdentifyRoot(authority));
    const auto inventory = authority.pinAuthorityInventory();
    const auto * rooted_leaf = inventory ? inventory->find(leaf.key) : nullptr;
    if (!rooted_leaf || *rooted_leaf != leaf)
        fail(ExecutorError::Code::InvalidRoot, "verification cost estimator leaf is absent from the exact root");
    switch (leaf.key.record_kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return estimateDefinitionCost(input, limits.object_verifier);
        case AuthorityInventoryRecordKind::SidecarExpectation:
            return estimateDependentObjectCost(authority, leaf, input, limits.object_verifier);
    }
    fail(ExecutorError::Code::InvalidPlan, "verification cost estimator leaf has an unknown record kind");
}

AuthorityVerificationTrustedBatch::AuthorityVerificationTrustedBatch(
    DB::DatabaseAtomic & database_, AtomicDatabaseSchemaMutationStorage & storage_, std::unique_lock<std::mutex> schema_lock_) noexcept
    : database(&database_)
    , storage(&storage_)
    , schema_lock(std::move(schema_lock_))
{
}

AuthorityVerificationBatchReceipt::Ptr AuthorityVerificationBatchExecutor::execute(
    DB::DatabaseAtomic & database,
    const AuthorityVerificationBatchPlan & plan,
    const AuthorityVerificationBatchExecutorLimits & limits,
    const AuthorityVerificationBatchReceipt * verified_prefix)
{
    return database.executeUDTAuthorityVerificationBatch(plan, limits, true, verified_prefix);
}

AuthorityVerificationBatchReceipt::Ptr AuthorityVerificationBatchExecutor::executeTrusted(
    AtomicAuthority::RootSnapshot & pinned_root,
    const AuthorityVerificationBatchPlan & plan,
    AuthorityVerificationTrustedBatch & trusted_batch,
    const AuthorityVerificationBatchExecutorLimits & limits,
    const AuthorityVerificationBatchReceipt * verified_prefix)
{
    if (limits.maximum_terminal_targets == 0 || limits.maximum_terminal_targets > 1'024)
        fail(ExecutorError::Code::InvalidConfiguration, "verification executor terminal-target limit is invalid");
    try
    {
        validateAuthorityIntegrityVerifierLimits(limits.object_verifier);
    }
    catch (const AuthorityIntegrityVerifierError &)
    {
        fail(ExecutorError::Code::InvalidConfiguration, "verification executor limits are invalid");
    }

    auto & batch = trusted_batch;
    if (!batch.database || !batch.storage || !batch.schema_lock.owns_lock()
        || batch.schema_lock.mutex() != &batch.database->udt_schema_mutation_mutex
        || batch.storage != batch.database->udt_mutation_storage.get()
        || batch.database->active_udt_authority.load(std::memory_order_acquire) != batch.database->udt_authority.get())
    {
        fail(ExecutorError::Code::InvalidTrustedBatch, "verification executor received an invalid trusted Atomic batch");
    }

    if (!pinned_root)
        fail(ExecutorError::Code::InvalidRoot, "verification executor requires a nonempty exact-root snapshot");
    const AuthorityRoot & authority = pinned_root.get();
    const AuthorityRootIdentity executed_root = validateAndIdentifyRoot(authority);
    const Digest target_set_digest = computeAuthorityVerificationTargetSetDigest(authority.getInventorySummary());
    if (executed_root != plan.getRoot() || target_set_digest != plan.getTargetSetDigest()
        || plan.getChargeABI() != authority_verification_charge_abi)
        fail(ExecutorError::Code::InvalidPlan, "verification plan is not bound to the supplied exact root and charge ABI");

    const auto targets = plan.getTargets();
    if ((plan.getStatus() == AuthorityVerificationScheduleStatus::EmptySnapshot) != targets.empty())
        fail(ExecutorError::Code::InvalidPlan, "verification plan status and target set are inconsistent");

    const auto inventory = authority.pinAuthorityInventory();
    std::vector<AuthorityVerificationTargetCompletion> completions;
    completions.reserve(targets.size());
    AuthorityVerificationTargetCost total_charged_cost;
    if (verified_prefix)
        AuthorityVerificationBatchReceiptFactory::adoptVerifiedPrefix(plan, *verified_prefix, completions, total_charged_cost);
    const size_t prefix_completion_count = completions.size();
    for (size_t target_index = completions.size(); target_index < targets.size(); ++target_index)
    {
        if (toUInt64(completions.size() - prefix_completion_count) >= limits.maximum_terminal_targets)
            break;
        const auto & target = targets[target_index];
        if (executionBudgetExpired(limits))
            break;
        const AuthorityInventoryLeaf * rooted_leaf = inventory->find(target.leaf.key);
        if (!rooted_leaf || *rooted_leaf != target.leaf)
            fail(ExecutorError::Code::InvalidPlan, "verification target is absent from the supplied exact root");

        auto durable = batch.storage->readAuthorityVerificationTarget(authority, target);
        if (executionBudgetExpired(limits))
        {
            total_charged_cost = aggregateCosts(total_charged_cost, durable.charged_cost);
            break;
        }
        if (durable.state == AtomicDatabaseSchemaMutationStorage::VerificationTargetRead::State::Damaged)
        {
            total_charged_cost = aggregateCosts(total_charged_cost, durable.charged_cost);
            completions.push_back({
                .leaf = target.leaf,
                .disposition = AuthorityVerificationTargetDisposition::Damaged,
                .actual_charged_cost = durable.charged_cost,
            });
            continue;
        }
        if (durable.state != AtomicDatabaseSchemaMutationStorage::VerificationTargetRead::State::Complete)
        {
            total_charged_cost = aggregateCosts(total_charged_cost, durable.charged_cost);
            break;
        }

        const TrustedTargetInput input{
            .authority_record = asView(durable.authority_record_bytes),
            .installation_record = asView(durable.installation_record_bytes),
            .persisted_references = asView(durable.persisted_references_bytes),
            .metadata = asView(durable.metadata_bytes),
            .retained_bytes = durable.retained_bytes,
            .source_cost = durable.charged_cost,
        };
        TargetExecutionResult result = verifyTarget(
            *batch.database,
            batch.database->getContext(),
            batch.database->getDatabaseName(),
            authority,
            target,
            input,
            limits.object_verifier);
        total_charged_cost = aggregateCosts(total_charged_cost, result.charged_cost);
        if (!result.disposition)
            break;
        completions.push_back({
            .leaf = target.leaf,
            .disposition = *result.disposition,
            .actual_charged_cost = result.charged_cost,
        });
    }

    const auto durable_state = batch.storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != authority.getAuthorityState() || batch.storage->getRecoveryRequiredTransactionID())
        fail(ExecutorError::Code::InvalidTrustedBatch, "verification durable authority head changed while its schema lock was held");

    return AuthorityVerificationBatchReceiptFactory::issue(
        plan, executed_root, target_set_digest, std::move(completions), total_charged_cost);
}

}
