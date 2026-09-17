#include <Databases/UDT/DependentObjectAdmissionPlanner.h>

#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>

#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <algorithm>
#include <limits>
#include <set>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = DependentObjectAdmissionPlannerError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

class RootBoundAuthorityAdapter final : public IAuthorityAdapter
{
public:
    explicit RootBoundAuthorityAdapter(const AuthorityRoot & root_) noexcept
        : root(root_)
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return root.getDatabaseUUID(); }

    ResolutionSession beginResolutionSession() const override
    {
        return makeSnapshotResolutionSession(
            &root,
            {
                .find_by_identity = findByIdentity,
                .find_by_name = findByName,
                .get_generation = getGeneration,
                .get_effective_resource_limits = getEffectiveResourceLimits,
            });
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view) const override
    {
        if (!capabilities.containsAll(required))
            fail(Error::Code::InvalidBase, "dependent-object admission root lacks required transient capabilities");
    }

private:
    static Definition::Ptr findByIdentity(const void * view, const DefinitionIdentity & identity)
    {
        return static_cast<const AuthorityRoot *>(view)->findByIdentity(identity);
    }

    static Definition::Ptr findByName(const void * view, std::string_view local_name)
    {
        return static_cast<const AuthorityRoot *>(view)->findByName(local_name);
    }

    static UInt64 getGeneration(const void * view) noexcept { return static_cast<const AuthorityRoot *>(view)->getTypeIndexGeneration(); }
    static const EffectiveResourceLimits * getEffectiveResourceLimits(const void * view) noexcept
    {
        return &static_cast<const AuthorityRoot *>(view)->getDatabaseResourceQuota().getLimits();
    }

    const AuthorityRoot & root;
    const TypeAuthorityCapabilities capabilities = atomicDatabaseAuthorityCapabilities();
};

AuthorityInventoryKey expectationInventoryKey(const SchemaObjectID & object)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object.object_uuid,
    };
}

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

bool samePhysicalSchema(const BoundObjectPhysicalSchema & lhs, const BoundObjectPhysicalSchema & rhs)
{
    if (lhs.object != rhs.object || lhs.object_schema_revision != rhs.object_schema_revision
        || lhs.physical_schema_fingerprint != rhs.physical_schema_fingerprint || lhs.occurrences.size() != rhs.occurrences.size())
        return false;

    for (size_t index = 0; index < lhs.occurrences.size(); ++index)
    {
        const auto & left = lhs.occurrences[index];
        const auto & right = rhs.occurrences[index];
        if (left.path != right.path || left.selected_semantic_capabilities != right.selected_semantic_capabilities || !left.physical_type
            || !right.physical_type || !left.physical_type->equals(*right.physical_type)
            || left.physical_type->getName() != right.physical_type->getName()
            || physicalTypeFingerprint(left.physical_type) != physicalTypeFingerprint(right.physical_type))
            return false;
    }
    return true;
}

std::vector<SchemaObjectDependencyEdge>
validateDefinitionsAndMakeEdges(const AuthorityRoot & root, const PersistedTypeReferences & references)
{
    std::set<SchemaObjectID> dependencies;
    for (const auto & descriptor : references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        const auto * record = root.findDefinitionRecord(identity);
        if (!record)
            fail(Error::Code::DefinitionNotFound, "dependent-object admission references an absent exact definition revision");
        const auto definition = root.findByIdentity(identity);
        if (!definition || !recordMatchesCheckedDefinition(*record, *definition))
            fail(Error::Code::InvalidBase, "dependent-object admission root contains an unmatched definition record");
        if (descriptor.getDefinitionHash() != record->definition_hash || descriptor.getCheckerABI() != record->checker_abi
            || descriptor.getCheckerChargeABI() != record->checker_charge_abi || descriptor.getPolicyABI() != record->policy_abi
            || descriptor.getFunctionRegistryABI() != record->function_registry_abi
            || descriptor.getPolicySemanticHash() != record->policy_semantic_hash
            || descriptor.getSemanticCapabilities() != record->semantic_capabilities)
        {
            fail(Error::Code::DefinitionMismatch, "dependent-object admission descriptor differs from its exact authority definition");
        }
        dependencies.insert(definitionObject(identity));
    }

    std::vector<SchemaObjectDependencyEdge> edges;
    edges.reserve(dependencies.size());
    for (const auto & dependency : dependencies)
    {
        edges.push_back({
            .dependent = references.object,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }
    return edges;
}

std::vector<SchemaObjectDependencyEdge> validateObjectDependenciesAndMakeEdges(
    const AuthorityRoot & root,
    const SchemaObjectID & dependent,
    std::span<const SchemaObjectID> dependencies,
    const DependentObjectAdmissionPlannerLimits & limits)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    if (dependencies.size() > limits.schema_wal.maximum_graph_edge_deltas
        || dependencies.size() > limits.schema_wal.schema_graph.maximum_mutation_edges
        || dependencies.size() > graph.getLimits().maximum_mutation_edges)
    {
        fail(Error::Code::LimitExceeded, "dependent-object admission object dependencies exceed the graph mutation limit");
    }

    std::vector<SchemaObjectDependencyEdge> edges;
    edges.reserve(dependencies.size());
    const SchemaObjectID * previous = nullptr;
    for (const auto & dependency : dependencies)
    {
        if (!dependency.isValid()
            || (dependency.kind != SchemaObjectKind::Table && dependency.kind != SchemaObjectKind::View
                && dependency.kind != SchemaObjectKind::Dictionary))
            fail(Error::Code::DependencyMismatch, "dependent-object admission has an invalid object dependency identity");
        if (dependency.database_uuid != root.getDatabaseUUID())
            fail(Error::Code::DatabaseMismatch, "dependent-object admission object dependency belongs to another database");
        if (dependency == dependent || (previous && !(*previous < dependency)))
            fail(Error::Code::DependencyMismatch, "dependent-object admission object dependencies are not canonical");
        const auto * expectation = root.findExpectationRecord(dependency);
        if (!expectation || expectation->object != dependency || !graph.containsNode(dependency))
            fail(Error::Code::DependencyMismatch, "dependent-object admission object dependency is not mapped in the pinned root");
        edges.push_back({
            .dependent = dependent,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
        });
        previous = &dependency;
    }
    return edges;
}
}

DependentObjectAdmissionPlannerLimits::DependentObjectAdmissionPlannerLimits()
{
    const auto atomic_limits = atomicDatabaseAuthorityCapabilities().limits;
    authority_root.type_catalog.maximum_definitions = atomic_limits.maximum_definitions;
    authority_root.maximum_definition_records = atomic_limits.maximum_definitions;
}

ValidatedDependentTableMetadata::ValidatedDependentTableMetadata(
    SchemaObjectID object_,
    UInt64 object_schema_revision_,
    String object_name_,
    Digest physical_schema_fingerprint_,
    String canonical_metadata_bytes_,
    Digest canonical_metadata_hash_)
    : object(object_)
    , object_schema_revision(object_schema_revision_)
    , object_name(std::move(object_name_))
    , physical_schema_fingerprint(physical_schema_fingerprint_)
    , canonical_metadata_bytes(std::move(canonical_metadata_bytes_))
    , canonical_metadata_hash(canonical_metadata_hash_)
{
}

ValidatedDependentTableMetadata IDependentTableMetadataValidator::validateAndCanonicalize(
    const SidecarExpectationRecord & expectation, std::string_view candidate_metadata_bytes, std::string_view canonical_sidecar_bytes) const
{
    auto decoded = decodeAndCanonicalize(candidate_metadata_bytes, canonical_sidecar_bytes);
    if (decoded.object != expectation.object || decoded.object_schema_revision != expectation.object_schema_revision
        || decoded.sidecar_hash != expectation.sidecar_hash
        || decoded.physical_schema_fingerprint != expectation.physical_schema_fingerprint)
    {
        throw std::invalid_argument("validated table metadata differs from its authority expectation");
    }
    if (decoded.object_name.empty())
        throw std::invalid_argument("validated table metadata has an empty database-local object name");
    if (decoded.canonical_metadata_bytes.empty())
        throw std::invalid_argument("validated canonical table metadata is empty");
    const Digest canonical_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, decoded.canonical_metadata_bytes);
    return ValidatedDependentTableMetadata(
        decoded.object,
        decoded.object_schema_revision,
        std::move(decoded.object_name),
        decoded.physical_schema_fingerprint,
        std::move(decoded.canonical_metadata_bytes),
        canonical_hash);
}

DependentObjectAdmissionPlannerError::DependentObjectAdmissionPlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

PreparedDependentObjectAdmission::PreparedDependentObjectAdmission(
    const AuthorityRoot * planning_root_,
    AuthorityRoot::Ptr replacement_root_,
    DatabaseSchemaWALValidatedTransition transition_,
    BoundObjectTypeReferences::Ptr bound_references_,
    AuthorityVerificationStamp::Ptr verification_stamp_)
    : planning_root(planning_root_)
    , replacement_root(std::move(replacement_root_))
    , transition(std::move(transition_))
    , bound_references(std::move(bound_references_))
    , verification_stamp(std::move(verification_stamp_))
{
}

PreparedDependentObjectAdmission DependentObjectAdmissionPlanner::planTableCreate(
    const AuthorityRoot & current_root,
    UInt64 transaction_id,
    UInt64 expected_database_catalog_epoch,
    const PreparedTableColumnTypeBindings & table_bindings,
    String candidate_metadata_bytes,
    const IDependentTableMetadataValidator & metadata_validator,
    const DependentObjectAdmissionPlannerLimits & limits)
{
    return planTableCreate(
        current_root,
        transaction_id,
        expected_database_catalog_epoch,
        table_bindings,
        {},
        std::move(candidate_metadata_bytes),
        metadata_validator,
        limits);
}

PreparedDependentObjectAdmission DependentObjectAdmissionPlanner::planTableCreate(
    const AuthorityRoot & current_root,
    UInt64 transaction_id,
    UInt64 expected_database_catalog_epoch,
    const PreparedTableColumnTypeBindings & table_bindings,
    std::span<const SchemaObjectID> object_dependencies,
    String candidate_metadata_bytes,
    const IDependentTableMetadataValidator & metadata_validator,
    const DependentObjectAdmissionPlannerLimits & limits)
{
    if (!transaction_id)
        fail(Error::Code::InvalidRequest, "dependent-object admission transaction ID must be nonzero");
    if (current_root.getDatabaseCatalogEpoch() != expected_database_catalog_epoch)
        fail(Error::Code::ExpectedEpochMismatch, "dependent-object admission expected database epoch is stale");
    if (current_root.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "dependent-object admission database epoch cannot advance");
    if (current_root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(
            Error::Code::InvalidBase, "dependent-object admission requires the complete dependent-object-capable authority capability set");
    if (candidate_metadata_bytes.empty())
        fail(Error::Code::InvalidRequest, "dependent-object admission canonical metadata is empty");
    if (candidate_metadata_bytes.size() > limits.schema_wal.maximum_staged_artifact_bytes)
        fail(Error::Code::LimitExceeded, "dependent-object admission metadata exceeds the WAL artifact limit");

    if (!table_bindings.persisted_references || !table_bindings.bound_physical_schema || !table_bindings.sidecar_expectation)
        fail(Error::Code::InvalidBindings, "dependent-object admission requires one complete logical table-binding package");

    const auto & references = *table_bindings.persisted_references;
    if (!references.object.isValid() || references.object.kind != SchemaObjectKind::Table)
        fail(Error::Code::InvalidBindings, "dependent-object admission object is not a valid table identity");
    if (references.object.database_uuid != current_root.getDatabaseUUID())
        fail(Error::Code::DatabaseMismatch, "dependent-object admission table belongs to another database");
    if (references.object_schema_revision != 1)
        fail(Error::Code::InvalidRevision, "first dependent-table admission must start at schema revision one");

    String sidecar_bytes;
    Digest sidecar_hash{};
    try
    {
        sidecar_bytes = encodePersistedTypeReferences(references, limits.schema_wal.persisted_references);
        sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.schema_wal.persisted_references);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object admission sidecar exceeds its WAL limit");
        fail(Error::Code::InvalidBindings, "dependent-object admission sidecar is not canonical V1");
    }

    BoundObjectPhysicalSchema reconstructed_schema;
    try
    {
        reconstructed_schema = reconstructTableColumnPhysicalSchema(
            references.object, references.object_schema_revision, table_bindings.physical_columns, references, limits.table_columns);
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "dependent-object admission table-binding limits are invalid");
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object admission table bindings exceed their limit");
        fail(Error::Code::InvalidBindings, "dependent-object admission physical columns and sidecar disagree");
    }
    if (table_bindings.physical_schema_fingerprint != references.physical_schema_fingerprint)
        fail(Error::Code::IntegrityMismatch, "dependent-object admission prepared physical-schema fingerprint differs from its sidecar");
    try
    {
        if (!samePhysicalSchema(*table_bindings.bound_physical_schema, reconstructed_schema))
            fail(
                Error::Code::InvalidBindings, "dependent-object admission supplied bound schema is not the canonical table reconstruction");
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const std::exception &)
    {
        fail(Error::Code::InvalidBindings, "dependent-object admission supplied bound schema cannot be validated");
    }

    const SidecarExpectationRecord sidecar_expectation{
        .object = references.object,
        .object_schema_revision = references.object_schema_revision,
        .sidecar_hash = sidecar_hash,
        .physical_schema_fingerprint = references.physical_schema_fingerprint,
    };
    if (*table_bindings.sidecar_expectation != sidecar_expectation)
        fail(Error::Code::IntegrityMismatch, "dependent-object admission expectation differs from its exact sidecar");

    auto validated_metadata = [&]
    {
        try
        {
            return metadata_validator.validateAndCanonicalize(sidecar_expectation, candidate_metadata_bytes, sidecar_bytes);
        }
        catch (const std::exception &)
        {
            fail(Error::Code::InvalidMetadata, "dependent-object admission table metadata failed database-owned validation");
        }
        catch (...)
        {
            fail(Error::Code::InvalidMetadata, "dependent-object admission table metadata validator failed");
        }
    }();
    if (validated_metadata.getObject() != references.object
        || validated_metadata.getObjectSchemaRevision() != references.object_schema_revision
        || validated_metadata.getPhysicalSchemaFingerprint() != references.physical_schema_fingerprint)
    {
        fail(Error::Code::InvalidMetadata, "dependent-object admission table metadata describes another identity or physical schema");
    }
    String canonical_metadata_bytes = validated_metadata.releaseCanonicalMetadataBytes();
    if (canonical_metadata_bytes.size() > limits.schema_wal.maximum_staged_artifact_bytes)
        fail(Error::Code::LimitExceeded, "dependent-object admission validated metadata exceeds the WAL artifact limit");
    const Digest metadata_hash = validated_metadata.getCanonicalMetadataHash();

    String installation_record_bytes;
    Digest installation_record_hash{};
    try
    {
        const DependentObjectMetadataInstallationRecord installation_record{
            .object = references.object,
            .object_schema_revision = references.object_schema_revision,
            .object_name = validated_metadata.getObjectName(),
            .metadata_artifact_hash = metadata_hash,
        };
        installation_record_bytes
            = encodeDependentObjectMetadataInstallationRecord(installation_record, limits.schema_wal.installation_record);
        installation_record_hash
            = computeDependentObjectMetadataInstallationRecordHash(installation_record, limits.schema_wal.installation_record);
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "dependent-object admission installation-record limits are invalid");
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object admission installation record exceeds its limit");
        fail(Error::Code::InvalidMetadata, "dependent-object admission trusted table name is invalid");
    }
    if (installation_record_bytes.size() > limits.schema_wal.maximum_staged_artifact_bytes)
        fail(Error::Code::LimitExceeded, "dependent-object admission installation record exceeds the WAL artifact limit");

    SidecarExpectationRecord expected_expectation = sidecar_expectation;
    expected_expectation.installation_record_hash = installation_record_hash;

    const auto expected_definition_edges = validateDefinitionsAndMakeEdges(current_root, references);
    if (table_bindings.dependency_edges != expected_definition_edges)
        fail(Error::Code::DependencyMismatch, "dependent-object admission edges differ from the exact descriptor dependencies");
    auto dependency_edges = validateObjectDependenciesAndMakeEdges(current_root, references.object, object_dependencies, limits);
    if (expected_definition_edges.size() > limits.schema_wal.maximum_graph_edge_deltas - dependency_edges.size()
        || expected_definition_edges.size() > limits.schema_wal.schema_graph.maximum_mutation_edges - dependency_edges.size()
        || expected_definition_edges.size()
            > current_root.getSchemaObjectDependencyGraph().getLimits().maximum_mutation_edges - dependency_edges.size())
    {
        fail(Error::Code::LimitExceeded, "dependent-object admission dependencies exceed the graph mutation limit");
    }
    dependency_edges.insert(dependency_edges.end(), expected_definition_edges.begin(), expected_definition_edges.end());
    std::sort(dependency_edges.begin(), dependency_edges.end());

    BoundObjectTypeReferences::Ptr bound_references;
    try
    {
        RootBoundAuthorityAdapter authority(current_root);
        bound_references = BoundObjectTypeReferences::bind(references, std::move(reconstructed_schema), authority, limits.bound_references);
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        if (error.code == BoundObjectTypeReferencesError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "dependent-object admission bound-reference limits are invalid");
        if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object admission rebound references exceed their limit");
        if (error.code == BoundObjectTypeReferencesError::Code::AuthorityFailure)
            fail(Error::Code::DefinitionMismatch, "dependent-object admission cannot rebind against the exact authority root");
        fail(Error::Code::InvalidBindings, "dependent-object admission rebound references differ from the prepared table bindings");
    }
    if (!bound_references || bound_references->getObject() != references.object
        || bound_references->getObjectSchemaRevision() != references.object_schema_revision
        || bound_references->getSidecarHash() != sidecar_hash
        || bound_references->getPhysicalSchemaFingerprint() != references.physical_schema_fingerprint)
    {
        fail(Error::Code::IntegrityMismatch, "dependent-object admission rebound result differs from its durable identity");
    }

    const auto base_inventory = current_root.pinAuthorityInventory();
    const auto base_graph = current_root.pinSchemaObjectDependencyGraph();
    if (!base_inventory || !base_graph || base_graph->getDatabaseUUID() != current_root.getDatabaseUUID())
        fail(Error::Code::InvalidBase, "dependent-object admission root pins are incomplete");
    if (base_inventory->find(expectationInventoryKey(references.object)) || base_graph->containsNode(references.object))
        fail(Error::Code::ObjectAlreadyExists, "dependent-object admission table identity is already durable");

    SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {references.object},
        .node_removals = {},
        .edge_additions = std::move(dependency_edges),
        .edge_removals = {},
    };

    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
            current_root,
            current_root.getDatabaseCatalogEpoch() + 1,
            expected_expectation,
            graph_delta,
            {
                .object = references.object,
                .canonical_metadata_bytes = canonical_metadata_bytes,
                .canonical_sidecar_bytes = sidecar_bytes,
                .canonical_installation_record_bytes = installation_record_bytes,
            },
            limits.authority_root,
            nullptr);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "dependent-object admission replacement root is invalid");
    }

    AuthorityVerificationStamp::Ptr verification_stamp;
    try
    {
        verification_stamp
            = verifyAndCreateAuthorityVerificationStamp(*replacement_root, expected_expectation, sidecar_bytes, *bound_references);
    }
    catch (const AuthorityVerificationStampError & error)
    {
        if (error.code == AuthorityVerificationStampError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "dependent-object admission verification-stamp limits are invalid");
        if (error.code == AuthorityVerificationStampError::Code::LimitExceeded
            || error.code == AuthorityVerificationStampError::Code::ArithmeticOverflow)
            fail(Error::Code::LimitExceeded, "dependent-object admission verification stamp exceeds its limit");
        fail(Error::Code::IntegrityMismatch, "dependent-object admission failed its mandatory post-DDL verification");
    }

    const Digest expectation_hash = computeSidecarExpectationRecordHash(expected_expectation);
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas{{
        .key = expectationInventoryKey(references.object),
        .before = std::nullopt,
        .after = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = references.object_schema_revision,
            .canonical_record_hash = expectation_hash,
        },
    }};
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = references.object,
        .before = std::nullopt,
        .after = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = references.object_schema_revision,
            .metadata_hash = metadata_hash,
            .sidecar_record_hash = sidecar_hash,
            .expectation_record_hash = expectation_hash,
        },
    }};
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts{
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = references.object,
            .revision = references.object_schema_revision,
            .canonical_bytes = encodeSidecarExpectationRecord(expected_expectation),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = references.object,
            .revision = references.object_schema_revision,
            .canonical_bytes = std::move(canonical_metadata_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = references.object,
            .revision = references.object_schema_revision,
            .canonical_bytes = std::move(sidecar_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = references.object,
            .revision = references.object_schema_revision,
            .canonical_bytes = std::move(installation_record_bytes),
        },
    };

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                transaction_id,
                {
                    .authority_state = current_root.getAuthorityState(),
                    .authority_inventory = base_inventory,
                    .schema_graph = base_graph,
                },
                replacement_root->getAuthorityState(),
                std::move(authority_deltas),
                std::move(dependent_deltas),
                graph_delta,
                std::move(staged_artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "dependent-object admission WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "dependent-object admission WAL transition is invalid");
        }
    }();

    if (!replacement_root->sharesDefinitionContentWith(current_root)
        || replacement_root->getAuthorityState() != transition.getPrepare().after_authority_state
        || replacement_root->getExpectationRecordCount() != current_root.getExpectationRecordCount() + 1
        || !replacement_root->findExpectationRecord(references.object)
        || *replacement_root->findExpectationRecord(references.object) != expected_expectation
        || replacement_root->getInventorySummary() != transition.getAfterInventory().getSummary()
        || replacement_root->getSchemaObjectDependencyGraph().computeRoot() != transition.getAfterGraph().computeRoot())
    {
        fail(Error::Code::InvalidTransition, "dependent-object admission replacement root changed unrelated authority content");
    }

    return PreparedDependentObjectAdmission(
        &current_root, std::move(replacement_root), std::move(transition), std::move(bound_references), std::move(verification_stamp));
}

}
