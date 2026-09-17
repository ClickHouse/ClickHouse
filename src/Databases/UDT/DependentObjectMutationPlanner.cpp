#include <Databases/UDT/DependentObjectMutationPlanner.h>

#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <algorithm>
#include <limits>
#include <set>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = DependentObjectMutationPlannerError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

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

bool isOrdinaryDependentObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

std::set<SchemaObjectID> validateObjectDependencies(
    const AuthorityRoot & root,
    const SchemaObjectID & dependent,
    const std::vector<SchemaObjectID> & dependencies,
    const DependentObjectMutationPlannerLimits & limits)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    if (dependencies.size() > limits.schema_wal.maximum_graph_edge_deltas
        || dependencies.size() > limits.schema_wal.schema_graph.maximum_mutation_edges
        || dependencies.size() > graph.getLimits().maximum_mutation_edges)
        fail(Error::Code::LimitExceeded, "dependent-object mutation object dependencies exceed the graph mutation limit");

    std::set<SchemaObjectID> result;
    const SchemaObjectID * previous = nullptr;
    for (const auto & dependency : dependencies)
    {
        if (!dependency.isValid() || !isOrdinaryDependentObjectKind(dependency.kind))
            fail(Error::Code::InvalidBindings, "dependent-object mutation has an invalid object dependency identity");
        if (dependency.database_uuid != root.getDatabaseUUID())
            fail(Error::Code::DatabaseMismatch, "dependent-object mutation object dependency belongs to another database");
        if (dependency == dependent || (previous && !(*previous < dependency)))
            fail(Error::Code::InvalidBindings, "dependent-object mutation object dependencies are not canonical");
        const auto * expectation = root.findExpectationRecord(dependency);
        if (!expectation || expectation->object != dependency || !graph.containsNode(dependency))
            fail(Error::Code::InvalidBindings, "dependent-object mutation object dependency is not mapped in the pinned root");
        result.insert(dependency);
        previous = &dependency;
    }
    return result;
}

BoundObjectPhysicalSchema reconstructMutationPhysicalSchema(
    const DependentObjectMutationRequest & request,
    const PersistedTypeReferences & references,
    std::string_view view_metadata_bytes,
    const DependentObjectMutationPlannerLimits & limits)
{
    try
    {
        switch (request.object.kind)
        {
            case SchemaObjectKind::Table:
                return reconstructTableColumnPhysicalSchema(
                    request.object, references.object_schema_revision, request.physical_columns, references, limits.table_columns);
            case SchemaObjectKind::View:
                if (references.format_version == persisted_type_references_format_version_v2
                    || references.path_dictionary_version == persisted_type_path_dictionary_version_v2)
                {
                    if (view_metadata_bytes.empty())
                        fail(Error::Code::InvalidRequest, "mapped View mutation has no canonical metadata for V2 endpoint replay");
                    ParserCreateQuery parser;
                    auto metadata = parseQuery(
                        parser,
                        view_metadata_bytes.data(),
                        view_metadata_bytes.data() + view_metadata_bytes.size(),
                        "mapped View mutation metadata",
                        16ULL << 20,
                        256,
                        1'000'000);
                    const auto * create = metadata ? metadata->as<ASTCreateQuery>() : nullptr;
                    if (!create || create->uuid != request.object.object_uuid || !create->isView())
                        fail(Error::Code::InvalidBindings, "mapped View mutation metadata describes another object");
                    const auto auxiliary = collectViewAuxiliaryPhysicalTypeBindings(*create, limits.view_outputs);
                    return reconstructViewMixedPhysicalSchema(
                        request.object,
                        references.object_schema_revision,
                        request.physical_columns,
                        auxiliary,
                        references,
                        limits.view_outputs);
                }
                return reconstructViewOutputPhysicalSchema(
                    request.object, references.object_schema_revision, request.physical_columns, references, limits.view_outputs);
            case SchemaObjectKind::Dictionary:
                return reconstructDictionaryAttributePhysicalSchema(
                    request.object, references.object_schema_revision, request.physical_columns, references, limits.dictionary_attributes);
            case SchemaObjectKind::TypeDefinition:
            case SchemaObjectKind::SyntheticTestObject: break;
        }
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-object physical schema exceeds its limit");
        fail(Error::Code::InvalidBindings, "mapped Table physical schema differs from its sidecar");
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        if (error.code == ViewOutputTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-object physical schema exceeds its limit");
        fail(Error::Code::InvalidBindings, "mapped View physical schema differs from its sidecar");
    }
    catch (const StoredObjectTypeBindingPreparationError & error)
    {
        if (error.code == StoredObjectTypeBindingPreparationError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped View auxiliary endpoint replay exceeds its limit");
        fail(Error::Code::InvalidBindings, "mapped View auxiliary endpoint replay differs from its metadata");
    }
    catch (const DictionaryAttributeTypeBindingError & error)
    {
        if (error.code == DictionaryAttributeTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-object physical schema exceeds its limit");
        fail(Error::Code::InvalidBindings, "mapped Dictionary physical schema differs from its sidecar");
    }
    fail(Error::Code::InvalidRequest, "mapped-object mutation has an unsupported object kind");
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
            fail(Error::Code::InvalidBindings, "dependent-object mutation root lacks required transient capabilities");
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

struct ValidatedBeforeImage
{
    PersistedTypeReferences references;
    String installation_bytes;
    Digest metadata_hash{};
    Digest expectation_hash{};
};

struct PlannedDependentObjectMutation
{
    DependentObjectMutationKind kind;
    AuthorityRoot::Ptr replacement_root;
    DatabaseSchemaWALValidatedTransition transition;
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

AuthorityVerificationStamp::Ptr createVerificationStamp(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_sidecar_bytes,
    const BoundObjectTypeReferences & bound_references)
{
    try
    {
        return verifyAndCreateAuthorityVerificationStamp(root, expectation, canonical_sidecar_bytes, bound_references);
    }
    catch (const AuthorityVerificationStampError & error)
    {
        if (error.code == AuthorityVerificationStampError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidRequest, "dependent-object mutation verification-stamp limits are invalid");
        if (error.code == AuthorityVerificationStampError::Code::LimitExceeded
            || error.code == AuthorityVerificationStampError::Code::ArithmeticOverflow)
            fail(Error::Code::LimitExceeded, "dependent-object mutation verification stamp exceeds its limit");
        fail(Error::Code::InvalidBindings, "dependent object mutation failed its mandatory post-DDL verification");
    }
}

ValidatedBeforeImage validateBeforeImage(
    const AuthorityRoot & root, const DependentObjectMutationRequest & request, const DependentObjectMutationPlannerLimits & limits)
{
    const auto * expectation = root.findExpectationRecord(request.object);
    if (!expectation)
        fail(Error::Code::ObjectNotFound, "dependent-object mutation expectation is absent");
    if (request.before_image.expectation != *expectation || request.before_image.expectation.object != request.object
        || request.before_image.object_name.empty() || request.before_image.canonical_metadata_bytes.empty()
        || request.before_image.canonical_sidecar_bytes.empty())
        fail(Error::Code::StaleImage, "dependent-object mutation before image differs from the authority root");

    PersistedTypeReferences references;
    try
    {
        references = decodePersistedTypeReferences(request.before_image.canonical_sidecar_bytes, limits.schema_wal.persisted_references);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object mutation sidecar exceeds its limit");
        fail(Error::Code::StaleImage, "dependent-object mutation sidecar is not canonical");
    }
    if (encodePersistedTypeReferences(references, limits.schema_wal.persisted_references) != request.before_image.canonical_sidecar_bytes
        || references.object != request.object || references.object_schema_revision != expectation->object_schema_revision
        || references.physical_schema_fingerprint != expectation->physical_schema_fingerprint
        || computePersistedTypeReferencesSidecarHash(references, limits.schema_wal.persisted_references) != expectation->sidecar_hash)
    {
        fail(Error::Code::StaleImage, "dependent-object mutation sidecar differs from its expectation");
    }

    const Digest metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, request.before_image.canonical_metadata_bytes);
    const DependentObjectMetadataInstallationRecord installation{
        .object = request.object,
        .object_schema_revision = expectation->object_schema_revision,
        .object_name = request.before_image.object_name,
        .metadata_artifact_hash = metadata_hash,
    };
    String installation_bytes;
    Digest installation_hash{};
    try
    {
        installation_bytes = encodeDependentObjectMetadataInstallationRecord(installation, limits.schema_wal.installation_record);
        installation_hash = computeDependentObjectMetadataInstallationRecordHash(installation, limits.schema_wal.installation_record);
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "dependent-object mutation installation record exceeds its limit");
        fail(Error::Code::StaleImage, "dependent-object mutation installation record is invalid");
    }
    if (!expectation->installation_record_hash || *expectation->installation_record_hash != installation_hash)
        fail(Error::Code::StaleImage, "dependent-object mutation installation mapping differs from its expectation");

    std::set<SchemaObjectID> expected_definition_dependencies;
    for (const auto & descriptor : references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        const auto * record = root.findDefinitionRecord(identity);
        if (!record || !root.findByIdentity(identity) || record->definition_hash != descriptor.getDefinitionHash())
            fail(Error::Code::StaleImage, "dependent-object mutation sidecar references a stale definition");
        expected_definition_dependencies.insert(definitionObject(identity));
    }
    std::set<SchemaObjectID> actual_definition_dependencies;
    for (const auto & dependency : root.getSchemaObjectDependencyGraph().getDependencies(request.object))
        if (dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
            actual_definition_dependencies.insert(dependency.object);
    if (actual_definition_dependencies != expected_definition_dependencies)
        fail(Error::Code::StaleImage, "dependent-object mutation graph differs from its sidecar definitions");

    return {
        .references = std::move(references),
        .installation_bytes = std::move(installation_bytes),
        .metadata_hash = metadata_hash,
        .expectation_hash = computeSidecarExpectationRecordHash(*expectation),
    };
}

PlannedDependentObjectMutation planDrop(
    const AuthorityRoot & root,
    DependentObjectMutationRequest request,
    ValidatedBeforeImage before,
    const DependentObjectMutationPlannerLimits & limits)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    if (!graph.getDependents(request.object).empty())
        fail(Error::Code::RemainingDependent, "cannot drop a mapped object with remaining graph dependents");

    SchemaObjectDependencyGraphMutation graph_delta;
    graph_delta.node_removals = {request.object};
    for (const auto & dependency : graph.getDependencies(request.object))
    {
        graph_delta.edge_removals.push_back({
            .dependent = request.object,
            .dependency = dependency.object,
            .kind = dependency.kind,
        });
    }

    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildPhysicalizationDelta(
            root,
            root.getDatabaseCatalogEpoch() + 1,
            std::span<const DefinitionIdentity>{},
            std::span<const SchemaObjectID>(&request.object, 1),
            graph_delta,
            limits.authority_root);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "mapped-object DROP replacement root is invalid");
    }

    const auto & expectation = request.before_image.expectation;
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas{{
        .key = expectationInventoryKey(request.object),
        .before = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = expectation.object_schema_revision,
            .canonical_record_hash = before.expectation_hash,
        },
        .after = std::nullopt,
    }};
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = request.object,
        .before = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = expectation.object_schema_revision,
            .metadata_hash = before.metadata_hash,
            .sidecar_record_hash = expectation.sidecar_hash,
            .expectation_record_hash = before.expectation_hash,
        },
        .after = std::nullopt,
    }};
    std::vector<DatabaseSchemaWALStagedArtifact> artifacts{
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = request.object,
            .revision = expectation.object_schema_revision,
            .canonical_bytes = encodeSidecarExpectationRecord(expectation),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = request.object,
            .revision = expectation.object_schema_revision,
            .canonical_bytes = std::move(request.before_image.canonical_metadata_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = request.object,
            .revision = expectation.object_schema_revision,
            .canonical_bytes = std::move(request.before_image.canonical_sidecar_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = request.object,
            .revision = expectation.object_schema_revision,
            .canonical_bytes = std::move(before.installation_bytes),
        },
    };

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::buildPhysicalization(
                request.transaction_id,
                root,
                *replacement_root,
                std::move(authority_deltas),
                std::move(dependent_deltas),
                graph_delta,
                std::move(artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-object DROP WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "mapped-object DROP WAL transition is invalid");
        }
    }();

    return {
        .kind = DependentObjectMutationKind::Drop,
        .replacement_root = std::move(replacement_root),
        .transition = std::move(transition),
        .bound_references = {},
        .expectation = std::nullopt,
        .verification_stamp = {},
    };
}

PlannedDependentObjectMutation planRename(
    const AuthorityRoot & root,
    DependentObjectMutationRequest request,
    ValidatedBeforeImage before,
    const DependentObjectMutationPlannerLimits & limits)
{
    const auto & before_expectation = request.before_image.expectation;
    if (request.after_object_name.empty() || request.after_object_name == request.before_image.object_name)
        fail(Error::Code::InvalidRequest, "mapped-table RENAME requires a distinct nonempty target name");
    if (before_expectation.object_schema_revision == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "mapped-table RENAME object revision cannot advance");

    auto after_references = before.references;
    ++after_references.object_schema_revision;
    String after_sidecar_bytes;
    Digest after_sidecar_hash{};
    try
    {
        after_sidecar_bytes = encodePersistedTypeReferences(after_references, limits.schema_wal.persisted_references);
        after_sidecar_hash = computePersistedTypeReferencesSidecarHash(after_references, limits.schema_wal.persisted_references);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-table RENAME sidecar exceeds its limit");
        fail(Error::Code::InvalidBindings, "mapped-table RENAME sidecar successor is invalid");
    }

    auto physical_schema
        = reconstructMutationPhysicalSchema(request, after_references, request.before_image.canonical_metadata_bytes, limits);

    BoundObjectTypeReferences::Ptr rebound;
    try
    {
        RootBoundAuthorityAdapter authority(root);
        rebound = BoundObjectTypeReferences::bind(after_references, std::move(physical_schema), authority, limits.bound_references);
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-table RENAME rebound references exceed their limit");
        fail(Error::Code::InvalidBindings, "mapped-table RENAME cannot rebind against the exact authority root");
    }

    const DependentObjectMetadataInstallationRecord after_installation{
        .object = request.object,
        .object_schema_revision = after_references.object_schema_revision,
        .object_name = request.after_object_name,
        .metadata_artifact_hash = before.metadata_hash,
    };
    String after_installation_bytes;
    Digest after_installation_hash{};
    try
    {
        after_installation_bytes
            = encodeDependentObjectMetadataInstallationRecord(after_installation, limits.schema_wal.installation_record);
        after_installation_hash
            = computeDependentObjectMetadataInstallationRecordHash(after_installation, limits.schema_wal.installation_record);
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "mapped-table RENAME installation record exceeds its limit");
        fail(Error::Code::InvalidRequest, "mapped-table RENAME target name is invalid");
    }
    const SidecarExpectationRecord after_expectation{
        .object = request.object,
        .object_schema_revision = after_references.object_schema_revision,
        .sidecar_hash = after_sidecar_hash,
        .physical_schema_fingerprint = before_expectation.physical_schema_fingerprint,
        .semantic_extension_version = before_expectation.semantic_extension_version,
        .semantic_extension_flags = before_expectation.semantic_extension_flags,
        .installation_record_hash = after_installation_hash,
    };

    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
            root,
            root.getDatabaseCatalogEpoch() + 1,
            after_expectation,
            {},
            {
                .object = request.object,
                .canonical_metadata_bytes = request.before_image.canonical_metadata_bytes,
                .canonical_sidecar_bytes = after_sidecar_bytes,
                .canonical_installation_record_bytes = after_installation_bytes,
            },
            limits.authority_root);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "mapped-table RENAME replacement root is invalid");
    }
    auto verification_stamp = createVerificationStamp(*replacement_root, after_expectation, after_sidecar_bytes, *rebound);

    const Digest after_expectation_hash = computeSidecarExpectationRecordHash(after_expectation);
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas{{
        .key = expectationInventoryKey(request.object),
        .before = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = before_expectation.object_schema_revision,
            .canonical_record_hash = before.expectation_hash,
        },
        .after = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = after_expectation.object_schema_revision,
            .canonical_record_hash = after_expectation_hash,
        },
    }};
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = request.object,
        .before = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = before_expectation.object_schema_revision,
            .metadata_hash = before.metadata_hash,
            .sidecar_record_hash = before_expectation.sidecar_hash,
            .expectation_record_hash = before.expectation_hash,
        },
        .after = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = after_expectation.object_schema_revision,
            .metadata_hash = before.metadata_hash,
            .sidecar_record_hash = after_expectation.sidecar_hash,
            .expectation_record_hash = after_expectation_hash,
        },
    }};

    std::vector<DatabaseSchemaWALStagedArtifact> artifacts;
    artifacts.reserve(8);
    const auto add_image = [&](DatabaseSchemaWALStagedArtifactImage image,
                               UInt64 revision,
                               const SidecarExpectationRecord & expectation,
                               String metadata,
                               String sidecar,
                               String installation)
    {
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = image,
            .object = request.object,
            .revision = revision,
            .canonical_bytes = encodeSidecarExpectationRecord(expectation),
        });
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = image,
            .object = request.object,
            .revision = revision,
            .canonical_bytes = std::move(metadata),
        });
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = image,
            .object = request.object,
            .revision = revision,
            .canonical_bytes = std::move(sidecar),
        });
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = image,
            .object = request.object,
            .revision = revision,
            .canonical_bytes = std::move(installation),
        });
    };
    add_image(
        DatabaseSchemaWALStagedArtifactImage::Before,
        before_expectation.object_schema_revision,
        before_expectation,
        request.before_image.canonical_metadata_bytes,
        request.before_image.canonical_sidecar_bytes,
        std::move(before.installation_bytes));
    add_image(
        DatabaseSchemaWALStagedArtifactImage::After,
        after_expectation.object_schema_revision,
        after_expectation,
        std::move(request.before_image.canonical_metadata_bytes),
        std::move(after_sidecar_bytes),
        std::move(after_installation_bytes));

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                request.transaction_id,
                {
                    .authority_state = root.getAuthorityState(),
                    .authority_inventory = root.pinAuthorityInventory(),
                    .schema_graph = root.pinSchemaObjectDependencyGraph(),
                },
                replacement_root->getAuthorityState(),
                std::move(authority_deltas),
                std::move(dependent_deltas),
                {},
                std::move(artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-table RENAME WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "mapped-table RENAME WAL transition is invalid");
        }
    }();

    return {
        .kind = DependentObjectMutationKind::Rename,
        .replacement_root = std::move(replacement_root),
        .transition = std::move(transition),
        .bound_references = std::move(rebound),
        .expectation = after_expectation,
        .verification_stamp = std::move(verification_stamp),
    };
}

PlannedDependentObjectMutation
planAlterAdmission(const AuthorityRoot & root, DependentObjectMutationRequest request, const DependentObjectMutationPlannerLimits & limits)
{
    if (request.physical_before_object_name.empty() || request.physical_before_canonical_metadata_bytes.empty()
        || request.after_canonical_metadata_bytes.empty() || request.physical_columns.empty() || !request.after_persisted_references)
    {
        fail(Error::Code::InvalidRequest, "physical-to-mapped table ALTER admission is incomplete");
    }
    if (root.findExpectationRecord(request.object) || root.getSchemaObjectDependencyGraph().containsNode(request.object)
        || root.pinAuthorityInventory()->find(expectationInventoryKey(request.object)))
    {
        fail(Error::Code::StaleImage, "physical-to-mapped table ALTER identity is already present in the authority root");
    }

    auto after_references = std::move(*request.after_persisted_references);
    if (after_references.object != request.object || after_references.object_schema_revision != 2 || after_references.descriptors.empty()
        || after_references.uses.empty())
    {
        fail(Error::Code::InvalidBindings, "physical-to-mapped table ALTER desired sidecar identity is invalid");
    }

    String after_sidecar_bytes;
    Digest after_sidecar_hash{};
    BoundObjectTypeReferences::Ptr rebound;
    try
    {
        after_sidecar_bytes = encodePersistedTypeReferences(after_references, limits.schema_wal.persisted_references);
        after_sidecar_hash = computePersistedTypeReferencesSidecarHash(after_references, limits.schema_wal.persisted_references);
        auto physical_schema
            = reconstructTableColumnPhysicalSchema(request.object, 2, request.physical_columns, after_references, limits.table_columns);
        RootBoundAuthorityAdapter authority(root);
        rebound = BoundObjectTypeReferences::bind(after_references, std::move(physical_schema), authority, limits.bound_references);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physical-to-mapped table ALTER sidecar exceeds its limit");
        fail(Error::Code::InvalidBindings, "physical-to-mapped table ALTER sidecar is not canonical");
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physical-to-mapped table ALTER physical schema exceeds its limit");
        fail(Error::Code::InvalidBindings, "physical-to-mapped table ALTER sidecar differs from its physical schema");
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physical-to-mapped table ALTER rebound references exceed their limit");
        fail(Error::Code::InvalidBindings, "physical-to-mapped table ALTER cannot bind against the exact authority root");
    }

    std::set<SchemaObjectID> dependencies;
    for (const auto & descriptor : after_references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        const auto * record = root.findDefinitionRecord(identity);
        if (!record || !root.findByIdentity(identity) || record->definition_hash != descriptor.getDefinitionHash())
            fail(Error::Code::InvalidBindings, "physical-to-mapped table ALTER references a stale definition");
        dependencies.insert(definitionObject(identity));
    }
    SchemaObjectDependencyGraphMutation graph_delta;
    graph_delta.node_additions = {request.object};
    for (const auto & dependency : dependencies)
    {
        graph_delta.edge_additions.push_back({
            .dependent = request.object,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }
    for (const auto & dependency : validateObjectDependencies(root, request.object, request.after_object_dependencies, limits))
    {
        graph_delta.edge_additions.push_back({
            .dependent = request.object,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
        });
    }
    std::sort(graph_delta.edge_additions.begin(), graph_delta.edge_additions.end());

    const Digest before_metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, request.physical_before_canonical_metadata_bytes);
    const Digest after_metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, request.after_canonical_metadata_bytes);
    const DependentObjectMetadataInstallationRecord installation{
        .object = request.object,
        .object_schema_revision = 2,
        .object_name = request.physical_before_object_name,
        .metadata_artifact_hash = after_metadata_hash,
    };
    String installation_bytes;
    Digest installation_hash{};
    try
    {
        installation_bytes = encodeDependentObjectMetadataInstallationRecord(installation, limits.schema_wal.installation_record);
        installation_hash = computeDependentObjectMetadataInstallationRecordHash(installation, limits.schema_wal.installation_record);
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physical-to-mapped table ALTER installation record exceeds its limit");
        fail(Error::Code::InvalidRequest, "physical-to-mapped table ALTER installation mapping is invalid");
    }
    const SidecarExpectationRecord after_expectation{
        .object = request.object,
        .object_schema_revision = 2,
        .sidecar_hash = after_sidecar_hash,
        .physical_schema_fingerprint = after_references.physical_schema_fingerprint,
        .semantic_extension_version = after_references.semantic_extension_version,
        .semantic_extension_flags = after_references.semantic_extension_flags,
        .installation_record_hash = installation_hash,
    };

    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
            root,
            root.getDatabaseCatalogEpoch() + 1,
            after_expectation,
            graph_delta,
            {
                .object = request.object,
                .canonical_metadata_bytes = request.after_canonical_metadata_bytes,
                .canonical_sidecar_bytes = after_sidecar_bytes,
                .canonical_installation_record_bytes = installation_bytes,
            },
            limits.authority_root,
            nullptr);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "physical-to-mapped table ALTER replacement root is invalid");
    }
    auto verification_stamp = createVerificationStamp(*replacement_root, after_expectation, after_sidecar_bytes, *rebound);

    const Digest expectation_hash = computeSidecarExpectationRecordHash(after_expectation);
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas{{
        .key = expectationInventoryKey(request.object),
        .before = std::nullopt,
        .after = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = 2,
            .canonical_record_hash = expectation_hash,
        },
    }};
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = request.object,
        .before = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = 1,
            .metadata_hash = before_metadata_hash,
            .sidecar_record_hash = std::nullopt,
            .expectation_record_hash = std::nullopt,
        },
        .after = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = 2,
            .metadata_hash = after_metadata_hash,
            .sidecar_record_hash = after_sidecar_hash,
            .expectation_record_hash = expectation_hash,
        },
    }};
    std::vector<DatabaseSchemaWALStagedArtifact> artifacts{
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = request.object,
            .revision = 1,
            .canonical_bytes = std::move(request.physical_before_canonical_metadata_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = 2,
            .canonical_bytes = encodeSidecarExpectationRecord(after_expectation),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = 2,
            .canonical_bytes = std::move(request.after_canonical_metadata_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = 2,
            .canonical_bytes = std::move(after_sidecar_bytes),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = 2,
            .canonical_bytes = std::move(installation_bytes),
        },
    };

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                request.transaction_id,
                {
                    .authority_state = root.getAuthorityState(),
                    .authority_inventory = root.pinAuthorityInventory(),
                    .schema_graph = root.pinSchemaObjectDependencyGraph(),
                },
                replacement_root->getAuthorityState(),
                std::move(authority_deltas),
                std::move(dependent_deltas),
                graph_delta,
                std::move(artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physical-to-mapped table ALTER WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "physical-to-mapped table ALTER WAL transition is invalid");
        }
    }();

    return {
        .kind = DependentObjectMutationKind::AlterAdmission,
        .replacement_root = std::move(replacement_root),
        .transition = std::move(transition),
        .bound_references = std::move(rebound),
        .expectation = after_expectation,
        .verification_stamp = std::move(verification_stamp),
    };
}

PlannedDependentObjectMutation planAlter(
    const AuthorityRoot & root,
    DependentObjectMutationRequest request,
    ValidatedBeforeImage before,
    const DependentObjectMutationPlannerLimits & limits)
{
    const auto & before_expectation = request.before_image.expectation;
    if (request.after_canonical_metadata_bytes.empty() || request.physical_columns.empty()
        || before_expectation.object_schema_revision == std::numeric_limits<UInt64>::max())
    {
        fail(Error::Code::InvalidRequest, "mapped-object ALTER successor is incomplete");
    }
    const UInt64 after_revision = before_expectation.object_schema_revision + 1;
    const Digest after_metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, request.after_canonical_metadata_bytes);

    std::optional<PersistedTypeReferences> after_references = std::move(request.after_persisted_references);
    String after_sidecar_bytes;
    BoundObjectTypeReferences::Ptr rebound;
    std::optional<SidecarExpectationRecord> after_expectation;
    String after_installation_bytes;
    if (after_references)
    {
        if (after_references->object != request.object || after_references->object_schema_revision != after_revision
            || after_references->descriptors.empty() || after_references->uses.empty())
        {
            fail(Error::Code::InvalidBindings, "mapped-object ALTER desired sidecar identity is invalid");
        }
        try
        {
            after_sidecar_bytes = encodePersistedTypeReferences(*after_references, limits.schema_wal.persisted_references);
            auto physical_schema
                = reconstructMutationPhysicalSchema(request, *after_references, request.after_canonical_metadata_bytes, limits);
            RootBoundAuthorityAdapter authority(root);
            rebound = BoundObjectTypeReferences::bind(*after_references, std::move(physical_schema), authority, limits.bound_references);
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-object ALTER sidecar exceeds its limit");
            fail(Error::Code::InvalidBindings, "mapped-object ALTER desired sidecar is not canonical");
        }
        catch (const BoundObjectTypeReferencesError & error)
        {
            if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-object ALTER rebound references exceed their limit");
            fail(Error::Code::InvalidBindings, "mapped-object ALTER cannot bind against the exact authority root");
        }

        const DependentObjectMetadataInstallationRecord after_installation{
            .object = request.object,
            .object_schema_revision = after_revision,
            .object_name = request.before_image.object_name,
            .metadata_artifact_hash = after_metadata_hash,
        };
        Digest after_installation_hash{};
        try
        {
            after_installation_bytes
                = encodeDependentObjectMetadataInstallationRecord(after_installation, limits.schema_wal.installation_record);
            after_installation_hash
                = computeDependentObjectMetadataInstallationRecordHash(after_installation, limits.schema_wal.installation_record);
        }
        catch (const DependentObjectMetadataInstallationRecordError & error)
        {
            if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-object ALTER installation record exceeds its limit");
            fail(Error::Code::InvalidRequest, "mapped-object ALTER installation record is invalid");
        }
        after_expectation = SidecarExpectationRecord{
            .object = request.object,
            .object_schema_revision = after_revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(*after_references, limits.schema_wal.persisted_references),
            .physical_schema_fingerprint = after_references->physical_schema_fingerprint,
            .semantic_extension_version = after_references->semantic_extension_version,
            .semantic_extension_flags = after_references->semantic_extension_flags,
            .installation_record_hash = after_installation_hash,
        };
    }

    const auto collect_dependencies = [&](const PersistedTypeReferences & references)
    {
        std::set<SchemaObjectID> result;
        for (const auto & descriptor : references.descriptors)
            result.insert(definitionObject(descriptor.getDefinitionIdentity()));
        return result;
    };
    const auto before_dependencies = collect_dependencies(before.references);
    const auto after_dependencies = after_references ? collect_dependencies(*after_references) : std::set<SchemaObjectID>{};
    std::set<SchemaObjectID> before_object_dependencies;
    for (const auto & dependency : root.getSchemaObjectDependencyGraph().getDependencies(request.object))
    {
        if (dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
            before_object_dependencies.insert(dependency.object);
    }
    if (!after_references && !request.after_object_dependencies.empty())
        fail(Error::Code::InvalidBindings, "physicalized object cannot retain mapped-object dependencies");
    const auto after_object_dependencies = after_references
        ? validateObjectDependencies(root, request.object, request.after_object_dependencies, limits)
        : std::set<SchemaObjectID>{};
    SchemaObjectDependencyGraphMutation graph_delta;
    for (const auto & dependency : before_dependencies)
    {
        if (!after_dependencies.contains(dependency))
        {
            graph_delta.edge_removals.push_back({
                .dependent = request.object,
                .dependency = dependency,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            });
        }
    }
    for (const auto & dependency : after_dependencies)
    {
        if (!before_dependencies.contains(dependency))
        {
            graph_delta.edge_additions.push_back({
                .dependent = request.object,
                .dependency = dependency,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            });
        }
    }
    for (const auto & dependency : before_object_dependencies)
    {
        if (!after_object_dependencies.contains(dependency))
        {
            graph_delta.edge_removals.push_back({
                .dependent = request.object,
                .dependency = dependency,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
            });
        }
    }
    for (const auto & dependency : after_object_dependencies)
    {
        if (!before_object_dependencies.contains(dependency))
        {
            graph_delta.edge_additions.push_back({
                .dependent = request.object,
                .dependency = dependency,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
            });
        }
    }
    std::sort(graph_delta.edge_additions.begin(), graph_delta.edge_additions.end());
    std::sort(graph_delta.edge_removals.begin(), graph_delta.edge_removals.end());
    if (!after_references)
    {
        /// Once the last logical occurrence is removed, the live stored object is an
        /// ordinary physical-only object again. Remove its authority graph
        /// node together with the expectation so a later ALTER may perform a
        /// clean physical-to-mapped admission for the same stable UUID.
        if (!root.getSchemaObjectDependencyGraph().getDependents(request.object).empty())
            fail(Error::Code::RemainingDependent, "cannot physicalize a mapped object with remaining graph dependents");
        graph_delta.node_removals = {request.object};
    }

    AuthorityRoot::Ptr replacement_root;
    try
    {
        if (after_expectation)
        {
            replacement_root = AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
                root,
                root.getDatabaseCatalogEpoch() + 1,
                *after_expectation,
                graph_delta,
                {
                    .object = request.object,
                    .canonical_metadata_bytes = request.after_canonical_metadata_bytes,
                    .canonical_sidecar_bytes = after_sidecar_bytes,
                    .canonical_installation_record_bytes = after_installation_bytes,
                },
                limits.authority_root);
        }
        else
        {
            replacement_root = AuthorityRootBuilder::buildPhysicalizationDelta(
                root,
                root.getDatabaseCatalogEpoch() + 1,
                std::span<const DefinitionIdentity>{},
                std::span<const SchemaObjectID>(&request.object, 1),
                graph_delta,
                limits.authority_root);
        }
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "mapped-object ALTER replacement root is invalid");
    }

    AuthorityVerificationStamp::Ptr verification_stamp;
    if (after_expectation)
        verification_stamp = createVerificationStamp(*replacement_root, *after_expectation, after_sidecar_bytes, *rebound);

    const Digest before_expectation_hash = before.expectation_hash;
    const std::optional<Digest> after_expectation_hash
        = after_expectation ? std::optional<Digest>(computeSidecarExpectationRecordHash(*after_expectation)) : std::nullopt;
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas{{
        .key = expectationInventoryKey(request.object),
        .before = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = before_expectation.object_schema_revision,
            .canonical_record_hash = before_expectation_hash,
        },
        .after = after_expectation
            ? std::optional<DatabaseSchemaWALAuthorityRecordState>(DatabaseSchemaWALAuthorityRecordState{
                  .object_revision = after_revision,
                  .canonical_record_hash = *after_expectation_hash,
              })
            : std::nullopt,
    }};
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = request.object,
        .before = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = before_expectation.object_schema_revision,
            .metadata_hash = before.metadata_hash,
            .sidecar_record_hash = before_expectation.sidecar_hash,
            .expectation_record_hash = before_expectation_hash,
        },
        .after = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = after_revision,
            .metadata_hash = after_metadata_hash,
            .sidecar_record_hash = after_expectation
                ? std::optional<Digest>(after_expectation->sidecar_hash)
                : std::nullopt,
            .expectation_record_hash = after_expectation_hash,
        },
    }};

    std::vector<DatabaseSchemaWALStagedArtifact> artifacts;
    artifacts.reserve(after_expectation ? 8 : 5);
    artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
        .image = DatabaseSchemaWALStagedArtifactImage::Before,
        .object = request.object,
        .revision = before_expectation.object_schema_revision,
        .canonical_bytes = encodeSidecarExpectationRecord(before_expectation),
    });
    artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
        .image = DatabaseSchemaWALStagedArtifactImage::Before,
        .object = request.object,
        .revision = before_expectation.object_schema_revision,
        .canonical_bytes = std::move(request.before_image.canonical_metadata_bytes),
    });
    artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
        .image = DatabaseSchemaWALStagedArtifactImage::Before,
        .object = request.object,
        .revision = before_expectation.object_schema_revision,
        .canonical_bytes = std::move(request.before_image.canonical_sidecar_bytes),
    });
    artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
        .image = DatabaseSchemaWALStagedArtifactImage::Before,
        .object = request.object,
        .revision = before_expectation.object_schema_revision,
        .canonical_bytes = std::move(before.installation_bytes),
    });
    artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = request.object,
        .revision = after_revision,
        .canonical_bytes = std::move(request.after_canonical_metadata_bytes),
    });
    if (after_expectation)
    {
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = after_revision,
            .canonical_bytes = encodeSidecarExpectationRecord(*after_expectation),
        });
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = after_revision,
            .canonical_bytes = std::move(after_sidecar_bytes),
        });
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = request.object,
            .revision = after_revision,
            .canonical_bytes = std::move(after_installation_bytes),
        });
    }

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                request.transaction_id,
                {
                    .authority_state = root.getAuthorityState(),
                    .authority_inventory = root.pinAuthorityInventory(),
                    .schema_graph = root.pinSchemaObjectDependencyGraph(),
                },
                replacement_root->getAuthorityState(),
                std::move(authority_deltas),
                std::move(dependent_deltas),
                graph_delta,
                std::move(artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "mapped-object ALTER WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "mapped-object ALTER WAL transition is invalid");
        }
    }();

    return {
        .kind = DependentObjectMutationKind::Alter,
        .replacement_root = std::move(replacement_root),
        .transition = std::move(transition),
        .bound_references = std::move(rebound),
        .expectation = std::move(after_expectation),
        .verification_stamp = std::move(verification_stamp),
    };
}

}

DependentObjectMutationPlannerLimits::DependentObjectMutationPlannerLimits()
{
    const auto atomic_limits = atomicDatabaseAuthorityCapabilities().limits;
    authority_root.type_catalog.maximum_definitions = atomic_limits.maximum_definitions;
    authority_root.maximum_definition_records = atomic_limits.maximum_definitions;
}

DependentObjectMutationPlannerError::DependentObjectMutationPlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

PreparedDependentObjectMutation::PreparedDependentObjectMutation(
    DependentObjectMutationKind kind_,
    const AuthorityRoot * planning_root_,
    AuthorityRoot::Ptr replacement_root_,
    DatabaseSchemaWALValidatedTransition transition_,
    BoundObjectTypeReferences::Ptr bound_references_,
    std::optional<SidecarExpectationRecord> expectation_,
    AuthorityVerificationStamp::Ptr verification_stamp_)
    : kind(kind_)
    , planning_root(planning_root_)
    , replacement_root(std::move(replacement_root_))
    , transition(std::move(transition_))
    , bound_references(std::move(bound_references_))
    , expectation(std::move(expectation_))
    , verification_stamp(std::move(verification_stamp_))
{
}

PreparedDependentObjectMutation DependentObjectMutationPlanner::plan(
    const AuthorityRoot & current_root, DependentObjectMutationRequest request, const DependentObjectMutationPlannerLimits & limits)
{
    if (!request.transaction_id || !request.object.isValid() || !isOrdinaryDependentObjectKind(request.object.kind))
        fail(Error::Code::InvalidRequest, "dependent-object mutation request identity is invalid");
    if (request.object.database_uuid != current_root.getDatabaseUUID())
        fail(Error::Code::DatabaseMismatch, "dependent-object mutation belongs to another database");
    if (request.expected_database_catalog_epoch != current_root.getDatabaseCatalogEpoch())
        fail(Error::Code::ExpectedEpochMismatch, "dependent-object mutation expected database epoch is stale");
    if (current_root.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "dependent-object mutation database epoch cannot advance");
    if (current_root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(
            Error::Code::InvalidRequest,
            "dependent-object mutation requires the complete dependent-object-capable authority capability set");

    const auto prepare = [&](PlannedDependentObjectMutation planned)
    {
        return PreparedDependentObjectMutation(
            planned.kind,
            &current_root,
            std::move(planned.replacement_root),
            std::move(planned.transition),
            std::move(planned.bound_references),
            std::move(planned.expectation),
            std::move(planned.verification_stamp));
    };

    if (request.kind == DependentObjectMutationKind::AlterAdmission)
    {
        if (request.object.kind != SchemaObjectKind::Table)
            fail(Error::Code::InvalidRequest, "only mapped Tables support ALTER admission");
        return prepare(planAlterAdmission(current_root, std::move(request), limits));
    }

    auto before = validateBeforeImage(current_root, request, limits);
    switch (request.kind)
    {
        case DependentObjectMutationKind::Drop: return prepare(planDrop(current_root, std::move(request), std::move(before), limits));
        case DependentObjectMutationKind::Rename: return prepare(planRename(current_root, std::move(request), std::move(before), limits));
        case DependentObjectMutationKind::Alter: return prepare(planAlter(current_root, std::move(request), std::move(before), limits));
        case DependentObjectMutationKind::AlterAdmission: break;
    }
    fail(Error::Code::InvalidRequest, "dependent-object mutation kind is unknown");
}

}
