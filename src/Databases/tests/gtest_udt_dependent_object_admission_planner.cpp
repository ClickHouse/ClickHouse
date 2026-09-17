#include <Databases/UDT/DependentObjectAdmissionCoordinator.h>
#include <Databases/UDT/DependentObjectAdmissionPlanner.h>
#include <Databases/UDT/DependentObjectMutationCoordinator.h>
#include <Databases/UDT/DependentObjectMutationPlanner.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

SchemaObjectID objectID(SchemaObjectKind kind, UUID database_uuid, UUID object_uuid)
{
    return {.kind = kind, .database_uuid = database_uuid, .object_uuid = object_uuid};
}

DefinitionInput aliasInput(UUID database_uuid, UUID type_uuid, String local_name)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = "app." + local_name;
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return input;
}

Record definitionRecord(const Definition & definition)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE " + definition.getNormalizedName() + " AS UInt64",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = "admission fixture",
            .creation_time_us_utc = 123,
        });
}

SchemaObjectID definitionObject(const Definition & definition)
{
    return objectID(SchemaObjectKind::TypeDefinition, definition.getIdentity().database_uuid, definition.getIdentity().type_uuid);
}

AuthorityInventorySummary inventorySummary(std::span<const Record> records)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    leaves.reserve(records.size());
    for (const auto & record : records)
    {
        leaves.push_back({
        .key =
            {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
        .object_revision = record.identity.revision,
        .canonical_record_hash = computeRecordHash(record),
    });
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return buildAuthorityInventorySummary(leaves);
}

AuthorityRoot::Ptr
buildRoot(UUID database_uuid, std::span<const Definition::Ptr> definitions, std::span<const Record> records)
{
    std::vector<SchemaObjectID> nodes;
    nodes.reserve(definitions.size());
    for (const auto & definition : definitions)
        nodes.push_back(definitionObject(*definition));
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, {});
    const auto summary = inventorySummary(records);
    auto state = makeAuthorityState(
        database_uuid, 7, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    return AuthorityRootBuilder::build(state, 3, definitions, records, {}, std::move(graph));
}

InstantiatedTypeDescriptor::Ptr descriptor(const Definition::Ptr & definition)
{
    return InstantiatedTypeDescriptor::create(
        definition, CanonicalTypeArguments::validate(definition->getParameters(), {}), std::make_shared<DataTypeUInt64>());
}

BoundDeclaredTypeResult rootAlias(const InstantiatedTypeDescriptor::Ptr & logical_descriptor)
{
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = logical_descriptor->getPhysicalType()}},
        {{.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {});
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}

PreparedTableColumnTypeBindings
tableBindings(const SchemaObjectID & table, UInt64 revision, const Definition::Ptr & definition)
{
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", rootAlias(descriptor(definition))});
    columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    return prepareTableColumnTypeBindings(table, revision, columns);
}

PreparedTableColumnTypeBindings multiTableBindings(
    const SchemaObjectID & table, UInt64 revision, const std::vector<std::pair<String, Definition::Ptr>> & logical_columns)
{
    std::vector<TableColumnTypeBindingInput> columns;
    columns.reserve(logical_columns.size() + 1);
    for (const auto & [name, definition] : logical_columns)
        columns.push_back({name, rootAlias(descriptor(definition))});
    columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    return prepareTableColumnTypeBindings(table, revision, columns);
}

template <typename Callback>
void expectPlannerError(DependentObjectAdmissionPlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a dependent-object admission planner error";
    }
    catch (const DependentObjectAdmissionPlannerError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

template <typename Callback>
void expectMutationPlannerError(DependentObjectMutationPlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a dependent-object mutation planner error";
    }
    catch (const DependentObjectMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

const String & artifactBytes(
    const DatabaseSchemaWALValidatedTransition & transition,
    DatabaseSchemaWALStagedArtifactKind kind,
    const SchemaObjectID & object,
    DatabaseSchemaWALStagedArtifactImage image = DatabaseSchemaWALStagedArtifactImage::After)
{
    const auto & artifacts = transition.getPrepare().staged_artifacts;
    const auto bytes = transition.getStagedArtifactBytes();
    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        if (artifacts[index].kind == kind && artifacts[index].image == image && artifacts[index].object == object)
            return bytes[index];
    }
    throw std::logic_error("artifact not found");
}

std::vector<String> copyArtifactBytes(const DatabaseSchemaWALValidatedTransition & transition)
{
    const auto bytes = transition.getStagedArtifactBytes();
    return {bytes.begin(), bytes.end()};
}

class ExactTableMetadataValidator final : public IDependentTableMetadataValidator
{
public:
    explicit ExactTableMetadataValidator(String accepted_metadata_)
        : accepted_metadata(std::move(accepted_metadata_))
    {
    }

    bool return_wrong_object = false;

protected:
    DecodedTableMetadata
    decodeAndCanonicalize(std::string_view candidate_metadata_bytes, std::string_view canonical_sidecar_bytes) const override
    {
        if (candidate_metadata_bytes != accepted_metadata)
            throw std::runtime_error("metadata is not the exact canonical table image");
        const auto references = decodePersistedTypeReferences(canonical_sidecar_bytes);
        SchemaObjectID object = references.object;
        if (return_wrong_object)
            object.object_uuid = uuid(0xdead, 0xbeef);
        return {
            .object = object,
            .object_schema_revision = references.object_schema_revision,
            .object_name = "events",
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = references.physical_schema_fingerprint,
            .canonical_metadata_bytes = accepted_metadata,
        };
    }

private:
    String accepted_metadata;
};

struct Fixture
{
    UUID database_uuid = uuid(1, 2);
    SchemaObjectID table = objectID(SchemaObjectKind::Table, database_uuid, uuid(3, 4));
    std::vector<Definition::Ptr> definitions;
    std::vector<Record> records;
    AuthorityRoot::Ptr root;
    PreparedTableColumnTypeBindings bindings;
    String metadata = "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id "
                      "UInt64, label String)";
    ExactTableMetadataValidator metadata_validator{metadata};

    Fixture()
    {
        definitions = TemplateChecker::checkAll({aliasInput(database_uuid, uuid(0x10, 1), "UserId")});
        records = {definitionRecord(*definitions.front())};
        root = buildRoot(database_uuid, definitions, records);
        bindings = tableBindings(table, 1, definitions.front());
    }

    DependentObjectMetadataInstallationRecord installationRecord() const
    {
        return {
            .object = table,
            .object_schema_revision = 1,
            .object_name = "events",
            .metadata_artifact_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata),
        };
    }

    SidecarExpectationRecord durableExpectation() const
    {
        auto result = *bindings.sidecar_expectation;
        result.installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installationRecord());
        return result;
    }
};

struct MutationFixture : Fixture
{
    AuthorityRoot::Ptr mapped_root;
    PreparedTableColumnTypeBindings alternate_bindings;
    AtomicDatabaseSchemaMutationDependentObjectImage before_image;

    MutationFixture()
    {
        definitions = TemplateChecker::checkAll({
            aliasInput(database_uuid, uuid(0x10, 1), "UserId"),
            aliasInput(database_uuid, uuid(0x10, 2), "AccountId"),
        });
        records.clear();
        for (const auto & definition : definitions)
            records.push_back(definitionRecord(*definition));
        root = buildRoot(database_uuid, definitions, records);
        bindings = tableBindings(table, 1, definitions[0]);
        alternate_bindings = tableBindings(table, 2, definitions[1]);

        auto admission
            = DependentObjectAdmissionPlanner::planTableCreate(*root, 4'900, 7, bindings, metadata, metadata_validator);
        mapped_root = admission.releaseReplacementRoot();
        const auto * expectation = mapped_root->findExpectationRecord(table);
        if (!expectation || !bindings.persisted_references)
            throw std::logic_error("failed to construct mapped-table mutation fixture");
        before_image = {
            .expectation = *expectation,
            .object_name = "events",
            .canonical_metadata_bytes = metadata,
            .canonical_sidecar_bytes = encodePersistedTypeReferences(*bindings.persisted_references),
            .canonical_installation_record_bytes = encodeDependentObjectMetadataInstallationRecord(installationRecord()),
        };
    }

    DependentObjectMutationRequest request(DependentObjectMutationKind kind, UInt64 transaction_id) const
    {
        DependentObjectMutationRequest result;
        result.kind = kind;
        result.object = table;
        result.transaction_id = transaction_id;
        result.expected_database_catalog_epoch = mapped_root->getDatabaseCatalogEpoch();
        result.before_image = before_image;
        result.physical_columns = bindings.physical_columns;
        return result;
    }

    DependentObjectMutationRequest mappedToAlternate(UInt64 transaction_id) const
    {
        auto result = request(DependentObjectMutationKind::Alter, transaction_id);
        result.after_canonical_metadata_bytes = metadata;
        result.physical_columns = alternate_bindings.physical_columns;
        result.after_persisted_references = *alternate_bindings.persisted_references;
        return result;
    }
};

class DurableStorage final : public IDatabaseSchemaMutationDurableStorage
{
public:
    enum class Failure
    {
        None,
        FinishStaging,
        Install,
        Commit,
        CommitAfter,
    };

    DurableStorage(AuthorityState current_state_, UInt64 predecessor_)
        : current_state(std::move(current_state_))
        , predecessor(predecessor_)
    {
    }

    void validateMutationGuardAndDurablePredecessor(
        const DatabaseSchemaMutationGuard & guard,
        const std::optional<AuthorityState> & expected_preceding_authority_state,
        UInt64 transaction_id) override
    {
        events.emplace_back("validate");
        if (guard.getDatabaseUUID() != current_state.database_uuid || guard.getDurablePredecessorTransactionID() != predecessor
            || expected_preceding_authority_state != current_state || transaction_id <= predecessor)
            throw std::runtime_error("invalid predecessor");
    }

    void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard &, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept override
    {
        recovery_required = true;
        recovery_transaction_id = transaction_id;
        recovery_phase = phase;
    }

    void validateRecoveryGuard(const DatabaseSchemaMutationGuard &, UInt64 transaction_id) override
    {
        events.emplace_back("validate-recovery");
        if (!prepare || prepare->transaction_id != transaction_id)
            throw std::runtime_error("invalid recovery transaction");
    }

    void
    stageArtifact(const DatabaseSchemaWALStagedArtifactLocator &, const DatabaseSchemaWALStagedArtifactRef &, std::string_view) override
    {
        events.emplace_back("stage");
    }

    void finishStaging(UUID, UInt64) override
    {
        events.emplace_back("finish-staging");
        if (failure == Failure::FinishStaging)
            throw std::runtime_error("finish staging failed");
    }

    void persistPrepare(UInt64, std::string_view canonical_prepare) override
    {
        events.emplace_back("prepare");
        prepare = decodeDatabaseSchemaWALPrepare(canonical_prepare);
    }

    void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view) override
    {
        events.emplace_back("install");
        if (failure == Failure::Install)
            throw std::runtime_error("install failed");
        installed_kinds.push_back(artifact.kind);
    }

    void removeArtifact(DatabaseSchemaWALStagedArtifactKind, const SchemaObjectID &) override { events.emplace_back("remove"); }

    void finishInstallation(UUID, UInt64) override { events.emplace_back("finish-installation"); }

    void persistCommit(UInt64, std::string_view canonical_commit) override
    {
        events.emplace_back("commit");
        if (failure == Failure::Commit)
            throw std::runtime_error("commit failed");
        commit = decodeDatabaseSchemaWALCommit(canonical_commit);
        if (!prepare)
            throw std::runtime_error("commit before prepare");
        current_state = prepare->after_authority_state;
        if (failure == Failure::CommitAfter)
            throw std::runtime_error("commit completion was not observed");
    }

    void finishRecovery(UUID, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision) override
    {
        events.emplace_back("finish-recovery");
        if (!prepare || prepare->transaction_id != transaction_id)
            throw std::runtime_error("invalid recovery completion");
        recovery_decision = decision;
        current_state = decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted ? prepare->after_authority_state
                                                                                           : *prepare->before_authority_state;
        recovery_required = false;
    }

    void discardUnpreparedStaging(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        events.emplace_back("discard");
        discarded = true;
    }

    void retireRolledBackTransaction(const DatabaseSchemaMutationGuard &, UInt64) override
    {
        events.emplace_back("retire-rollback");
        retired_rollback = true;
    }

    void persistValidatedCheckpoint(
        const DatabaseSchemaMutationGuard &,
        const DatabaseSchemaWALCheckpoint &,
        std::string_view,
        std::string_view,
        std::string_view) override
    {
        throw std::runtime_error("unused checkpoint");
    }

    void compactThroughValidatedCheckpoint(const DatabaseSchemaMutationGuard &, const DatabaseSchemaWALCheckpoint &) override
    {
        throw std::runtime_error("unused checkpoint");
    }

    AuthorityState current_state;
    UInt64 predecessor;
    Failure failure = Failure::None;
    std::vector<String> events;
    std::vector<DatabaseSchemaWALStagedArtifactKind> installed_kinds;
    std::optional<DatabaseSchemaWALPrepare> prepare;
    std::optional<DatabaseSchemaWALCommit> commit;
    std::optional<DatabaseSchemaWALRecoveryDecision> recovery_decision;
    bool discarded = false;
    bool retired_rollback = false;
    bool recovery_required = false;
    UInt64 recovery_transaction_id = 0;
    DatabaseSchemaMutationDurabilityPhase recovery_phase = DatabaseSchemaMutationDurabilityPhase::PrepareMarker;
};

} // namespace

TEST(DependentObjectAdmissionPlanner, TableCreateBuildsExactAfterOnlyTransitionAndReboundPublication)
{
    Fixture fixture;
    auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
        *fixture.root, 1001, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_TRUE(prepare.before_authority_state);
    EXPECT_EQ(*prepare.before_authority_state, fixture.root->getAuthorityState());
    EXPECT_EQ(prepare.transaction_id, 1001);

    ASSERT_EQ(prepare.authority_record_deltas.size(), 1);
    EXPECT_FALSE(prepare.authority_record_deltas.front().before);
    ASSERT_TRUE(prepare.authority_record_deltas.front().after);
    EXPECT_EQ(prepare.authority_record_deltas.front().key.record_kind, AuthorityInventoryRecordKind::SidecarExpectation);
    EXPECT_EQ(prepare.authority_record_deltas.front().key.object_uuid, fixture.table.object_uuid);

    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1);
    const auto & object_delta = prepare.dependent_object_deltas.front();
    EXPECT_EQ(object_delta.object, fixture.table);
    EXPECT_FALSE(object_delta.before);
    ASSERT_TRUE(object_delta.after);
    EXPECT_EQ(object_delta.after->object_schema_revision, 1);
    EXPECT_TRUE(object_delta.after->sidecar_record_hash);
    EXPECT_TRUE(object_delta.after->expectation_record_hash);

    ASSERT_EQ(prepare.graph_delta.node_additions, std::vector<SchemaObjectID>({fixture.table}));
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());
    ASSERT_EQ(prepare.graph_delta.edge_additions, fixture.bindings.dependency_edges);
    EXPECT_TRUE(prepare.graph_delta.edge_removals.empty());
    ASSERT_EQ(prepare.staged_artifacts.size(), 4);
    for (const auto & artifact : prepare.staged_artifacts)
        EXPECT_EQ(artifact.image, DatabaseSchemaWALStagedArtifactImage::After);

    EXPECT_EQ(artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, fixture.table), fixture.metadata);
    EXPECT_EQ(
        decodePersistedTypeReferences(
            artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, fixture.table)),
        *fixture.bindings.persisted_references);
    EXPECT_EQ(
        decodeSidecarExpectationRecord(
            artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord, fixture.table)),
        fixture.durableExpectation());
    EXPECT_EQ(
        decodeDependentObjectMetadataInstallationRecord(
            artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, fixture.table)),
        fixture.installationRecord());

    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(replacement.getDatabaseCatalogEpoch(), 8);
    EXPECT_TRUE(replacement.sharesDefinitionContentWith(*fixture.root));
    ASSERT_EQ(replacement.getExpectationRecordCount(), 1);
    ASSERT_TRUE(replacement.findExpectationRecord(fixture.table));
    EXPECT_EQ(*replacement.findExpectationRecord(fixture.table), fixture.durableExpectation());
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.table));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));

    const auto & rebound = prepared.getBoundUDTReferences();
    ASSERT_TRUE(rebound);
    EXPECT_EQ(rebound->getObject(), fixture.table);
    EXPECT_EQ(rebound->getObjectSchemaRevision(), 1);
    EXPECT_EQ(rebound->getSidecarHash(), fixture.bindings.sidecar_expectation->sidecar_hash);
    ASSERT_EQ(rebound->getDefinitionHandles().size(), 1);
    EXPECT_EQ(rebound->getDefinitionHandles().front(), fixture.definitions.front());

    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = fixture.root->getAuthorityState(),
            .authority_inventory = fixture.root->pinAuthorityInventory(),
            .schema_graph = fixture.root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(decoded.getPrepare(), prepare);
    EXPECT_EQ(decoded.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());

    auto mismatched_prepare = prepare;
    auto mismatched_bytes = copyArtifactBytes(transition);
    const auto installation_it = std::find_if(
        mismatched_prepare.staged_artifacts.begin(),
        mismatched_prepare.staged_artifacts.end(),
        [](const auto & artifact)
        { return artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord; });
    ASSERT_NE(installation_it, mismatched_prepare.staged_artifacts.end());
    const size_t installation_index = static_cast<size_t>(installation_it - mismatched_prepare.staged_artifacts.begin());
    auto mismatched_installation = decodeDependentObjectMetadataInstallationRecord(mismatched_bytes[installation_index]);
    mismatched_installation.object_name = "another_name";
    mismatched_bytes[installation_index] = encodeDependentObjectMetadataInstallationRecord(mismatched_installation);
    installation_it->byte_size = mismatched_bytes[installation_index].size();
    installation_it->content_hash
        = computeDatabaseSchemaWALStagedArtifactHash(installation_it->kind, mismatched_bytes[installation_index]);
    mismatched_prepare.prepare_hash = computeDatabaseSchemaWALPrepareHash(mismatched_prepare);
    try
    {
        static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(
            std::move(mismatched_prepare),
            {
                .authority_state = fixture.root->getAuthorityState(),
                .authority_inventory = fixture.root->pinAuthorityInventory(),
                .schema_graph = fixture.root->pinSchemaObjectDependencyGraph(),
            },
            std::move(mismatched_bytes)));
        FAIL() << "expected the installation-record hash chain to reject another "
                  "object name";
    }
    catch (const DatabaseSchemaWALError & error)
    {
        EXPECT_EQ(error.code, DatabaseSchemaWALError::Code::ArtifactMismatch) << error.what();
    }
}

TEST(DependentObjectAdmissionPlanner, RejectsMutatedPreparedPackageAndUnpinnedDefinition)
{
    Fixture fixture;

    auto wrong_expectation = fixture.bindings;
    wrong_expectation.sidecar_expectation->sidecar_hash.front() ^= 0xff;
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::IntegrityMismatch,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2001, 7, wrong_expectation, fixture.metadata, fixture.metadata_validator));
        });

    auto wrong_edges = fixture.bindings;
    wrong_edges.dependency_edges.clear();
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::DependencyMismatch,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2002, 7, wrong_edges, fixture.metadata, fixture.metadata_validator));
        });

    auto wrong_bound_schema = fixture.bindings;
    wrong_bound_schema.bound_physical_schema->occurrences.front().physical_type = std::make_shared<DataTypeString>();
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::InvalidBindings,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2003, 7, wrong_bound_schema, fixture.metadata, fixture.metadata_validator));
        });

    const auto absent_definition = TemplateChecker::checkAll({aliasInput(fixture.database_uuid, uuid(0x20, 1), "Absent")}).front();
    const auto absent_bindings = tableBindings(fixture.table, 1, absent_definition);
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::DefinitionNotFound,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2004, 7, absent_bindings, fixture.metadata, fixture.metadata_validator));
        });

    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::InvalidMetadata,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2005, 7, fixture.bindings, "not canonical table metadata", fixture.metadata_validator));
        });

    fixture.metadata_validator.return_wrong_object = true;
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::InvalidMetadata,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 2006, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator));
        });
}

TEST(DependentObjectAdmissionPlanner, IsCreateOnlyEpochBoundAndDeterministic)
{
    Fixture fixture;

    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::ExpectedEpochMismatch,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 3001, 6, fixture.bindings, fixture.metadata, fixture.metadata_validator));
        });
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::InvalidRequest,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 3002, 7, fixture.bindings, {}, fixture.metadata_validator));
        });

    const auto revision_two = tableBindings(fixture.table, 2, fixture.definitions.front());
    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::InvalidRevision,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                *fixture.root, 3003, 7, revision_two, fixture.metadata, fixture.metadata_validator));
        });

    auto first = DependentObjectAdmissionPlanner::planTableCreate(
        *fixture.root, 3004, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    auto same = DependentObjectAdmissionPlanner::planTableCreate(
        *fixture.root, 3004, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    EXPECT_EQ(first.getValidatedTransition().getPrepare(), same.getValidatedTransition().getPrepare());
    EXPECT_EQ(copyArtifactBytes(first.getValidatedTransition()), copyArtifactBytes(same.getValidatedTransition()));

    expectPlannerError(
        DependentObjectAdmissionPlannerError::Code::ObjectAlreadyExists,
        [&]
        {
            static_cast<void>(DependentObjectAdmissionPlanner::planTableCreate(
                first.getReplacementRoot(), 3005, 8, fixture.bindings, fixture.metadata, fixture.metadata_validator));
        });
}

TEST(DependentObjectMutationPlanner, MappedRenamePreservesIdentityAndBuildsReplayableDualInstallationImages)
{
    MutationFixture fixture;
    auto request = fixture.request(DependentObjectMutationKind::Rename, 5'001);
    request.after_object_name = "renamed_events";
    auto prepared = DependentObjectMutationPlanner::plan(*fixture.mapped_root, std::move(request));

    EXPECT_EQ(prepared.getKind(), DependentObjectMutationKind::Rename);
    ASSERT_TRUE(prepared.getBoundUDTReferences());
    EXPECT_EQ(prepared.getBoundUDTReferences()->getObject(), fixture.table);
    EXPECT_EQ(prepared.getBoundUDTReferences()->getObjectSchemaRevision(), 2u);
    ASSERT_TRUE(prepared.getSidecarExpectation());
    EXPECT_EQ(prepared.getSidecarExpectation()->object_schema_revision, 2u);
    EXPECT_EQ(prepared.getSidecarExpectation()->physical_schema_fingerprint, fixture.before_image.expectation.physical_schema_fingerprint);

    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(replacement.getDatabaseCatalogEpoch(), fixture.mapped_root->getDatabaseCatalogEpoch() + 1);
    ASSERT_TRUE(replacement.findExpectationRecord(fixture.table));
    EXPECT_EQ(*replacement.findExpectationRecord(fixture.table), *prepared.getSidecarExpectation());
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    EXPECT_TRUE(prepare.graph_delta.node_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_removals.empty());
    ASSERT_EQ(prepare.staged_artifacts.size(), 8u);
    const auto before_installation = decodeDependentObjectMetadataInstallationRecord(artifactBytes(
        transition,
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
        fixture.table,
        DatabaseSchemaWALStagedArtifactImage::Before));
    const auto after_installation = decodeDependentObjectMetadataInstallationRecord(artifactBytes(
        transition,
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
        fixture.table,
        DatabaseSchemaWALStagedArtifactImage::After));
    EXPECT_EQ(before_installation.object, after_installation.object);
    EXPECT_EQ(before_installation.object_schema_revision, 1u);
    EXPECT_EQ(after_installation.object_schema_revision, 2u);
    EXPECT_EQ(before_installation.object_name, "events");
    EXPECT_EQ(after_installation.object_name, "renamed_events");
    EXPECT_EQ(before_installation.metadata_artifact_hash, after_installation.metadata_artifact_hash);

    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = fixture.mapped_root->getAuthorityState(),
            .authority_inventory = fixture.mapped_root->pinAuthorityInventory(),
            .schema_graph = fixture.mapped_root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(decoded.getPrepare(), prepare);
    EXPECT_EQ(decoded.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());
}

TEST(DependentObjectMutationPlanner, MappedAlterAtomicallyReplacesItsDefinitionEdgeAndRejectsAStaleBeforeImage)
{
    MutationFixture fixture;
    auto prepared = DependentObjectMutationPlanner::plan(*fixture.mapped_root, fixture.mappedToAlternate(5'101));

    EXPECT_EQ(prepared.getKind(), DependentObjectMutationKind::Alter);
    ASSERT_TRUE(prepared.getBoundUDTReferences());
    ASSERT_EQ(prepared.getBoundUDTReferences()->getDefinitionHandles().size(), 1u);
    EXPECT_EQ(prepared.getBoundUDTReferences()->getDefinitionHandles().front(), fixture.definitions[1]);
    ASSERT_TRUE(prepared.getSidecarExpectation());
    EXPECT_EQ(prepared.getSidecarExpectation()->object_schema_revision, 2u);

    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    ASSERT_EQ(prepare.graph_delta.edge_removals.size(), 1u);
    ASSERT_EQ(prepare.graph_delta.edge_additions.size(), 1u);
    EXPECT_EQ(prepare.graph_delta.edge_removals.front(), fixture.bindings.dependency_edges.front());
    EXPECT_EQ(prepare.graph_delta.edge_additions.front(), fixture.alternate_bindings.dependency_edges.front());
    EXPECT_TRUE(prepare.graph_delta.node_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());

    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    EXPECT_EQ(
        decodePersistedTypeReferences(artifactBytes(
            prepared.getValidatedTransition(), DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, fixture.table)),
        *fixture.alternate_bindings.persisted_references);

    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = fixture.mapped_root->getAuthorityState(),
            .authority_inventory = fixture.mapped_root->pinAuthorityInventory(),
            .schema_graph = fixture.mapped_root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(prepared.getValidatedTransition()));
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());

    auto stale = fixture.mappedToAlternate(5'102);
    stale.before_image.canonical_metadata_bytes.push_back(' ');
    expectMutationPlannerError(
        DependentObjectMutationPlannerError::Code::StaleImage,
        [&] { static_cast<void>(DependentObjectMutationPlanner::plan(*fixture.mapped_root, std::move(stale))); });
    EXPECT_TRUE(fixture.mapped_root->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
    EXPECT_FALSE(fixture.mapped_root->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
}

TEST(DependentObjectMutationPlanner, MultiUDTAdmissionAndAlterReplaceTheExactDefinitionEdgeSetAtomically)
{
    Fixture fixture;
    fixture.definitions = TemplateChecker::checkAll({
        aliasInput(fixture.database_uuid, uuid(0x10, 1), "UserId"),
        aliasInput(fixture.database_uuid, uuid(0x10, 2), "AccountId"),
        aliasInput(fixture.database_uuid, uuid(0x10, 3), "RegionId"),
    });
    fixture.records.clear();
    for (const auto & definition : fixture.definitions)
        fixture.records.push_back(definitionRecord(*definition));
    fixture.root = buildRoot(fixture.database_uuid, fixture.definitions, fixture.records);

    const auto before_bindings = multiTableBindings(
        fixture.table,
        1,
        {
            {"owner_id", fixture.definitions[0]},
            {"delegate_id", fixture.definitions[0]},
            {"account_id", fixture.definitions[1]},
        });
    ASSERT_TRUE(before_bindings.persisted_references);
    ASSERT_EQ(before_bindings.persisted_references->descriptors.size(), 2u);
    ASSERT_EQ(before_bindings.persisted_references->uses.size(), 3u);
    ASSERT_EQ(before_bindings.dependency_edges.size(), 2u);

    auto admission = DependentObjectAdmissionPlanner::planTableCreate(
        *fixture.root, 5'151, 7, before_bindings, fixture.metadata, fixture.metadata_validator);
    EXPECT_EQ(admission.getValidatedTransition().getPrepare().graph_delta.edge_additions, before_bindings.dependency_edges);
    ASSERT_TRUE(admission.getBoundUDTReferences());
    EXPECT_EQ(admission.getBoundUDTReferences()->getDefinitionHandles().size(), 2u);
    auto mapped_root = admission.releaseReplacementRoot();
    ASSERT_TRUE(mapped_root);

    const auto after_bindings = multiTableBindings(
        fixture.table,
        2,
        {
            {"owner_id", fixture.definitions[1]},
            {"delegate_id", fixture.definitions[1]},
            {"account_id", fixture.definitions[2]},
        });
    ASSERT_TRUE(after_bindings.persisted_references);
    ASSERT_EQ(after_bindings.persisted_references->descriptors.size(), 2u);
    ASSERT_EQ(after_bindings.persisted_references->uses.size(), 3u);
    ASSERT_EQ(after_bindings.dependency_edges.size(), 2u);

    const auto * expectation = mapped_root->findExpectationRecord(fixture.table);
    ASSERT_NE(expectation, nullptr);
    DependentObjectMutationRequest request;
    request.kind = DependentObjectMutationKind::Alter;
    request.object = fixture.table;
    request.transaction_id = 5'152;
    request.expected_database_catalog_epoch = mapped_root->getDatabaseCatalogEpoch();
    request.before_image = {
        .expectation = *expectation,
        .object_name = "events",
        .canonical_metadata_bytes = fixture.metadata,
        .canonical_sidecar_bytes = encodePersistedTypeReferences(*before_bindings.persisted_references),
        .canonical_installation_record_bytes = encodeDependentObjectMetadataInstallationRecord(fixture.installationRecord()),
    };
    request.physical_columns = after_bindings.physical_columns;
    request.after_canonical_metadata_bytes = fixture.metadata;
    request.after_persisted_references = *after_bindings.persisted_references;

    auto prepared = DependentObjectMutationPlanner::plan(*mapped_root, std::move(request));
    const SchemaObjectDependencyEdge removed{
        .dependent = fixture.table,
        .dependency = definitionObject(*fixture.definitions[0]),
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge retained{
        .dependent = fixture.table,
        .dependency = definitionObject(*fixture.definitions[1]),
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge added{
        .dependent = fixture.table,
        .dependency = definitionObject(*fixture.definitions[2]),
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };

    const auto & graph_delta = prepared.getValidatedTransition().getPrepare().graph_delta;
    EXPECT_EQ(graph_delta.edge_removals, std::vector<SchemaObjectDependencyEdge>({removed}));
    EXPECT_EQ(graph_delta.edge_additions, std::vector<SchemaObjectDependencyEdge>({added}));
    EXPECT_TRUE(graph_delta.node_additions.empty());
    EXPECT_TRUE(graph_delta.node_removals.empty());

    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsEdge(removed));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(retained));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(added));
    EXPECT_TRUE(mapped_root->getSchemaObjectDependencyGraph().containsEdge(removed));
    EXPECT_TRUE(mapped_root->getSchemaObjectDependencyGraph().containsEdge(retained));
    EXPECT_FALSE(mapped_root->getSchemaObjectDependencyGraph().containsEdge(added));

    const auto persisted = decodePersistedTypeReferences(artifactBytes(
        prepared.getValidatedTransition(), DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, fixture.table));
    EXPECT_EQ(persisted, *after_bindings.persisted_references);
    ASSERT_TRUE(prepared.getBoundUDTReferences());
    EXPECT_EQ(prepared.getBoundUDTReferences()->getDefinitionHandles().size(), 2u);
}

TEST(DependentObjectMutationPlanner, RemovingTheLastLogicalOccurrenceProducesExactPhysicalOnlyReplay)
{
    MutationFixture fixture;
    auto request = fixture.request(DependentObjectMutationKind::Alter, 5'201);
    request.after_canonical_metadata_bytes = fixture.metadata;
    auto prepared = DependentObjectMutationPlanner::plan(*fixture.mapped_root, std::move(request));

    EXPECT_EQ(prepared.getKind(), DependentObjectMutationKind::Alter);
    EXPECT_FALSE(prepared.getBoundUDTReferences());
    EXPECT_FALSE(prepared.getSidecarExpectation());
    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(replacement.findExpectationRecord(fixture.table), nullptr);
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.table));
    EXPECT_TRUE(replacement.findByIdentity(fixture.definitions[0]->getIdentity()));
    EXPECT_TRUE(replacement.findByIdentity(fixture.definitions[1]->getIdentity()));

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1u);
    ASSERT_TRUE(prepare.dependent_object_deltas.front().before);
    ASSERT_TRUE(prepare.dependent_object_deltas.front().after);
    EXPECT_FALSE(prepare.dependent_object_deltas.front().after->sidecar_record_hash);
    EXPECT_FALSE(prepare.dependent_object_deltas.front().after->expectation_record_hash);
    EXPECT_EQ(prepare.graph_delta.node_removals, std::vector<SchemaObjectID>({fixture.table}));
    EXPECT_EQ(prepare.graph_delta.edge_removals, fixture.bindings.dependency_edges);
    ASSERT_EQ(prepare.staged_artifacts.size(), 5u);
    EXPECT_EQ(artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, fixture.table), fixture.metadata);

    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = fixture.mapped_root->getAuthorityState(),
            .authority_inventory = fixture.mapped_root->pinAuthorityInventory(),
            .schema_graph = fixture.mapped_root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(decoded.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());
}

TEST(DependentObjectMutationPlanner, MappedDropRemovesOnlyTheTableAuthorityStateAndReplays)
{
    MutationFixture fixture;
    auto request = fixture.request(DependentObjectMutationKind::Drop, 5'225);
    auto prepared = DependentObjectMutationPlanner::plan(*fixture.mapped_root, std::move(request));

    EXPECT_EQ(prepared.getKind(), DependentObjectMutationKind::Drop);
    EXPECT_FALSE(prepared.getBoundUDTReferences());
    EXPECT_FALSE(prepared.getSidecarExpectation());
    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(replacement.findExpectationRecord(fixture.table), nullptr);
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.table));
    EXPECT_TRUE(replacement.findByIdentity(fixture.definitions[0]->getIdentity()));
    EXPECT_TRUE(replacement.findByIdentity(fixture.definitions[1]->getIdentity()));

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_EQ(prepare.authority_record_deltas.size(), 1u);
    ASSERT_TRUE(prepare.authority_record_deltas.front().before);
    EXPECT_FALSE(prepare.authority_record_deltas.front().after);
    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1u);
    ASSERT_TRUE(prepare.dependent_object_deltas.front().before);
    EXPECT_FALSE(prepare.dependent_object_deltas.front().after);
    EXPECT_EQ(prepare.graph_delta.node_removals, std::vector<SchemaObjectID>({fixture.table}));
    EXPECT_EQ(prepare.graph_delta.edge_removals, fixture.bindings.dependency_edges);
    ASSERT_EQ(prepare.staged_artifacts.size(), 4u);
    for (const auto & artifact : prepare.staged_artifacts)
        EXPECT_EQ(artifact.image, DatabaseSchemaWALStagedArtifactImage::Before);

    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = fixture.mapped_root->getAuthorityState(),
            .authority_inventory = fixture.mapped_root->pinAuthorityInventory(),
            .schema_graph = fixture.mapped_root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(decoded.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());
}

TEST(DependentObjectMutationPlanner, SameUUIDReadmissionStartsANewMappedIncarnationAtRevisionTwo)
{
    MutationFixture fixture;
    auto physicalize_request = fixture.request(DependentObjectMutationKind::Alter, 5'251);
    physicalize_request.after_canonical_metadata_bytes = fixture.metadata;
    auto physicalize = DependentObjectMutationPlanner::plan(*fixture.mapped_root, std::move(physicalize_request));
    auto physical_root = physicalize.releaseReplacementRoot();
    ASSERT_TRUE(physical_root);
    EXPECT_EQ(physical_root->findExpectationRecord(fixture.table), nullptr);
    EXPECT_FALSE(physical_root->getSchemaObjectDependencyGraph().containsNode(fixture.table));

    auto readmission_bindings = tableBindings(fixture.table, 2, fixture.definitions[0]);
    ASSERT_TRUE(readmission_bindings.persisted_references);
    DependentObjectMutationRequest readmission;
    readmission.kind = DependentObjectMutationKind::AlterAdmission;
    readmission.object = fixture.table;
    readmission.transaction_id = 5'252;
    readmission.expected_database_catalog_epoch = physical_root->getDatabaseCatalogEpoch();
    readmission.physical_before_object_name = "events";
    readmission.physical_before_canonical_metadata_bytes = fixture.metadata;
    readmission.after_canonical_metadata_bytes = fixture.metadata;
    readmission.physical_columns = readmission_bindings.physical_columns;
    readmission.after_persisted_references = *readmission_bindings.persisted_references;
    auto prepared = DependentObjectMutationPlanner::plan(*physical_root, std::move(readmission));

    EXPECT_EQ(prepared.getKind(), DependentObjectMutationKind::AlterAdmission);
    ASSERT_TRUE(prepared.getBoundUDTReferences());
    EXPECT_EQ(prepared.getBoundUDTReferences()->getObjectSchemaRevision(), 2u);
    ASSERT_EQ(prepared.getBoundUDTReferences()->getDefinitionHandles().size(), 1u);
    EXPECT_EQ(prepared.getBoundUDTReferences()->getDefinitionHandles().front(), fixture.definitions[0]);
    ASSERT_TRUE(prepared.getSidecarExpectation());
    EXPECT_EQ(prepared.getSidecarExpectation()->object_schema_revision, 2u);

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1u);
    ASSERT_TRUE(prepare.dependent_object_deltas.front().before);
    ASSERT_TRUE(prepare.dependent_object_deltas.front().after);
    EXPECT_EQ(prepare.dependent_object_deltas.front().before->object_schema_revision, 1u);
    EXPECT_FALSE(prepare.dependent_object_deltas.front().before->sidecar_record_hash);
    EXPECT_FALSE(prepare.dependent_object_deltas.front().before->expectation_record_hash);
    EXPECT_EQ(prepare.dependent_object_deltas.front().after->object_schema_revision, 2u);
    EXPECT_TRUE(prepare.dependent_object_deltas.front().after->sidecar_record_hash);
    EXPECT_TRUE(prepare.dependent_object_deltas.front().after->expectation_record_hash);
    EXPECT_EQ(prepare.graph_delta.node_additions, std::vector<SchemaObjectID>({fixture.table}));
    EXPECT_EQ(prepare.graph_delta.edge_additions, fixture.bindings.dependency_edges);
    ASSERT_EQ(prepare.staged_artifacts.size(), 5u);

    const auto installation = decodeDependentObjectMetadataInstallationRecord(
        artifactBytes(transition, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, fixture.table));
    EXPECT_EQ(installation.object, fixture.table);
    EXPECT_EQ(installation.object_schema_revision, 2u);
    EXPECT_EQ(installation.object_name, "events");

    const auto & replacement = prepared.getReplacementRoot();
    ASSERT_TRUE(replacement.findExpectationRecord(fixture.table));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
    auto decoded = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        {
            .authority_state = physical_root->getAuthorityState(),
            .authority_inventory = physical_root->pinAuthorityInventory(),
            .schema_graph = physical_root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(decoded.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(decoded.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());
}

TEST(DependentObjectMutationCoordinator, MappedAlterCommitsDurablyBeforePublishingTheReplacementRoot)
{
    MutationFixture fixture;
    auto request = fixture.mappedToAlternate(5'301);
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.mapped_root));
    auto planning_root = authority.acquireCurrentRoot();
    ASSERT_TRUE(planning_root);
    auto prepared = DependentObjectMutationPlanner::plan(planning_root.get(), std::move(request));
    DurableStorage storage(planning_root->getAuthorityState(), 5'300);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 51, 5'300);

    auto prepared_commit = DependentObjectMutationCoordinator::prepareCommit(
        std::move(planning_root), authority, storage, guard, std::move(prepared));
    EXPECT_EQ(storage.events, std::vector<String>({"validate"}));
    {
        auto still_before = authority.acquireCurrentRoot();
        ASSERT_TRUE(still_before);
        EXPECT_TRUE(still_before->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
        EXPECT_FALSE(still_before->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    }

    auto durable = DependentObjectMutationCoordinator::commitDurably(storage, guard, std::move(prepared_commit));
    ASSERT_TRUE(storage.commit);
    EXPECT_EQ(storage.commit->transaction_id, 5'301u);
    {
        auto still_unpublished = authority.acquireCurrentRoot();
        ASSERT_TRUE(still_unpublished);
        EXPECT_TRUE(still_unpublished->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
        EXPECT_FALSE(still_unpublished->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    }

    auto published = DependentObjectMutationCoordinator::publish(authority, std::move(durable));
    EXPECT_EQ(published.kind, DependentObjectMutationKind::Alter);
    EXPECT_EQ(published.commit.transaction_id, 5'301u);
    ASSERT_TRUE(published.bound_references);
    ASSERT_TRUE(published.expectation);
    EXPECT_EQ(published.expectation->object_schema_revision, 2u);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_FALSE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
    EXPECT_TRUE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    EXPECT_EQ(authority.scanRetired().retired_root_count, 0u);
}

TEST(DependentObjectMutationCoordinator, AlterRecoveryRollsBackPreparedAndCompletesDurableCommitWithoutEarlyPublication)
{
    {
        MutationFixture fixture;
        auto request = fixture.mappedToAlternate(5'351);
        AtomicAuthority authority(
            fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.mapped_root));
        auto planning_root = authority.acquireCurrentRoot();
        ASSERT_TRUE(planning_root);
        auto prepared = DependentObjectMutationPlanner::plan(planning_root.get(), std::move(request));
        DurableStorage storage(planning_root->getAuthorityState(), 5'350);
        auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 52, 5'350);
        auto prepared_commit = DependentObjectMutationCoordinator::prepareCommit(
            std::move(planning_root), authority, storage, guard, std::move(prepared));
        storage.failure = DurableStorage::Failure::Install;

        EXPECT_THROW(
            static_cast<void>(
                DependentObjectMutationCoordinator::commitDurably(storage, guard, std::move(prepared_commit))),
            DatabaseSchemaMutationIndeterminateDurabilityError);
        EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::AfterImage);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);

        storage.failure = DurableStorage::Failure::None;
        auto recovered
            = DependentObjectMutationCoordinator::recoverDurably(storage, guard, std::move(prepared_commit), std::nullopt);
        EXPECT_FALSE(recovered);
        ASSERT_TRUE(storage.recovery_decision);
        EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        EXPECT_TRUE(storage.retired_rollback);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
        auto after = authority.acquireCurrentRoot();
        ASSERT_TRUE(after);
        EXPECT_TRUE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
        EXPECT_FALSE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    }

    {
        MutationFixture fixture;
        auto request = fixture.mappedToAlternate(5'361);
        AtomicAuthority authority(
            fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.mapped_root));
        auto planning_root = authority.acquireCurrentRoot();
        ASSERT_TRUE(planning_root);
        auto prepared = DependentObjectMutationPlanner::plan(planning_root.get(), std::move(request));
        DurableStorage storage(planning_root->getAuthorityState(), 5'360);
        auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 53, 5'360);
        auto prepared_commit = DependentObjectMutationCoordinator::prepareCommit(
            std::move(planning_root), authority, storage, guard, std::move(prepared));
        storage.failure = DurableStorage::Failure::CommitAfter;

        EXPECT_THROW(
            static_cast<void>(
                DependentObjectMutationCoordinator::commitDurably(storage, guard, std::move(prepared_commit))),
            DatabaseSchemaMutationIndeterminateDurabilityError);
        EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::CommitMarker);
        ASSERT_TRUE(storage.commit);
        {
            auto still_before = authority.acquireCurrentRoot();
            ASSERT_TRUE(still_before);
            EXPECT_TRUE(still_before->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
            EXPECT_FALSE(still_before->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
        }

        storage.failure = DurableStorage::Failure::None;
        auto recovered = DependentObjectMutationCoordinator::recoverDurably(
            storage, guard, std::move(prepared_commit), storage.commit);
        ASSERT_TRUE(recovered);
        ASSERT_TRUE(storage.recovery_decision);
        EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::CompleteCommitted);
        auto published = DependentObjectMutationCoordinator::publish(authority, std::move(*recovered));
        EXPECT_EQ(published.kind, DependentObjectMutationKind::Alter);
        EXPECT_EQ(published.commit.transaction_id, 5'361u);
        auto after = authority.acquireCurrentRoot();
        ASSERT_TRUE(after);
        EXPECT_FALSE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.bindings.dependency_edges.front()));
        EXPECT_TRUE(after->getSchemaObjectDependencyGraph().containsEdge(fixture.alternate_bindings.dependency_edges.front()));
    }
}

TEST(DependentObjectAdmissionCoordinator, DurableCommitPrecedesAuthorityPublication)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto before = authority.acquireCurrentRoot();
    ASSERT_TRUE(before);
    auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
        before.get(), 4001, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    DurableStorage storage(before->getAuthorityState(), 4000);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 1, 4000);

    auto prepared_commit = DependentObjectAdmissionCoordinator::prepareTableCreateCommit(
        std::move(before), authority, storage, guard, std::move(prepared));

    EXPECT_EQ(storage.events, std::vector<String>({"validate"}));
    {
        auto still_before = authority.acquireCurrentRoot();
        ASSERT_TRUE(still_before);
        EXPECT_EQ(still_before->getDatabaseCatalogEpoch(), 7);
        EXPECT_FALSE(still_before->findExpectationRecord(fixture.table));
    }

    auto durable = DependentObjectAdmissionCoordinator::commitPreparedTableCreateDurably(
        storage, guard, std::move(prepared_commit));

    EXPECT_EQ(durable.getCommit().transaction_id, 4001);
    {
        auto still_unpublished = authority.acquireCurrentRoot();
        ASSERT_TRUE(still_unpublished);
        EXPECT_EQ(still_unpublished->getDatabaseCatalogEpoch(), 7);
        EXPECT_FALSE(still_unpublished->findExpectationRecord(fixture.table));
    }

    const auto committed
        = DependentObjectAdmissionCoordinator::publishDurablyCommittedTableCreate(authority, std::move(durable));

    EXPECT_EQ(committed.getCommit().transaction_id, 4001);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    EXPECT_FALSE(storage.discarded);
    EXPECT_FALSE(storage.recovery_required);
    ASSERT_TRUE(storage.commit);
    EXPECT_EQ(
        storage.events,
        std::vector<String>({
            "validate",
            "stage",
            "stage",
            "stage",
            "stage",
            "finish-staging",
            "prepare",
            "install",
            "install",
            "install",
            "install",
            "finish-installation",
            "commit",
        }));
    EXPECT_EQ(storage.installed_kinds.size(), 4);
    ASSERT_TRUE(committed.getBoundUDTReferences());
    EXPECT_EQ(committed.getBoundUDTReferences()->getObject(), fixture.table);
    EXPECT_EQ(committed.getSidecarExpectation(), fixture.durableExpectation());
    EXPECT_TRUE(committed.getPublicationStatistics().reused_definition_validation);
    EXPECT_EQ(committed.getPublicationStatistics().definition_records_validated, 0);
    EXPECT_EQ(authority.scanRetired().retired_root_count, 0);

    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 8);
    ASSERT_TRUE(after->findExpectationRecord(fixture.table));
    EXPECT_EQ(*after->findExpectationRecord(fixture.table), fixture.durableExpectation());
}

TEST(DependentObjectAdmissionCoordinator, CreateRecoveryRollsBackPreparedAndCompletesDurableCommitWithoutEarlyPublication)
{
    {
        Fixture fixture;
        AtomicAuthority authority(
            fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
        auto planning_root = authority.acquireCurrentRoot();
        ASSERT_TRUE(planning_root);
        auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
            planning_root.get(), 4'051, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
        DurableStorage storage(planning_root->getAuthorityState(), 4'050);
        auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 11, 4'050);
        auto prepared_commit = DependentObjectAdmissionCoordinator::prepareTableCreateCommit(
            std::move(planning_root), authority, storage, guard, std::move(prepared));
        storage.failure = DurableStorage::Failure::Install;

        EXPECT_THROW(
            static_cast<void>(DependentObjectAdmissionCoordinator::commitPreparedTableCreateDurably(
                storage, guard, std::move(prepared_commit))),
            DatabaseSchemaMutationIndeterminateDurabilityError);
        EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::AfterImage);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);

        storage.failure = DurableStorage::Failure::None;
        auto recovered = DependentObjectAdmissionCoordinator::recoverPreparedTableCreateDurably(
            storage, guard, std::move(prepared_commit), std::nullopt);
        EXPECT_FALSE(recovered);
        ASSERT_TRUE(storage.recovery_decision);
        EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        EXPECT_TRUE(storage.retired_rollback);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
        auto after = authority.acquireCurrentRoot();
        ASSERT_TRUE(after);
        EXPECT_FALSE(after->findExpectationRecord(fixture.table));
    }

    {
        Fixture fixture;
        AtomicAuthority authority(
            fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
        auto planning_root = authority.acquireCurrentRoot();
        ASSERT_TRUE(planning_root);
        auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
            planning_root.get(), 4'061, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
        DurableStorage storage(planning_root->getAuthorityState(), 4'060);
        auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 12, 4'060);
        auto prepared_commit = DependentObjectAdmissionCoordinator::prepareTableCreateCommit(
            std::move(planning_root), authority, storage, guard, std::move(prepared));
        storage.failure = DurableStorage::Failure::CommitAfter;

        EXPECT_THROW(
            static_cast<void>(DependentObjectAdmissionCoordinator::commitPreparedTableCreateDurably(
                storage, guard, std::move(prepared_commit))),
            DatabaseSchemaMutationIndeterminateDurabilityError);
        EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::CommitMarker);
        ASSERT_TRUE(storage.commit);
        {
            auto still_before = authority.acquireCurrentRoot();
            ASSERT_TRUE(still_before);
            EXPECT_FALSE(still_before->findExpectationRecord(fixture.table));
        }

        storage.failure = DurableStorage::Failure::None;
        auto recovered = DependentObjectAdmissionCoordinator::recoverPreparedTableCreateDurably(
            storage, guard, std::move(prepared_commit), storage.commit);
        ASSERT_TRUE(recovered);
        ASSERT_TRUE(storage.recovery_decision);
        EXPECT_EQ(*storage.recovery_decision, DatabaseSchemaWALRecoveryDecision::CompleteCommitted);
        auto committed
            = DependentObjectAdmissionCoordinator::publishDurablyCommittedTableCreate(authority, std::move(*recovered));
        EXPECT_EQ(committed.getCommit().transaction_id, 4'061u);
        auto after = authority.acquireCurrentRoot();
        ASSERT_TRUE(after);
        ASSERT_NE(after->findExpectationRecord(fixture.table), nullptr);
        EXPECT_EQ(*after->findExpectationRecord(fixture.table), fixture.durableExpectation());
    }
}

TEST(DependentObjectAdmissionCoordinator, PrePrepareFailureDiscardsStagingWithoutPublication)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto before = authority.acquireCurrentRoot();
    ASSERT_TRUE(before);
    auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
        before.get(), 4101, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    DurableStorage storage(before->getAuthorityState(), 4100);
    storage.failure = DurableStorage::Failure::FinishStaging;
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 2, 4100);

    EXPECT_THROW(
        static_cast<void>(DependentObjectAdmissionCoordinator::commitPreparedTableCreate(
            std::move(before), authority, storage, guard, std::move(prepared))),
        std::runtime_error);

    EXPECT_TRUE(storage.discarded);
    EXPECT_FALSE(storage.recovery_required);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 7);
    EXPECT_FALSE(after->findExpectationRecord(fixture.table));
}

TEST(DependentObjectAdmissionCoordinator, RejectsAPlanningSnapshotFromAnotherAuthorityBeforeStorage)
{
    Fixture fixture;
    Fixture foreign_fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    AtomicAuthority foreign_authority(
        foreign_fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(foreign_fixture.root));
    auto before = authority.acquireCurrentRoot();
    auto foreign = foreign_authority.acquireCurrentRoot();
    ASSERT_TRUE(before);
    ASSERT_TRUE(foreign);
    auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
        before.get(), 4151, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    DurableStorage storage(before->getAuthorityState(), 4150);
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 4, 4150);

    EXPECT_THROW(
        static_cast<void>(DependentObjectAdmissionCoordinator::commitPreparedTableCreate(
            std::move(foreign), authority, storage, guard, std::move(prepared))),
        std::logic_error);

    EXPECT_TRUE(storage.events.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 7);
}

TEST(DependentObjectAdmissionCoordinator, CommitAmbiguityFailStopsWithoutPublication)
{
    Fixture fixture;
    AtomicAuthority authority(
        fixture.database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(fixture.root));
    auto before = authority.acquireCurrentRoot();
    ASSERT_TRUE(before);
    auto prepared = DependentObjectAdmissionPlanner::planTableCreate(
        before.get(), 4201, 7, fixture.bindings, fixture.metadata, fixture.metadata_validator);
    DurableStorage storage(before->getAuthorityState(), 4200);
    storage.failure = DurableStorage::Failure::Commit;
    auto guard = DatabaseSchemaMutationGuard::issue(fixture.database_uuid, 3, 4200);

    EXPECT_THROW(
        static_cast<void>(DependentObjectAdmissionCoordinator::commitPreparedTableCreate(
            std::move(before), authority, storage, guard, std::move(prepared))),
        DatabaseSchemaMutationIndeterminateDurabilityError);

    EXPECT_FALSE(storage.discarded);
    EXPECT_TRUE(storage.recovery_required);
    EXPECT_EQ(storage.recovery_transaction_id, 4201);
    EXPECT_EQ(storage.recovery_phase, DatabaseSchemaMutationDurabilityPhase::CommitMarker);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);
    auto after = authority.acquireCurrentRoot();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->getDatabaseCatalogEpoch(), 7);
    EXPECT_FALSE(after->findExpectationRecord(fixture.table));
}

} // namespace DB::UDT
