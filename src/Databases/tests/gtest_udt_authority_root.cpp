#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <stdexcept>
#include <string>
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

Digest digest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

DefinitionInput definitionInput(UUID database_uuid, UUID type_uuid, String qualified_name, String local_name)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return input;
}

Record definitionRecord(const Definition & definition, UInt8 metadata_tag)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "CREATE TYPE " + definition.getNormalizedName() + " AS UInt64",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, metadata_tag),
            .owner_display_name = "owner",
            .comment = "record-" + std::to_string(metadata_tag),
            .creation_time_us_utc = metadata_tag,
        });
}

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor descriptor(const Record & definition, Digest storage_fingerprint)
{
    const String canonical_arguments = noArguments();
    const String canonical_physical_type = "UInt64";
    const Digest instantiation_hash = computeInstantiationSemanticHash({
        .definition_identity = definition.identity,
        .definition_hash = definition.definition_hash,
        .canonical_arguments_encoding = canonical_arguments,
        .canonical_physical_type = canonical_physical_type,
        .storage_fingerprint = storage_fingerprint,
        .checker_abi = definition.checker_abi,
        .checker_charge_abi = definition.checker_charge_abi,
        .policy_abi = definition.policy_abi,
        .function_registry_abi = definition.function_registry_abi,
        .policy_semantic_hash = definition.policy_semantic_hash,
        .semantic_capabilities = definition.semantic_capabilities,
    });
    return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        definition.identity,
        definition.definition_hash,
        canonical_arguments,
        canonical_physical_type,
        instantiation_hash,
        storage_fingerprint,
        definition.checker_abi,
        definition.checker_charge_abi,
        definition.policy_abi,
        definition.function_registry_abi,
        definition.policy_semantic_hash,
        definition.semantic_capabilities,
        definition.normalized_name);
}

PersistedTypePathSection pathSection(SchemaObjectKind kind)
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return PersistedTypePathSection::ColumnType;
        case SchemaObjectKind::View: return PersistedTypePathSection::ViewExpression;
        case SchemaObjectKind::Dictionary: return PersistedTypePathSection::DictionaryAttribute;
        case SchemaObjectKind::SyntheticTestObject: return PersistedTypePathSection::SyntheticPayload;
        case SchemaObjectKind::TypeDefinition: break;
    }
    throw std::logic_error("a type definition cannot own a persisted object sidecar");
}

struct TestDependentObject
{
    SidecarExpectationRecord expectation;
    String canonical_metadata_bytes;
    String canonical_sidecar_bytes;
    String canonical_installation_record_bytes;

    AuthorityDependentObjectResourceImage image() const
    {
        return {
            .object = expectation.object,
            .canonical_metadata_bytes = canonical_metadata_bytes,
            .canonical_sidecar_bytes = canonical_sidecar_bytes,
            .canonical_installation_record_bytes = canonical_installation_record_bytes,
        };
    }
};

TestDependentObject
dependentObject(UUID database_uuid, UUID object_uuid, SchemaObjectKind kind, UInt64 revision, UInt8 hash_tag, const Record & definition)
{
    const SchemaObjectID object{
        .kind = kind,
        .database_uuid = database_uuid,
        .object_uuid = object_uuid,
    };
    PersistedTypeReferences references;
    references.object = object;
    references.object_schema_revision = revision;
    references.physical_schema_fingerprint = digest(static_cast<UInt8>(hash_tag + 32));
    references.descriptors = {descriptor(definition, digest(static_cast<UInt8>(hash_tag + 64)))};
    references.occurrence_paths = {{
        .section = pathSection(kind),
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    references.uses = {{.path_id = 0, .descriptor_id = 0}};
    String sidecar = encodePersistedTypeReferences(references);
    String metadata = "canonical-metadata-" + std::to_string(hash_tag);
    const DependentObjectMetadataInstallationRecord installation{
        .object = object,
        .object_schema_revision = revision,
        .object_name = "object_" + std::to_string(hash_tag),
        .metadata_artifact_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata),
    };
    String installation_bytes = encodeDependentObjectMetadataInstallationRecord(installation);
    return {
        .expectation = {
            .object = object,
            .object_schema_revision = revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = references.physical_schema_fingerprint,
            .installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation),
        },
        .canonical_metadata_bytes = std::move(metadata),
        .canonical_sidecar_bytes = std::move(sidecar),
        .canonical_installation_record_bytes = std::move(installation_bytes),
    };
}

std::vector<AuthorityDependentObjectResourceImage> images(std::span<const TestDependentObject> dependent_objects)
{
    std::vector<AuthorityDependentObjectResourceImage> result;
    result.reserve(dependent_objects.size());
    for (const auto & object : dependent_objects)
        result.push_back(object.image());
    return result;
}

AuthorityInventorySummary
inventorySummary(std::span<const Record> definition_records, std::span<const SidecarExpectationRecord> expectation_records)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    leaves.reserve(definition_records.size() + expectation_records.size());
    for (const auto & record : definition_records)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record),
        });
    }
    for (const auto & record : expectation_records)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = record.object.object_uuid,
            },
            .object_revision = record.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(record),
        });
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return buildAuthorityInventorySummary(leaves);
}

struct AuthorityFixture
{
    UUID database_uuid;
    std::vector<Definition::Ptr> definitions;
    std::vector<Record> definition_records;
    std::vector<TestDependentObject> dependent_objects;
    std::vector<SidecarExpectationRecord> expectation_records;
    SchemaObjectDependencyGraph::Ptr graph;
    AuthorityState state;
};

AuthorityFixture fixture()
{
    const UUID database_uuid = uuid(1, 2);
    auto definitions = TemplateChecker::checkAll({
        definitionInput(database_uuid, uuid(0x20, 1), "db.Beta", "Beta"),
        definitionInput(database_uuid, uuid(0x10, 1), "db.Alpha", "Alpha"),
    });
    std::vector definition_records{
        definitionRecord(*definitions[0], 1),
        definitionRecord(*definitions[1], 2),
    };
    std::vector dependent_objects{
        dependentObject(database_uuid, uuid(0x40, 1), SchemaObjectKind::Table, 3, 10, definition_records[0]),
        dependentObject(database_uuid, uuid(0x30, 1), SchemaObjectKind::Table, 4, 20, definition_records[1]),
    };
    std::vector expectation_records{dependent_objects[0].expectation, dependent_objects[1].expectation};

    const SchemaObjectID beta{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = definitions[0]->getIdentity().type_uuid,
    };
    const SchemaObjectID alpha{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = definitions[1]->getIdentity().type_uuid,
    };
    const SchemaObjectID beta_table = expectation_records[0].object;
    const SchemaObjectID alpha_table = expectation_records[1].object;
    const SchemaObjectDependencyEdge beta_edge{
        .dependent = beta_table,
        .dependency = beta,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge alpha_edge{
        .dependent = alpha_table,
        .dependency = alpha,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    auto graph = SchemaObjectDependencyGraph::build(
        database_uuid, std::vector{beta_table, alpha_table, beta, alpha}, std::vector{beta_edge, alpha_edge});
    const auto summary = inventorySummary(definition_records, expectation_records);
    auto state = makeAuthorityState(
        database_uuid, 7, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    return {
        .database_uuid = database_uuid,
        .definitions = std::move(definitions),
        .definition_records = std::move(definition_records),
        .dependent_objects = std::move(dependent_objects),
        .expectation_records = std::move(expectation_records),
        .graph = std::move(graph),
        .state = state,
    };
}

template <typename Callback>
void expectRootError(AuthorityRootError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityRootError";
    }
    catch (const AuthorityRootError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(AuthorityRoot, BuildsOneCanonicalImmutableValue)
{
    auto input = fixture();
    const auto dependent_images = images(input.dependent_objects);
    const auto root = AuthorityRootBuilder::build(
        input.state,
        41,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);

    EXPECT_EQ(root->getDatabaseUUID(), input.database_uuid);
    EXPECT_EQ(root->getDatabaseCatalogEpoch(), 7);
    EXPECT_EQ(root->getTypeIndexGeneration(), 41);
    EXPECT_NE(root->getTypeIndexContentDigest(), Digest{});
    EXPECT_EQ(root->getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    EXPECT_EQ(root->getInventorySummary().leaf_count, 4);
    EXPECT_EQ(root->getInventorySummary().merkle_radix_root, input.state.inventory_root);

    ASSERT_EQ(root->getDefinitionRecords().size(), 2);
    EXPECT_EQ(root->getDefinitionRecords()[0].normalized_local_name, "Alpha");
    EXPECT_EQ(root->getDefinitionRecords()[1].normalized_local_name, "Beta");
    ASSERT_EQ(root->getExpectationRecords().size(), 2);
    EXPECT_EQ(root->getExpectationRecords()[0].object.object_uuid, uuid(0x30, 1));
    EXPECT_EQ(root->getExpectationRecords()[1].object.object_uuid, uuid(0x40, 1));

    EXPECT_EQ(root->findByName("Alpha"), input.definitions[1]);
    EXPECT_EQ(root->findByName("Beta"), input.definitions[0]);
    EXPECT_EQ(root->findByIdentity(input.definitions[0]->getIdentity()), input.definitions[0]);
    EXPECT_FALSE(root->findByName("Missing"));
    EXPECT_EQ(root->getSchemaObjectDependencyGraph().computeRoot(), input.state.schema_graph_root);
}

TEST(AuthorityRoot, BuildsAnEmptyAuthorityWithoutConflatingItsCounters)
{
    const UUID database_uuid = uuid(5, 6);
    const auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto summary = buildAuthorityInventorySummary(std::span<const AuthorityInventoryLeaf>{});
    const auto state = makeAuthorityState(
        database_uuid, 9, definition_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    const std::vector<Definition::Ptr> definitions;
    const std::vector<Record> definition_records;
    const std::vector<SidecarExpectationRecord> expectation_records;

    const auto root = AuthorityRootBuilder::build(state, 0, definitions, definition_records, expectation_records, graph);
    EXPECT_EQ(root->getDatabaseCatalogEpoch(), 9);
    EXPECT_EQ(root->getTypeIndexGeneration(), 0);
    EXPECT_TRUE(root->getDefinitionRecords().empty());
    EXPECT_TRUE(root->getExpectationRecords().empty());
    EXPECT_FALSE(root->findByName("Missing"));
}

TEST(AuthorityRoot, TypeIndexDigestIgnoresAdministrativeBytesAndInputOrder)
{
    auto input = fixture();
    const auto dependent_images = images(input.dependent_objects);
    const auto original = AuthorityRootBuilder::build(
        input.state,
        12,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);

    std::reverse(input.definitions.begin(), input.definitions.end());
    std::reverse(input.definition_records.begin(), input.definition_records.end());
    std::reverse(input.expectation_records.begin(), input.expectation_records.end());
    input.definition_records.front().comment = "administrative-change";
    const auto changed_inventory = inventorySummary(input.definition_records, input.expectation_records);
    const auto changed_state = makeAuthorityState(
        input.database_uuid,
        input.state.database_catalog_epoch + 1,
        dependent_object_authority_capability_mask,
        changed_inventory.leaf_count,
        changed_inventory.merkle_radix_root,
        input.graph->computeRoot());
    const auto changed = AuthorityRootBuilder::build(
        changed_state,
        12,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);

    EXPECT_NE(changed->getInventorySummary().merkle_radix_root, original->getInventorySummary().merkle_radix_root);
    EXPECT_EQ(changed->getTypeIndexContentDigest(), original->getTypeIndexContentDigest());
}

TEST(AuthorityRoot, DependentObjectAdmissionPathCopiesOnlyTouchedPersistentPaths)
{
    auto input = fixture();
    const auto dependent_images = images(input.dependent_objects);
    const auto base = AuthorityRootBuilder::build(
        input.state,
        12,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);
    const auto added_object
        = dependentObject(input.database_uuid, uuid(0x50, 1), SchemaObjectKind::Table, 1, 30, input.definition_records[1]);
    const auto & added_expectation = added_object.expectation;
    const SchemaObjectID dependency{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[1]->getIdentity().type_uuid,
    };
    const SchemaObjectDependencyEdge added_edge{
        .dependent = added_expectation.object,
        .dependency = dependency,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {added_expectation.object},
        .node_removals = {},
        .edge_additions = {added_edge},
        .edge_removals = {},
    };
    const auto * shared_expectation = base->findExpectationRecord(input.expectation_records.front().object);
    const auto shared_definition = base->findByIdentity(input.definitions.front()->getIdentity());
    const auto base_inventory = base->pinAuthorityInventory();
    const auto base_graph = base->pinSchemaObjectDependencyGraph();
    DependentObjectAdmissionDeltaStatistics statistics;

    ASSERT_NE(shared_expectation, nullptr);
    ASSERT_TRUE(shared_definition);

    const auto admitted = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
        *base, 8, added_expectation, graph_delta, added_object.image(), AuthorityRootBuildLimits{}, &statistics);

    EXPECT_FALSE(admitted->sharesContentPayloadWith(*base));
    EXPECT_TRUE(admitted->sharesDefinitionContentWith(*base));
    EXPECT_EQ(admitted->findByIdentity(input.definitions.front()->getIdentity()), shared_definition);
    EXPECT_EQ(admitted->findExpectationRecord(input.expectation_records.front().object), shared_expectation);
    EXPECT_NE(admitted->pinAuthorityInventory(), base_inventory);
    EXPECT_NE(admitted->pinSchemaObjectDependencyGraph(), base_graph);
    EXPECT_EQ(admitted->getDatabaseCatalogEpoch(), 8);
    EXPECT_EQ(admitted->getExpectationRecordCount(), base->getExpectationRecordCount() + 1);
    ASSERT_TRUE(admitted->findExpectationRecord(added_expectation.object));
    EXPECT_EQ(*admitted->findExpectationRecord(added_expectation.object), added_expectation);
    EXPECT_TRUE(admitted->getSchemaObjectDependencyGraph().containsEdge(added_edge));

    EXPECT_EQ(statistics.inventory.deltas_applied, 1);
    EXPECT_EQ(statistics.inventory.leaves_materialized, 0);
    EXPECT_LE(statistics.inventory.nodes_visited, 64);
    EXPECT_LE(statistics.inventory.nodes_created, 65);
    EXPECT_EQ(statistics.graph.node_deltas_applied, 1);
    EXPECT_EQ(statistics.graph.edge_deltas_applied, 1);
    EXPECT_EQ(statistics.graph.adjacency_neighbors_copied, 0);
    EXPECT_EQ(statistics.graph.neighbors_materialized, 0);
    EXPECT_EQ(statistics.graph.snapshot_nodes_materialized, 0);
    EXPECT_EQ(statistics.graph.snapshot_edges_materialized, 0);
    EXPECT_EQ(statistics.expectation_record_deltas_applied, 1);
    EXPECT_EQ(statistics.expectation_records_materialized, 0);
    EXPECT_LE(statistics.expectation_record_nodes_visited, 67);
    EXPECT_LE(statistics.expectation_record_nodes_created, statistics.expectation_record_nodes_visited + 1);
    EXPECT_EQ(statistics.expectation_record_nodes_hashed, statistics.expectation_record_nodes_created);
}

TEST(AuthorityRoot, DependentObjectAdmissionRetainsObjectAndDefinitionDependencies)
{
    auto input = fixture();
    const auto dependent_images = images(input.dependent_objects);
    const auto base = AuthorityRootBuilder::build(
        input.state,
        12,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);
    const auto added_object
        = dependentObject(input.database_uuid, uuid(0x50, 2), SchemaObjectKind::View, 1, 31, input.definition_records[1]);
    const auto & added_expectation = added_object.expectation;
    const SchemaObjectID definition_dependency{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[1]->getIdentity().type_uuid,
    };
    const SchemaObjectDependencyEdge object_edge{
        .dependent = added_expectation.object,
        .dependency = input.expectation_records[0].object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
    };
    const SchemaObjectDependencyEdge definition_edge{
        .dependent = added_expectation.object,
        .dependency = definition_dependency,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {added_expectation.object},
        .node_removals = {},
        .edge_additions = {object_edge, definition_edge},
        .edge_removals = {},
    };

    const auto admitted
        = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(*base, 8, added_expectation, graph_delta, added_object.image());

    EXPECT_EQ(admitted->getSchemaObjectDependencyGraph().getEdgeCount(), input.graph->getEdgeCount() + 2);
    EXPECT_TRUE(admitted->getSchemaObjectDependencyGraph().containsEdge(object_edge));
    EXPECT_TRUE(admitted->getSchemaObjectDependencyGraph().containsEdge(definition_edge));
    EXPECT_TRUE(admitted->findExpectationRecord(added_expectation.object));

    const SchemaObjectDependencyGraphMutation object_only_delta{
        .node_additions = {added_expectation.object},
        .node_removals = {},
        .edge_additions = {object_edge},
        .edge_removals = {},
    };
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
                *base, 8, added_expectation, object_only_delta, added_object.image()));
        });
}

TEST(AuthorityRoot, DependentObjectReplacementPreservesDefinitionAndMutatesObjectDependencies)
{
    auto input = fixture();
    const auto dependent_images = images(input.dependent_objects);
    const auto base = AuthorityRootBuilder::build(
        input.state,
        12,
        input.definitions,
        input.definition_records,
        input.expectation_records,
        input.graph,
        AuthorityRootBuildLimits{},
        dependent_images);
    const auto & before = input.expectation_records[0];
    const SchemaObjectID definition_dependency{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[0]->getIdentity().type_uuid,
    };
    const SchemaObjectDependencyEdge definition_edge{
        .dependent = before.object,
        .dependency = definition_dependency,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge object_edge{
        .dependent = before.object,
        .dependency = input.expectation_records[1].object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
    };

    const auto with_object_dependency = dependentObject(
        input.database_uuid,
        before.object.object_uuid,
        before.object.kind,
        before.object_schema_revision + 1,
        32,
        input.definition_records[0]);
    const SchemaObjectDependencyGraphMutation addition{
        .node_additions = {},
        .node_removals = {},
        .edge_additions = {object_edge},
        .edge_removals = {},
    };
    const auto added = AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
        *base, base->getDatabaseCatalogEpoch() + 1, with_object_dependency.expectation, addition, with_object_dependency.image());
    EXPECT_TRUE(added->getSchemaObjectDependencyGraph().containsEdge(definition_edge));
    EXPECT_TRUE(added->getSchemaObjectDependencyGraph().containsEdge(object_edge));

    const auto without_object_dependency = dependentObject(
        input.database_uuid,
        before.object.object_uuid,
        before.object.kind,
        before.object_schema_revision + 2,
        33,
        input.definition_records[0]);
    const SchemaObjectDependencyGraphMutation removal{
        .node_additions = {},
        .node_removals = {},
        .edge_additions = {},
        .edge_removals = {object_edge},
    };
    const auto removed = AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
        *added, added->getDatabaseCatalogEpoch() + 1, without_object_dependency.expectation, removal, without_object_dependency.image());
    EXPECT_TRUE(removed->getSchemaObjectDependencyGraph().containsEdge(definition_edge));
    EXPECT_FALSE(removed->getSchemaObjectDependencyGraph().containsEdge(object_edge));

    const auto without_definition_dependency = dependentObject(
        input.database_uuid,
        before.object.object_uuid,
        before.object.kind,
        before.object_schema_revision + 1,
        34,
        input.definition_records[0]);
    const SchemaObjectDependencyGraphMutation invalid_removal{
        .node_additions = {},
        .node_removals = {},
        .edge_additions = {object_edge},
        .edge_removals = {definition_edge},
    };
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
                *base,
                base->getDatabaseCatalogEpoch() + 1,
                without_definition_dependency.expectation,
                invalid_removal,
                without_definition_dependency.image()));
        });
}

TEST(AuthorityRoot, DependentObjectAdmissionMatchesFullRebuildForOrderedRadixInsertions)
{
    auto input = fixture();
    std::vector base_objects{
        dependentObject(input.database_uuid, uuid(0x20, 1), SchemaObjectKind::Table, 3, 10, input.definition_records[0]),
        dependentObject(input.database_uuid, uuid(0x40, 1), SchemaObjectKind::Table, 4, 20, input.definition_records[1]),
    };
    std::vector base_expectations{base_objects[0].expectation, base_objects[1].expectation};
    const auto base_images = images(base_objects);
    const SchemaObjectID beta{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[0]->getIdentity().type_uuid,
    };
    const SchemaObjectID alpha{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[1]->getIdentity().type_uuid,
    };
    const std::vector base_edges{
        SchemaObjectDependencyEdge{
            .dependent = base_expectations[0].object,
            .dependency = beta,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        },
        SchemaObjectDependencyEdge{
            .dependent = base_expectations[1].object,
            .dependency = alpha,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        },
    };
    const auto base_graph = SchemaObjectDependencyGraph::build(
        input.database_uuid, std::vector{base_expectations[0].object, base_expectations[1].object, beta, alpha}, base_edges);
    const auto base_summary = inventorySummary(input.definition_records, base_expectations);
    const auto base_state = makeAuthorityState(
        input.database_uuid,
        7,
        dependent_object_authority_capability_mask,
        base_summary.leaf_count,
        base_summary.merkle_radix_root,
        base_graph->computeRoot());
    const auto base = AuthorityRootBuilder::build(
        base_state,
        12,
        input.definitions,
        input.definition_records,
        base_expectations,
        base_graph,
        AuthorityRootBuildLimits{},
        base_images);

    for (const auto & [object_uuid, hash_tag] : std::vector<std::pair<UUID, UInt8>>{{uuid(0x10, 1), 30}, {uuid(0x30, 1), 40}})
    {
        SCOPED_TRACE(hash_tag == 30 ? "new radix child before existing children" : "new radix child between existing children");
        const auto added_object
            = dependentObject(input.database_uuid, object_uuid, SchemaObjectKind::Table, 1, hash_tag, input.definition_records[1]);
        const auto & added_expectation = added_object.expectation;
        const SchemaObjectDependencyEdge added_edge{
            .dependent = added_expectation.object,
            .dependency = alpha,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        };
        const SchemaObjectDependencyGraphMutation graph_delta{
            .node_additions = {added_expectation.object},
            .node_removals = {},
            .edge_additions = {added_edge},
            .edge_removals = {},
        };

        const auto path_copy = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
            *base, 8, added_expectation, graph_delta, added_object.image(), AuthorityRootBuildLimits{}, nullptr);
        auto full_expectations = base_expectations;
        full_expectations.push_back(added_expectation);
        auto full_images = base_images;
        full_images.push_back(added_object.image());
        const auto full_graph = SchemaObjectDependencyGraph::applyMutation(base_graph, graph_delta);
        const auto full_summary = inventorySummary(input.definition_records, full_expectations);
        const auto full_state = makeAuthorityState(
            input.database_uuid,
            8,
            dependent_object_authority_capability_mask,
            full_summary.leaf_count,
            full_summary.merkle_radix_root,
            full_graph->computeRoot());
        const auto full_rebuild = AuthorityRootBuilder::build(
            full_state,
            12,
            input.definitions,
            input.definition_records,
            full_expectations,
            full_graph,
            AuthorityRootBuildLimits{},
            full_images);

        EXPECT_EQ(path_copy->getAuthorityState(), full_rebuild->getAuthorityState());
        EXPECT_EQ(path_copy->getInventorySummary(), full_rebuild->getInventorySummary());
        EXPECT_EQ(path_copy->getSchemaObjectDependencyGraph().computeRoot(), full_rebuild->getSchemaObjectDependencyGraph().computeRoot());
        EXPECT_TRUE(std::ranges::equal(path_copy->getDefinitionRecords(), full_rebuild->getDefinitionRecords()));
        EXPECT_TRUE(std::ranges::equal(path_copy->getExpectationRecords(), full_rebuild->getExpectationRecords()));
        EXPECT_EQ(path_copy->getTypeIndexGeneration(), full_rebuild->getTypeIndexGeneration());
        EXPECT_EQ(path_copy->getTypeIndexContentDigest(), full_rebuild->getTypeIndexContentDigest());
        EXPECT_TRUE(path_copy->sharesDefinitionContentWith(*base));
        EXPECT_EQ(
            path_copy->findExpectationRecord(base_expectations.front().object),
            base->findExpectationRecord(base_expectations.front().object));
    }
}

TEST(AuthorityRoot, DependentObjectActivationClonesTheExactContentPayloadInConstantTime)
{
    auto input = fixture();
    const SchemaObjectID beta{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[0]->getIdentity().type_uuid,
    };
    const SchemaObjectID alpha{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[1]->getIdentity().type_uuid,
    };
    const auto definition_only_graph
        = SchemaObjectDependencyGraph::build(input.database_uuid, std::vector{beta, alpha}, std::vector<SchemaObjectDependencyEdge>{});
    const auto definition_only_summary = inventorySummary(input.definition_records, {});
    const auto definition_only_state = makeAuthorityState(
        input.database_uuid,
        7,
        definition_authority_capability_mask,
        definition_only_summary.leaf_count,
        definition_only_summary.merkle_radix_root,
        definition_only_graph->computeRoot());
    const std::vector<SidecarExpectationRecord> no_expectations;
    const auto definition_only = AuthorityRootBuilder::build(
        definition_only_state, 55, input.definitions, input.definition_records, no_expectations, definition_only_graph);

    const auto dependent_object_state = activateDependentObjectAuthority(definition_only_state);
    const auto definition_only_definition_data = definition_only->getDefinitionRecords().data();
    const auto definition_only_catalog_definition = definition_only->findByIdentity(input.definitions.front()->getIdentity());
    const auto definition_only_inventory = definition_only->pinAuthorityInventory();
    const auto definition_only_graph_payload = definition_only->pinSchemaObjectDependencyGraph();
    const auto dependent_object = definition_only->cloneWithAuthorityState(dependent_object_state);

    EXPECT_TRUE(dependent_object->sharesContentPayloadWith(*definition_only));
    EXPECT_EQ(dependent_object->getContentPayloadLogicalCharge(), definition_only->getContentPayloadLogicalCharge());
    EXPECT_EQ(dependent_object->getTypeIndexGeneration(), definition_only->getTypeIndexGeneration());
    EXPECT_EQ(dependent_object->getTypeIndexContentDigest(), definition_only->getTypeIndexContentDigest());
    EXPECT_EQ(dependent_object->getDatabaseCatalogEpoch(), definition_only->getDatabaseCatalogEpoch() + 1);
    EXPECT_EQ(dependent_object->getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    EXPECT_EQ(dependent_object->getInventorySummary(), definition_only->getInventorySummary());
    EXPECT_TRUE(std::ranges::equal(dependent_object->getDefinitionRecords(), definition_only->getDefinitionRecords()));
    EXPECT_TRUE(std::ranges::equal(dependent_object->getExpectationRecords(), definition_only->getExpectationRecords()));
    EXPECT_EQ(
        dependent_object->getSchemaObjectDependencyGraph().computeRoot(), definition_only->getSchemaObjectDependencyGraph().computeRoot());
    EXPECT_EQ(dependent_object->getDefinitionRecords().data(), definition_only_definition_data);
    EXPECT_EQ(dependent_object->findByIdentity(input.definitions.front()->getIdentity()), definition_only_catalog_definition);
    EXPECT_EQ(dependent_object->pinAuthorityInventory(), definition_only_inventory);
    EXPECT_EQ(dependent_object->pinSchemaObjectDependencyGraph(), definition_only_graph_payload);
}

TEST(AuthorityRoot, DependentObjectActivationRejectsEveryAuthorityAnchorDrift)
{
    const UUID database_uuid = uuid(15, 16);
    const auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto summary = buildAuthorityInventorySummary(std::span<const AuthorityInventoryLeaf>{});
    const auto definition_only_state
        = makeAuthorityState(database_uuid, 11, definition_authority_capability_mask, 0, summary.merkle_radix_root, graph->computeRoot());
    const std::vector<Definition::Ptr> definitions;
    const std::vector<Record> definition_records;
    const std::vector<SidecarExpectationRecord> expectation_records;
    const auto definition_only
        = AuthorityRootBuilder::build(definition_only_state, 0, definitions, definition_records, expectation_records, graph);

    const auto expect_rejected = [&](const AuthorityState & next)
    {
        expectRootError(
            AuthorityRootError::Code::InvalidAuthorityState, [&] { static_cast<void>(definition_only->cloneWithAuthorityState(next)); });
    };

    expect_rejected(makeAuthorityState(
        database_uuid, 13, dependent_object_authority_capability_mask, 0, summary.merkle_radix_root, graph->computeRoot()));
    expect_rejected(
        makeAuthorityState(database_uuid, 12, definition_authority_capability_mask, 0, summary.merkle_radix_root, graph->computeRoot()));
    expect_rejected(makeAuthorityState(
        database_uuid, 12, dependent_object_authority_capability_mask, 1, summary.merkle_radix_root, graph->computeRoot()));
    expect_rejected(makeAuthorityState(database_uuid, 12, dependent_object_authority_capability_mask, 0, digest(91), graph->computeRoot()));
    expect_rejected(
        makeAuthorityState(database_uuid, 12, dependent_object_authority_capability_mask, 0, summary.merkle_radix_root, digest(92)));
    expect_rejected(makeAuthorityState(
        uuid(17, 18), 12, dependent_object_authority_capability_mask, 0, summary.merkle_radix_root, graph->computeRoot()));
}

TEST(AuthorityRoot, RejectsDependentObjectRecordsAndObjectsBeforeActivation)
{
    auto input = fixture();
    auto definition_only_with_expectations = input.state;
    definition_only_with_expectations.persistent_capability_mask = definition_authority_capability_mask;
    expectRootError(
        AuthorityRootError::Code::InvalidAuthorityState,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                definition_only_with_expectations, 1, input.definitions, input.definition_records, input.expectation_records, input.graph));
        });

    const auto definition_summary = inventorySummary(input.definition_records, {});
    const auto definition_only_with_objects = makeAuthorityState(
        input.database_uuid,
        input.state.database_catalog_epoch,
        definition_authority_capability_mask,
        definition_summary.leaf_count,
        definition_summary.merkle_radix_root,
        input.graph->computeRoot());
    const std::vector<SidecarExpectationRecord> no_expectations;
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                definition_only_with_objects, 1, input.definitions, input.definition_records, no_expectations, input.graph));
        });
}

TEST(AuthorityRoot, RejectsDatabaseDriftFromEveryOwnedComponent)
{
    auto input = fixture();
    const UUID other_database = uuid(9, 9);

    auto other_definitions = input.definitions;
    other_definitions[0] = TemplateChecker::checkAll({definitionInput(other_database, uuid(0x20, 1), "db.Beta", "Beta")}).front();
    expectRootError(
        AuthorityRootError::Code::DatabaseMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 1, other_definitions, input.definition_records, input.expectation_records, input.graph));
        });

    auto other_records = input.definition_records;
    other_records[0].identity.database_uuid = other_database;
    expectRootError(
        AuthorityRootError::Code::DatabaseMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(input.state, 1, input.definitions, other_records, input.expectation_records, input.graph));
        });

    auto other_expectations = input.expectation_records;
    other_expectations[0].object.database_uuid = other_database;
    expectRootError(
        AuthorityRootError::Code::DatabaseMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(input.state, 1, input.definitions, input.definition_records, other_expectations, input.graph));
        });

    const auto other_graph = SchemaObjectDependencyGraph::createEmpty(other_database);
    expectRootError(
        AuthorityRootError::Code::DatabaseMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 1, input.definitions, input.definition_records, input.expectation_records, other_graph));
        });
}

TEST(AuthorityRoot, RejectsInexactRecordsAndRootDigests)
{
    auto input = fixture();

    std::vector<Definition::Ptr> one_definition(input.definitions.begin() + 1, input.definitions.end());
    expectRootError(
        AuthorityRootError::Code::RecordDefinitionMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 1, one_definition, input.definition_records, input.expectation_records, input.graph));
        });

    auto mismatched_records = input.definition_records;
    mismatched_records[0].normalized_name = "db.Changed";
    expectRootError(
        AuthorityRootError::Code::RecordDefinitionMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(input.state, 1, input.definitions, mismatched_records, input.expectation_records, input.graph));
        });

    auto changed_metadata = input.definition_records;
    changed_metadata[0].comment = "changed";
    expectRootError(
        AuthorityRootError::Code::InventoryMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(input.state, 1, input.definitions, changed_metadata, input.expectation_records, input.graph));
        });

    const auto empty_graph = SchemaObjectDependencyGraph::createEmpty(input.database_uuid);
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 1, input.definitions, input.definition_records, input.expectation_records, empty_graph));
        });

    auto unknown_capability_state = input.state;
    unknown_capability_state.persistent_capability_mask = UInt64{1} << 63;
    expectRootError(
        AuthorityRootError::Code::InvalidAuthorityState,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                unknown_capability_state, 1, input.definitions, input.definition_records, input.expectation_records, input.graph));
        });
}

TEST(AuthorityRoot, RejectsGraphContentThatDoesNotMatchCheckedAuthority)
{
    auto input = fixture();
    const SchemaObjectID beta{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[0]->getIdentity().type_uuid,
    };
    const SchemaObjectID alpha{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = input.database_uuid,
        .object_uuid = input.definitions[1]->getIdentity().type_uuid,
    };
    const std::vector<SchemaObjectID> nodes(input.graph->getNodes().begin(), input.graph->getNodes().end());
    const SchemaObjectDependencyEdge beta_edge{
        .dependent = input.expectation_records[0].object,
        .dependency = beta,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge alpha_edge{
        .dependent = input.expectation_records[1].object,
        .dependency = alpha,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };

    const auto missing_expectation_edge = SchemaObjectDependencyGraph::build(input.database_uuid, nodes, std::vector{beta_edge});
    const auto missing_edge_state = makeAuthorityState(
        input.database_uuid,
        input.state.database_catalog_epoch,
        dependent_object_authority_capability_mask,
        input.state.leaf_count,
        input.state.inventory_root,
        missing_expectation_edge->computeRoot());
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                missing_edge_state, 1, input.definitions, input.definition_records, input.expectation_records, missing_expectation_edge));
        });

    const SchemaObjectDependencyEdge unproved_definition_edge{
        .dependent = beta,
        .dependency = alpha,
        .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition,
    };
    const auto graph_with_unproved_definition_edge
        = SchemaObjectDependencyGraph::build(input.database_uuid, nodes, std::vector{beta_edge, alpha_edge, unproved_definition_edge});
    const auto unproved_edge_state = makeAuthorityState(
        input.database_uuid,
        input.state.database_catalog_epoch,
        dependent_object_authority_capability_mask,
        input.state.leaf_count,
        input.state.inventory_root,
        graph_with_unproved_definition_edge->computeRoot());
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                unproved_edge_state,
                1,
                input.definitions,
                input.definition_records,
                input.expectation_records,
                graph_with_unproved_definition_edge));
        });
}

TEST(AuthorityRoot, RejectsDuplicateRecordIdentitiesAndProspectiveLimitExcess)
{
    auto input = fixture();
    expectRootError(
        AuthorityRootError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 0, input.definitions, input.definition_records, input.expectation_records, input.graph));
        });

    auto duplicate_expectations = input.expectation_records;
    auto duplicate = duplicate_expectations.front();
    duplicate.object.kind = SchemaObjectKind::View;
    ++duplicate.object_schema_revision;
    duplicate_expectations.push_back(std::move(duplicate));
    const auto duplicate_state = makeAuthorityState(
        input.database_uuid,
        8,
        dependent_object_authority_capability_mask,
        input.state.leaf_count + 1,
        digest(1),
        input.state.schema_graph_root);
    expectRootError(
        AuthorityRootError::Code::DuplicateRecordIdentity,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                duplicate_state, 1, input.definitions, input.definition_records, duplicate_expectations, input.graph));
        });

    auto short_limits = AuthorityRootBuildLimits{};
    short_limits.maximum_canonical_record_bytes = 1;
    expectRootError(
        AuthorityRootError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(AuthorityRootBuilder::build(
                input.state, 1, input.definitions, input.definition_records, input.expectation_records, input.graph, short_limits));
        });

    std::vector<SidecarExpectationRecord> one_less(input.expectation_records.begin() + 1, input.expectation_records.end());
    expectRootError(
        AuthorityRootError::Code::InventoryMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(input.state, 1, input.definitions, input.definition_records, one_less, input.graph));
        });

    const auto one_less_summary = inventorySummary(input.definition_records, one_less);
    const auto one_less_state = makeAuthorityState(
        input.database_uuid,
        input.state.database_catalog_epoch,
        dependent_object_authority_capability_mask,
        one_less_summary.leaf_count,
        one_less_summary.merkle_radix_root,
        input.graph->computeRoot());
    expectRootError(
        AuthorityRootError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(
                AuthorityRootBuilder::build(one_less_state, 1, input.definitions, input.definition_records, one_less, input.graph));
        });
}

}
}
