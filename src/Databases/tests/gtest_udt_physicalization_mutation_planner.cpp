#include <Databases/UDT/PhysicalizationMutationPlanner.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>
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
    input.semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input);
    return input;
}

Record definitionRecord(const Definition & definition, UInt8 tag)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "CREATE TYPE " + definition.getNormalizedName() + " AS UInt64",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, tag),
            .owner_display_name = "owner",
            .comment = "definition-" + std::to_string(tag),
            .creation_time_us_utc = tag,
        });
}

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor descriptor(const Record & definition)
{
    const String canonical_arguments = noArguments();
    const auto physical_type = DataTypeFactory::instance().get("UInt64");
    const String canonical_physical_type = physical_type->getName();
    const Digest storage_fingerprint = physicalTypeFingerprint(physical_type);
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

PersistedTypeOccurrencePath occurrencePath()
{
    return {
        .section = PersistedTypePathSection::SyntheticPayload,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    };
}

AuthorityInventorySummary
inventorySummary(std::span<const Record> definitions, std::span<const SidecarExpectationRecord> expectations)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    leaves.reserve(definitions.size() + expectations.size());
    for (const auto & definition : definitions)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = definition.identity.type_uuid,
            },
            .object_revision = definition.identity.revision,
            .canonical_record_hash = computeRecordHash(definition),
        });
    }
    for (const auto & expectation : expectations)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = expectation.object.object_uuid,
            },
            .object_revision = expectation.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
        });
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return buildAuthorityInventorySummary(leaves);
}

class Provider final : public IPhysicalizationObjectProvider
{
public:
    PhysicalizationObject load(const SidecarExpectationRecord & expectation) const override
    {
        return objects.at(expectation.object);
    }

    std::map<SchemaObjectID, PhysicalizationObject> objects;
};

struct Fixture
{
    UUID database_uuid = uuid(1, 2);
    SchemaObjectID alpha_type = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(0x10, 1));
    SchemaObjectID beta_type = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(0x20, 1));
    SchemaObjectID seed = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x30, 1));
    SchemaObjectID left = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x31, 1));
    SchemaObjectID right = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x32, 1));
    SchemaObjectID diamond = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x33, 1));
    SchemaObjectID external = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x34, 1));
    std::vector<Definition::Ptr> definitions;
    std::vector<Record> records;
    std::vector<SidecarExpectationRecord> expectations;
    SchemaObjectDependencyGraph::Ptr graph;
    AuthorityRoot::Ptr root;
    Provider provider;
    std::map<SchemaObjectID, PhysicalizationRewriteImage> rewrites;

    Fixture()
    {
        definitions = TemplateChecker::checkAll({
            definitionInput(database_uuid, alpha_type.object_uuid, "db.Alpha", "Alpha"),
            definitionInput(database_uuid, beta_type.object_uuid, "db.Beta", "Beta"),
        });
        records = {definitionRecord(*definitions[0], 1), definitionRecord(*definitions[1], 2)};

        addObject(seed, 1, records[0]);
        addObject(left, 2, records[0]);
        addObject(right, 3, records[1]);
        addObject(diamond, 4, records[1]);
        addObject(external, 5, records[1]);

        graph = makeGraph(false);
        root = makeRoot(graph);
    }

    void addObject(const SchemaObjectID & object, UInt64 revision, const Record & definition)
    {
        addObject(object, revision, std::vector<const Record *>{&definition});
    }

    void addObject(const SchemaObjectID & object, UInt64 revision, const std::vector<const Record *> & object_definitions)
    {
        if (object_definitions.empty())
            throw std::logic_error("synthetic physicalization object requires a definition");

        PersistedTypeReferences references;
        references.object = object;
        references.object_schema_revision = revision;
        std::vector<PersistedTypeDescriptor> occurrence_descriptors;
        std::vector<SyntheticObjectPhysicalOccurrence> before_occurrences;
        occurrence_descriptors.reserve(object_definitions.size());
        before_occurrences.reserve(object_definitions.size());
        references.descriptors.reserve(object_definitions.size());
        references.occurrence_paths.reserve(object_definitions.size());
        references.uses.reserve(object_definitions.size());
        for (const auto * definition : object_definitions)
            occurrence_descriptors.push_back(descriptor(*definition));
        references.descriptors = occurrence_descriptors;
        std::sort(
            references.descriptors.begin(), references.descriptors.end(), [](const auto & lhs, const auto & rhs) { return lhs.stableLess(rhs); });
        references.descriptors.erase(
            std::unique(
                references.descriptors.begin(),
                references.descriptors.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.hasSameInstantiation(rhs); }),
            references.descriptors.end());
        for (size_t index = 0; index < object_definitions.size(); ++index)
        {
            auto path = occurrencePath();
            path.object_ordinal = static_cast<UInt64>(index);
            before_occurrences.push_back({
                .path = path,
                .canonical_physical_type = occurrence_descriptors[index].getCanonicalPhysicalType(),
                .storage_fingerprint = occurrence_descriptors[index].getStorageFingerprint(),
                .selected_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input),
            });
            references.occurrence_paths.push_back(path);
            const auto descriptor_it = std::find_if(
                references.descriptors.begin(),
                references.descriptors.end(),
                [&](const auto & candidate) { return candidate.hasSameInstantiation(occurrence_descriptors[index]); });
            if (descriptor_it == references.descriptors.end())
                throw std::logic_error("synthetic physicalization descriptor was not interned");
            references.uses.push_back({
                .path_id = static_cast<UInt64>(index),
                .descriptor_id = static_cast<UInt64>(descriptor_it - references.descriptors.begin()),
            });
        }
        auto after_occurrences = before_occurrences;
        for (auto & occurrence : after_occurrences)
            occurrence.selected_semantic_capabilities = 0;
        const auto before_metadata
            = makeSyntheticObjectMetadata(object, revision, "synthetic-" + std::to_string(revision), before_occurrences);
        const auto after_metadata = makeSyntheticObjectMetadata(
            object, revision + 1, "synthetic-" + std::to_string(revision), after_occurrences);
        EXPECT_EQ(before_metadata.physical_schema_fingerprint, after_metadata.physical_schema_fingerprint);
        EXPECT_NE(before_metadata.canonical_record_hash, after_metadata.canonical_record_hash);
        const auto bound_before = makeSyntheticBoundPhysicalSchema(before_metadata);
        ASSERT_EQ(bound_before.occurrences.size(), object_definitions.size());
        for (const auto & occurrence : bound_before.occurrences)
            EXPECT_EQ(occurrence.selected_semantic_capabilities, semanticCapabilityBit(SemanticCapability::Input));
        const String before_bytes = encodeSyntheticObjectMetadata(before_metadata);
        const String after_bytes = encodeSyntheticObjectMetadata(after_metadata);

        references.physical_schema_fingerprint = before_metadata.physical_schema_fingerprint;
        expectations.push_back({
            .object = object,
            .object_schema_revision = revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
            .physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
        });
        provider.objects.emplace(
            object,
            PhysicalizationObject{
                .object = object,
                .object_schema_revision = revision,
                .diagnostic_name = "synthetic-" + std::to_string(revision),
                .canonical_metadata_hash
                = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, before_bytes),
                .references = std::move(references),
                .selected_semantic_capabilities
                = std::vector<SemanticCapabilityMask>(object_definitions.size(), semanticCapabilityBit(SemanticCapability::Input)),
            });
        rewrites.emplace(
            object,
            PhysicalizationRewriteImage{
                .object = object,
                .before_object_schema_revision = revision,
                .after_object_schema_revision = revision + 1,
                .before_canonical_metadata_bytes = before_bytes,
                .after_canonical_metadata_bytes = after_bytes,
                .before_physical_schema_fingerprint = before_metadata.physical_schema_fingerprint,
                .after_physical_schema_fingerprint = after_metadata.physical_schema_fingerprint,
            });
    }

    SchemaObjectDependencyGraph::Ptr makeGraph(bool move_external_to_alpha) const
    {
        std::vector<SchemaObjectID> nodes{alpha_type, beta_type, seed, left, right, diamond, external};
        std::vector<SchemaObjectDependencyEdge> edges{
            {.dependent = seed, .dependency = alpha_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = left, .dependency = alpha_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = right, .dependency = beta_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = diamond, .dependency = beta_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = external,
             .dependency = move_external_to_alpha ? alpha_type : beta_type,
             .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = left, .dependency = seed, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = right, .dependency = seed, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = diamond, .dependency = left, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = diamond, .dependency = right, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
        };
        return SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    }

    AuthorityRoot::Ptr makeRoot(SchemaObjectDependencyGraph::Ptr root_graph) const
    {
        const auto summary = inventorySummary(records, expectations);
        const auto state = makeAuthorityState(
            database_uuid,
            7,
            dependent_object_authority_capability_mask,
            summary.leaf_count,
            summary.merkle_radix_root,
            root_graph->computeRoot());
        std::vector<Test::DependentObjectResourceImageInput> inputs;
        inputs.reserve(expectations.size());
        for (const auto & expectation : expectations)
        {
            inputs.push_back({
                .canonical_metadata_bytes = rewrites.at(expectation.object).before_canonical_metadata_bytes,
                .references = provider.objects.at(expectation.object).references,
                .canonical_installation_record_bytes = {},
            });
        }
        const Test::DependentObjectResourceImageBatch dependent_objects(expectations, std::move(inputs));
        return AuthorityRootBuilder::build(
            state, 3, definitions, records, expectations, std::move(root_graph), {}, dependent_objects.get());
    }

    std::vector<PhysicalizationRewriteImage> imagesFor(const PhysicalizationPlan & plan) const
    {
        std::vector<PhysicalizationRewriteImage> result;
        result.reserve(plan.getObjects().size());
        for (const auto & object : plan.getObjects())
            result.push_back(rewrites.at(object.object));
        return result;
    }
};

template <typename Callback>
void expectMutationError(PhysicalizationMutationPlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected PhysicalizationMutationPlannerError";
    }
    catch (const PhysicalizationMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

const String & artifactBytes(
    const DatabaseSchemaWALValidatedTransition & transition,
    DatabaseSchemaWALStagedArtifactKind kind,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object)
{
    const auto & artifacts = transition.getPrepare().staged_artifacts;
    const auto bytes = transition.getStagedArtifactBytes();
    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        if (artifacts[index].kind == kind && artifacts[index].image == image && artifacts[index].object == object)
            return bytes[index];
    }
    throw std::runtime_error("artifact not found");
}

std::vector<String> copyArtifactBytes(const DatabaseSchemaWALValidatedTransition & transition)
{
    const auto bytes = transition.getStagedArtifactBytes();
    return {bytes.begin(), bytes.end()};
}

TEST(PhysicalizationMutationPlanner, OneObjectBuildsExactPhysicalOnlyTransition)
{
    Fixture fixture;
    const auto plan = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.external,
            .drop_unused_types = false,
        },
        fixture.provider);
    const auto images = fixture.imagesFor(plan);
    auto prepared = PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1001, 7, images);

    const auto & transition = prepared.getValidatedTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1);
    ASSERT_TRUE(prepare.dependent_object_deltas[0].before);
    ASSERT_TRUE(prepare.dependent_object_deltas[0].after);
    EXPECT_TRUE(prepare.dependent_object_deltas[0].before->sidecar_record_hash);
    EXPECT_TRUE(prepare.dependent_object_deltas[0].before->expectation_record_hash);
    EXPECT_FALSE(prepare.dependent_object_deltas[0].after->sidecar_record_hash);
    EXPECT_FALSE(prepare.dependent_object_deltas[0].after->expectation_record_hash);
    EXPECT_EQ(prepare.staged_artifacts.size(), 4);
    EXPECT_EQ(prepare.authority_record_deltas.size(), 1);

    const auto decoded_after = decodeSyntheticObjectMetadata(artifactBytes(
        transition,
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
        DatabaseSchemaWALStagedArtifactImage::After,
        fixture.external));
    EXPECT_EQ(decoded_after.object_schema_revision, 6);
    EXPECT_EQ(decoded_after.physical_schema_fingerprint, plan.getObjects()[0].physical_schema_fingerprint);
    const auto bound_after = makeSyntheticBoundPhysicalSchema(decoded_after);
    ASSERT_EQ(bound_after.occurrences.size(), 1);
    EXPECT_EQ(bound_after.occurrences[0].selected_semantic_capabilities, 0);
    EXPECT_EQ(
        artifactBytes(
            transition,
            DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            DatabaseSchemaWALStagedArtifactImage::Before,
            fixture.external),
        images[0].before_canonical_metadata_bytes);
    EXPECT_EQ(
        artifactBytes(
            transition,
            DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            DatabaseSchemaWALStagedArtifactImage::Before,
            fixture.external),
        encodePersistedTypeReferences(plan.getObjects()[0].references));
    EXPECT_EQ(
        artifactBytes(
            transition,
            DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            DatabaseSchemaWALStagedArtifactImage::Before,
            fixture.external),
        encodeSidecarExpectationRecord(fixture.expectations.back()));

    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(replacement.getDatabaseCatalogEpoch(), 8);
    EXPECT_EQ(replacement.getTypeIndexGeneration(), fixture.root->getTypeIndexGeneration());
    EXPECT_EQ(replacement.getTypeIndexContentDigest(), fixture.root->getTypeIndexContentDigest());
    EXPECT_EQ(replacement.getDefinitionRecords().size(), 2);
    EXPECT_EQ(replacement.getExpectationRecords().size(), 4);
    EXPECT_EQ(transition.pinAfterInventory(), replacement.pinAuthorityInventory());
    EXPECT_EQ(transition.pinAfterGraph(), replacement.pinSchemaObjectDependencyGraph());
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.external));
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsEdge({
        .dependent = fixture.external,
        .dependency = fixture.beta_type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    }));

    const auto encoded_prepare = encodeDatabaseSchemaWALPrepare(prepare);
    auto recovered = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encoded_prepare),
        {
            .authority_state = fixture.root->getAuthorityState(),
            .authority_inventory = fixture.root->pinAuthorityInventory(),
            .schema_graph = fixture.root->pinSchemaObjectDependencyGraph(),
        },
        copyArtifactBytes(transition));
    EXPECT_EQ(recovered.getPrepare(), prepare);
    EXPECT_EQ(recovered.getAfterInventory().getSummary(), replacement.getInventorySummary());
    EXPECT_EQ(recovered.getAfterGraph().computeRoot(), replacement.getSchemaObjectDependencyGraph().computeRoot());
}

TEST(PhysicalizationMutationPlanner, DiamondClosureDropsOnlyExactUnusedDefinitionAndIsDeterministic)
{
    Fixture fixture;
    const PhysicalizationSelector selector{
        .scope = PhysicalizationScope::DependentClosure,
        .object = fixture.seed,
        .drop_unused_types = true,
    };
    const auto first_plan = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);
    const auto second_plan = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);
    EXPECT_EQ(first_plan.getCanonicalScopeBytes(), second_plan.getCanonicalScopeBytes());
    EXPECT_EQ(first_plan.getCanonicalManifestBytes(), second_plan.getCanonicalManifestBytes());
    ASSERT_EQ(first_plan.getObjects().size(), 4);
    ASSERT_EQ(first_plan.getDefinitions().size(), 2);
    EXPECT_TRUE(first_plan.getDefinitions()[0].selected_for_drop);
    EXPECT_FALSE(first_plan.getDefinitions()[1].selected_for_drop);

    const auto images = fixture.imagesFor(first_plan);
    auto first = PhysicalizationMutationPlanner::plan(*fixture.root, first_plan, 1002, 7, images);
    auto second = PhysicalizationMutationPlanner::plan(*fixture.root, second_plan, 1002, 7, images);
    EXPECT_EQ(first.getValidatedTransition().getPrepare(), second.getValidatedTransition().getPrepare());
    const auto first_artifacts = first.getValidatedTransition().getStagedArtifactBytes();
    const auto second_artifacts = second.getValidatedTransition().getStagedArtifactBytes();
    EXPECT_TRUE(std::equal(first_artifacts.begin(), first_artifacts.end(), second_artifacts.begin(), second_artifacts.end()));

    const auto & prepare = first.getValidatedTransition().getPrepare();
    EXPECT_EQ(prepare.dependent_object_deltas.size(), 4);
    EXPECT_EQ(prepare.authority_record_deltas.size(), 5);
    EXPECT_EQ(prepare.staged_artifacts.size(), 17);
    const auto & replacement = first.getReplacementRoot();
    ASSERT_EQ(replacement.getDefinitionRecords().size(), 1);
    EXPECT_EQ(replacement.getDefinitionRecords()[0].identity.type_uuid, fixture.beta_type.object_uuid);
    ASSERT_EQ(replacement.getExpectationRecords().size(), 1);
    EXPECT_EQ(replacement.getExpectationRecords()[0].object, fixture.external);
    EXPECT_EQ(replacement.getTypeIndexGeneration(), fixture.root->getTypeIndexGeneration() + 1);
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.alpha_type));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.seed));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge({
        .dependent = fixture.diamond,
        .dependency = fixture.left,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
    }));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge({
        .dependent = fixture.external,
        .dependency = fixture.beta_type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    }));
}

TEST(PhysicalizationMutationPlanner, DatabaseDropUnusedIncludesAndRemovesNeverReferencedDefinitions)
{
    const UUID database_uuid = uuid(0x40, 1);
    const auto definitions = TemplateChecker::checkAll({
        definitionInput(database_uuid, uuid(0x41, 1), "db.StandaloneA", "StandaloneA"),
        definitionInput(database_uuid, uuid(0x41, 2), "db.StandaloneB", "StandaloneB"),
    });
    const std::vector<Record> records{
        definitionRecord(*definitions[0], 1),
        definitionRecord(*definitions[1], 2),
    };
    const std::vector<SchemaObjectID> nodes{
        objectID(SchemaObjectKind::TypeDefinition, database_uuid, definitions[0]->getIdentity().type_uuid),
        objectID(SchemaObjectKind::TypeDefinition, database_uuid, definitions[1]->getIdentity().type_uuid),
    };
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, {});
    const auto summary = inventorySummary(records, std::span<const SidecarExpectationRecord>{});
    const auto state = makeAuthorityState(
        database_uuid, 7, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    auto root = AuthorityRootBuilder::build(state, 3, definitions, records, {}, std::move(graph));
    Provider provider;

    const auto plan = PhysicalizationPlanner::build(
        *root,
        {
            .scope = PhysicalizationScope::Database,
            .object = std::nullopt,
            .drop_unused_types = true,
        },
        provider);
    EXPECT_TRUE(plan.getObjects().empty());
    ASSERT_EQ(plan.getDefinitions().size(), definitions.size());
    for (const auto & definition : plan.getDefinitions())
        EXPECT_TRUE(definition.selected_for_drop);

    auto prepared = PhysicalizationMutationPlanner::plan(*root, plan, 1'031, 7, std::span<const PhysicalizationRewriteImage>{});
    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    EXPECT_EQ(prepare.authority_record_deltas.size(), definitions.size());
    EXPECT_TRUE(prepare.dependent_object_deltas.empty());
    EXPECT_EQ(prepare.staged_artifacts.size(), definitions.size());
    EXPECT_EQ(prepare.graph_delta.node_removals, nodes);
    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_TRUE(replacement.getDefinitionRecords().empty());
    EXPECT_EQ(replacement.getSchemaObjectDependencyGraph().getNodeCount(), 0u);
    EXPECT_EQ(replacement.getTypeIndexGeneration(), root->getTypeIndexGeneration() + 1);

    const auto no_op_plan = PhysicalizationPlanner::build(
        replacement,
        {
            .scope = PhysicalizationScope::Database,
            .object = std::nullopt,
            .drop_unused_types = true,
        },
        provider);
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::InvalidRequest,
        [&]
        {
            static_cast<void>(
                PhysicalizationMutationPlanner::plan(replacement, no_op_plan, 1'032, 8, std::span<const PhysicalizationRewriteImage>{}));
        });
}

TEST(PhysicalizationMutationPlanner, ObjectDropUnusedDropsExclusiveAndRetainsSharedDefinition)
{
    Fixture fixture;
    fixture.expectations.clear();
    fixture.provider.objects.clear();
    fixture.rewrites.clear();
    const SchemaObjectID target = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x35, 1));
    const SchemaObjectID shared_user = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x35, 2));
    fixture.addObject(target, 10, std::vector<const Record *>{&fixture.records[0], &fixture.records[1]});
    fixture.addObject(shared_user, 11, fixture.records[1]);

    const std::vector<SchemaObjectID> nodes{fixture.alpha_type, fixture.beta_type, target, shared_user};
    const SchemaObjectDependencyEdge target_alpha{
        .dependent = target,
        .dependency = fixture.alpha_type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge target_beta{
        .dependent = target,
        .dependency = fixture.beta_type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyEdge shared_beta{
        .dependent = shared_user,
        .dependency = fixture.beta_type,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    auto graph = SchemaObjectDependencyGraph::build(
        fixture.database_uuid, nodes, std::vector<SchemaObjectDependencyEdge>{target_alpha, target_beta, shared_beta});
    auto root = fixture.makeRoot(std::move(graph));

    const auto plan = PhysicalizationPlanner::build(
        *root,
        {
            .scope = PhysicalizationScope::Object,
            .object = target,
            .drop_unused_types = true,
        },
        fixture.provider);
    ASSERT_EQ(plan.getObjects().size(), 1u);
    ASSERT_EQ(plan.getDefinitions().size(), 2u);
    const auto alpha = std::find_if(
        plan.getDefinitions().begin(),
        plan.getDefinitions().end(),
        [&](const auto & definition) { return definition.identity.type_uuid == fixture.alpha_type.object_uuid; });
    const auto beta = std::find_if(
        plan.getDefinitions().begin(),
        plan.getDefinitions().end(),
        [&](const auto & definition) { return definition.identity.type_uuid == fixture.beta_type.object_uuid; });
    ASSERT_NE(alpha, plan.getDefinitions().end());
    ASSERT_NE(beta, plan.getDefinitions().end());
    EXPECT_TRUE(alpha->selected_for_drop);
    EXPECT_FALSE(beta->selected_for_drop);

    const auto images = fixture.imagesFor(plan);
    auto prepared = PhysicalizationMutationPlanner::plan(*root, plan, 1'032, 7, images);
    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    EXPECT_EQ(prepare.dependent_object_deltas.size(), 1u);
    EXPECT_EQ(prepare.authority_record_deltas.size(), 2u);
    EXPECT_EQ(prepare.staged_artifacts.size(), 5u);
    const auto & replacement = prepared.getReplacementRoot();
    ASSERT_EQ(replacement.getDefinitionRecords().size(), 1u);
    EXPECT_EQ(replacement.getDefinitionRecords().front().identity.type_uuid, fixture.beta_type.object_uuid);
    ASSERT_EQ(replacement.getExpectationRecords().size(), 1u);
    EXPECT_EQ(replacement.getExpectationRecords().front().object, shared_user);
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.alpha_type));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsNode(fixture.beta_type));
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsEdge(target_alpha));
    EXPECT_FALSE(replacement.getSchemaObjectDependencyGraph().containsEdge(target_beta));
    EXPECT_TRUE(replacement.getSchemaObjectDependencyGraph().containsEdge(shared_beta));
}

TEST(PhysicalizationMutationPlanner, RejectsStaleHashRevisionFingerprintAndImageCardinality)
{
    Fixture fixture;
    const auto plan = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.external,
            .drop_unused_types = false,
        },
        fixture.provider);
    const auto valid = fixture.imagesFor(plan);

    expectMutationError(
        PhysicalizationMutationPlannerError::Code::ExpectedEpochMismatch,
        [&] { static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 8, valid)); });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::InvalidRewriteImages,
        [&]
        {
            auto images = valid;
            images.clear();
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, images));
        });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::InvalidRewriteImages,
        [&]
        {
            auto images = valid;
            images.push_back(images.front());
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, images));
        });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::InvalidRewriteImages,
        [&]
        {
            auto images = valid;
            ++images[0].after_object_schema_revision;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, images));
        });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::StalePlan,
        [&]
        {
            auto images = valid;
            images[0].before_canonical_metadata_bytes[0] ^= 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, images));
        });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::IntegrityMismatch,
        [&]
        {
            auto images = valid;
            images[0].after_physical_schema_fingerprint[0] ^= 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, images));
        });
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::LimitExceeded,
        [&]
        {
            PhysicalizationMutationPlannerLimits limits;
            limits.schema_wal.maximum_staged_artifact_bytes = valid[0].before_canonical_metadata_bytes.size() - 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1003, 7, valid, limits));
        });
}

TEST(PhysicalizationMutationPlanner, EnforcesPinnedWALLimitsOnThePhysicalizationFastPath)
{
    Fixture fixture;
    const auto plan = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.external,
            .drop_unused_types = false,
        },
        fixture.provider);
    const auto images = fixture.imagesFor(plan);

    expectMutationError(
        PhysicalizationMutationPlannerError::Code::LimitExceeded,
        [&]
        {
            PhysicalizationMutationPlannerLimits limits;
            limits.schema_wal.inventory_snapshot.inventory.maximum_leaves = fixture.root->getInventorySummary().leaf_count - 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1010, 7, images, limits));
        });

    const auto pinned_inventory = fixture.root->pinAuthorityInventory();
    ASSERT_FALSE(pinned_inventory->getLeaves().empty());
    const UInt64 leaf_bytes = encodeAuthorityInventoryLeaf(pinned_inventory->getLeaves().front()).size();
    ASSERT_GT(leaf_bytes, 1u);
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::LimitExceeded,
        [&]
        {
            PhysicalizationMutationPlannerLimits limits;
            limits.schema_wal.inventory_snapshot.inventory.maximum_leaf_bytes = leaf_bytes - 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1011, 7, images, limits));
        });

    expectMutationError(
        PhysicalizationMutationPlannerError::Code::LimitExceeded,
        [&]
        {
            PhysicalizationMutationPlannerLimits limits;
            limits.schema_wal.schema_graph.maximum_nodes = fixture.graph->getNodeCount() - 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1012, 7, images, limits));
        });
}

TEST(PhysicalizationMutationPlanner, EnforcesRetainedDefinitionAndExactArtifactBudgetsProspectively)
{
    Fixture fixture;
    const PhysicalizationSelector selector{
        .scope = PhysicalizationScope::DependentClosure,
        .object = fixture.seed,
        .drop_unused_types = true,
    };
    const auto plan = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);
    const auto images = fixture.imagesFor(plan);
    auto baseline = PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1020, 7, images);

    UInt64 exact_total = 0;
    UInt64 exact_maximum = 0;
    for (const auto & artifact : baseline.getValidatedTransition().getPrepare().staged_artifacts)
    {
        exact_total += artifact.byte_size;
        exact_maximum = std::max(exact_maximum, artifact.byte_size);
    }
    ASSERT_GT(exact_maximum, 1u);
    ASSERT_GT(exact_total, exact_maximum);

    const auto check_aggregate = [&](UInt64 limit, bool succeeds)
    {
        PhysicalizationMutationPlannerLimits limits;
        limits.schema_wal.maximum_staged_artifact_bytes = exact_maximum;
        limits.schema_wal.maximum_total_staged_artifact_bytes = limit;
        if (succeeds)
        {
            auto result = PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1021, 7, images, limits);
            EXPECT_EQ(result.getValidatedTransition().getPrepare().staged_artifacts.size(), 17u);
        }
        else
        {
            expectMutationError(
                PhysicalizationMutationPlannerError::Code::LimitExceeded,
                [&]
                {
                    static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1021, 7, images, limits));
                });
        }
    };
    check_aggregate(exact_total - 1, false);
    check_aggregate(exact_total, true);
    check_aggregate(exact_total + 1, true);

    const auto check_per_artifact = [&](UInt64 limit, bool succeeds)
    {
        PhysicalizationMutationPlannerLimits limits;
        limits.schema_wal.maximum_staged_artifact_bytes = limit;
        if (succeeds)
        {
            auto result = PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1022, 7, images, limits);
            EXPECT_EQ(result.getValidatedTransition().getPrepare().staged_artifacts.size(), 17u);
        }
        else
        {
            expectMutationError(
                PhysicalizationMutationPlannerError::Code::LimitExceeded,
                [&]
                {
                    static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1022, 7, images, limits));
                });
        }
    };
    check_per_artifact(exact_maximum - 1, false);
    check_per_artifact(exact_maximum, true);
    check_per_artifact(exact_maximum + 1, true);

    ASSERT_FALSE(plan.getDefinitions().empty());
    const auto checked_definition = fixture.root->findByIdentity(plan.getDefinitions().front().identity);
    ASSERT_TRUE(checked_definition);
    const auto exact_retained = tryCountLogicalRetainedDefinitionBytes(*checked_definition, std::numeric_limits<UInt64>::max());
    ASSERT_TRUE(exact_retained);
    ASSERT_GT(*exact_retained, 1u);
    expectMutationError(
        PhysicalizationMutationPlannerError::Code::LimitExceeded,
        [&]
        {
            PhysicalizationMutationPlannerLimits limits;
            limits.maximum_definition_retained_bytes = *exact_retained - 1;
            static_cast<void>(PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1023, 7, images, limits));
        });
    for (const UInt64 accepted_limit : {*exact_retained, *exact_retained + 1})
    {
        PhysicalizationMutationPlannerLimits limits;
        limits.maximum_definition_retained_bytes = accepted_limit;
        auto result = PhysicalizationMutationPlanner::plan(*fixture.root, plan, 1024, 7, images, limits);
        EXPECT_EQ(result.getValidatedTransition().getPrepare().staged_artifacts.size(), 17u);
    }
}

TEST(PhysicalizationMutationPlanner, RejectsPlanWhenGraphGainsExternalDefinitionDependent)
{
    Fixture fixture;
    const auto plan = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::DependentClosure,
            .object = fixture.seed,
            .drop_unused_types = true,
        },
        fixture.provider);
    const auto images = fixture.imagesFor(plan);
    auto changed_root = fixture.makeRoot(fixture.makeGraph(true));

    expectMutationError(
        PhysicalizationMutationPlannerError::Code::RemainingDependent,
        [&] { static_cast<void>(PhysicalizationMutationPlanner::plan(*changed_root, plan, 1004, 7, images)); });
}

}
}
