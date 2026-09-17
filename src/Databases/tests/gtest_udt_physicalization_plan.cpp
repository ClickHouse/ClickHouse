#include <Databases/UDT/PhysicalizationPlan.h>
#include <Databases/UDT/PhysicalizationTokenStore.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <map>
#include <thread>
#include <utility>

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
    return input;
}

DefinitionInput definitionCallInput(UUID database_uuid, UUID type_uuid, String qualified_name, String local_name, UUID dependency_uuid)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::DefinitionCall;
    root.dependency_ordinal = 0;
    input.nodes.push_back(std::move(root));
    input.dependencies.push_back({
        .type_uuid = dependency_uuid,
        .revision = 1,
        .target_definition_hash = {},
    });
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
    const String canonical_physical_type = "UInt64";
    const Digest storage_fingerprint = digest(static_cast<UInt8>(0x40 + (UUIDHelpers::getLowBytes(definition.identity.type_uuid) & 0xff)));
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

PersistedTypeReferences
references(const SchemaObjectID & object, UInt64 revision, const Record & definition, Digest physical_fingerprint)
{
    PersistedTypeReferences result;
    result.object = object;
    result.object_schema_revision = revision;
    result.physical_schema_fingerprint = physical_fingerprint;
    result.descriptors = {descriptor(definition)};
    result.occurrence_paths = {{
        .section = PersistedTypePathSection::SyntheticPayload,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    result.uses = {{.path_id = 0, .descriptor_id = 0}};
    return result;
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
        ++calls;
        const auto found = objects.find(expectation.object);
        if (found == objects.end())
            throw std::runtime_error("missing synthetic object");
        return found->second;
    }

    std::map<SchemaObjectID, PhysicalizationObject> objects;
    mutable UInt64 calls = 0;
};

AuthorityRoot::Ptr buildRootWithProvider(
    AuthorityState state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> definitions,
    std::span<const Record> records,
    std::span<const SidecarExpectationRecord> expectations,
    SchemaObjectDependencyGraph::Ptr graph,
    const Provider & provider)
{
    std::vector<Test::DependentObjectResourceImageInput> inputs;
    inputs.reserve(expectations.size());
    for (const auto & expectation : expectations)
    {
        inputs.push_back({
            .canonical_metadata_bytes = "synthetic-test-metadata",
            .references = provider.objects.at(expectation.object).references,
            .canonical_installation_record_bytes = {},
        });
    }
    const Test::DependentObjectResourceImageBatch dependent_objects(expectations, std::move(inputs));
    return AuthorityRootBuilder::build(
        std::move(state), type_index_generation, definitions, records, expectations, std::move(graph), {}, dependent_objects.get());
}

class CountingEntropy final : public IPhysicalizationEntropySource
{
public:
    void fill(std::span<CanonicalByte> bytes) override
    {
        ++calls;
        for (size_t index = 0; index < bytes.size(); ++index)
            bytes[index] = static_cast<CanonicalByte>(calls + index);
    }

    UInt64 calls = 0;
};

class ConstantEntropy final : public IPhysicalizationEntropySource
{
public:
    void fill(std::span<CanonicalByte> bytes) override
    {
        for (size_t index = 0; index < bytes.size(); ++index)
            bytes[index] = static_cast<CanonicalByte>(1 + index);
    }
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

    Fixture()
    {
        definitions = TemplateChecker::checkAll({
            definitionInput(database_uuid, alpha_type.object_uuid, "db.Alpha", "Alpha"),
            definitionInput(database_uuid, beta_type.object_uuid, "db.Beta", "Beta"),
        });
        records = {definitionRecord(*definitions[0], 1), definitionRecord(*definitions[1], 2)};

        addObject(seed, 1, records[0], 0x80);
        addObject(left, 2, records[0], 0x90);
        addObject(right, 3, records[1], 0xa0);
        addObject(diamond, 4, records[1], 0xb0);
        addObject(external, 5, records[1], 0xc0);

        std::vector<SchemaObjectID> nodes{alpha_type, beta_type, seed, left, right, diamond, external};
        std::vector<SchemaObjectDependencyEdge> edges{
            {.dependent = seed, .dependency = alpha_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = left, .dependency = alpha_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = right, .dependency = beta_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = diamond, .dependency = beta_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = external, .dependency = beta_type, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
            {.dependent = left, .dependency = seed, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = right, .dependency = seed, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = diamond, .dependency = left, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
            {.dependent = diamond, .dependency = right, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
        };
        graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
        const auto summary = inventorySummary(records, expectations);
        const auto state = makeAuthorityState(
            database_uuid, 7, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
        root = buildRootWithProvider(state, 3, definitions, records, expectations, graph, provider);
    }

    void addObject(const SchemaObjectID & object, UInt64 revision, const Record & definition, UInt8 fingerprint_tag)
    {
        const Digest physical_fingerprint = digest(fingerprint_tag);
        auto sidecar = references(object, revision, definition, physical_fingerprint);
        expectations.push_back({
            .object = object,
            .object_schema_revision = revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar),
            .physical_schema_fingerprint = physical_fingerprint,
        });
        provider.objects.emplace(
            object,
            PhysicalizationObject{
                .object = object,
                .object_schema_revision = revision,
                .diagnostic_name = "synthetic-" + std::to_string(revision),
                .canonical_metadata_hash = digest(static_cast<UInt8>(fingerprint_tag + 1)),
                .references = std::move(sidecar),
                .selected_semantic_capabilities = {0},
            });
    }
};

template <typename Callback>
void expectPlanError(PhysicalizationPlanError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected PhysicalizationPlanError";
    }
    catch (const PhysicalizationPlanError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

template <typename Callback>
void expectTokenError(PhysicalizationTokenStoreError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected PhysicalizationTokenStoreError";
    }
    catch (const PhysicalizationTokenStoreError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(PhysicalizationPlan, DependentClosureIsCompleteAndDeterministic)
{
    Fixture fixture;
    const PhysicalizationSelector selector{
        .scope = PhysicalizationScope::DependentClosure,
        .object = fixture.seed,
        .drop_unused_types = true,
    };
    const auto first = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);
    const auto second = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);

    ASSERT_EQ(first.getObjects().size(), 4);
    EXPECT_EQ(first.getObjects()[0].object, fixture.seed);
    EXPECT_EQ(first.getObjects()[1].object, fixture.left);
    EXPECT_EQ(first.getObjects()[2].object, fixture.right);
    EXPECT_EQ(first.getObjects()[3].object, fixture.diamond);
    ASSERT_EQ(first.getDefinitions().size(), 2);
    EXPECT_TRUE(first.getDefinitions()[0].selected_for_drop);
    EXPECT_FALSE(first.getDefinitions()[1].selected_for_drop);
    for (const auto & definition : first.getDefinitions())
    {
        const auto decoded = decodeRecord(definition.canonical_record_bytes);
        EXPECT_EQ(decoded.identity, definition.identity);
        EXPECT_EQ(decoded.normalized_name, definition.normalized_name);
        EXPECT_EQ(decoded.definition_hash, definition.definition_hash);
        EXPECT_EQ(computeRecordHash(decoded), definition.canonical_record_hash);
        EXPECT_EQ(encodeRecord(decoded), definition.canonical_record_bytes);
    }
    EXPECT_EQ(first.getScopeCount(), 4);
    EXPECT_EQ(first.getScopeBytes(), first.getCanonicalScopeBytes().size());
    EXPECT_EQ(first.getManifestBytes(), first.getCanonicalManifestBytes().size());
    EXPECT_EQ(first.getCanonicalScopeBytes(), second.getCanonicalScopeBytes());
    EXPECT_EQ(first.getScopeDigest(), second.getScopeDigest());
    EXPECT_EQ(first.getCanonicalManifestBytes(), second.getCanonicalManifestBytes());
    EXPECT_EQ(first.getManifestDigest(), second.getManifestDigest());
}

TEST(PhysicalizationPlan, ObjectScopeRefusesImplicitWidening)
{
    Fixture fixture;
    expectPlanError(
        PhysicalizationPlanError::Code::IncompleteScope,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = fixture.seed,
                    .drop_unused_types = false,
                },
                fixture.provider));
        });

    const auto isolated = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.external,
            .drop_unused_types = false,
        },
        fixture.provider);
    ASSERT_EQ(isolated.getObjects().size(), 1);
    EXPECT_EQ(isolated.getObjects().front().object, fixture.external);
}

TEST(PhysicalizationPlan, DatabaseScopeSelectsEveryExpectation)
{
    Fixture fixture;
    const auto plan = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Database,
            .object = std::nullopt,
            .drop_unused_types = true,
        },
        fixture.provider);
    EXPECT_EQ(plan.getObjects().size(), 5);
    ASSERT_EQ(plan.getDefinitions().size(), 2);
    EXPECT_TRUE(plan.getDefinitions()[0].selected_for_drop);
    EXPECT_TRUE(plan.getDefinitions()[1].selected_for_drop);
}

TEST(PhysicalizationPlan, CanonicalSidecarExpectationHashIsAcceptedAndTamperingFailsClosed)
{
    Fixture fixture;
    const PhysicalizationSelector selector{
        .scope = PhysicalizationScope::Object,
        .object = fixture.external,
        .drop_unused_types = false,
    };

    const auto valid = PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider);
    ASSERT_EQ(valid.getObjects().size(), 1);
    EXPECT_EQ(valid.getObjects().front().sidecar_hash, computePersistedTypeReferencesSidecarHash(valid.getObjects().front().references));

    auto & tampered = fixture.provider.objects.at(fixture.external).references;
    ASSERT_EQ(tampered.occurrence_paths.size(), 1);
    ++tampered.occurrence_paths.front().occurrence_ordinal;
    expectPlanError(
        PhysicalizationPlanError::Code::IntegrityMismatch,
        [&] { static_cast<void>(PhysicalizationPlanner::build(*fixture.root, selector, fixture.provider)); });
}

TEST(PhysicalizationPlan, StaleSidecarAndGraphMismatchFailClosed)
{
    Fixture fixture;
    fixture.provider.objects.at(fixture.external).references.physical_schema_fingerprint = digest(0xee);
    expectPlanError(
        PhysicalizationPlanError::Code::IntegrityMismatch,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = fixture.external,
                    .drop_unused_types = false,
                },
                fixture.provider));
        });

    Fixture second_fixture;
    auto & changed_object = second_fixture.provider.objects.at(second_fixture.external);
    changed_object.references.descriptors.front() = descriptor(second_fixture.records[0]);
    const auto changed_expectation = std::find_if(
        second_fixture.expectations.begin(),
        second_fixture.expectations.end(),
        [&](const auto & expectation) { return expectation.object == second_fixture.external; });
    ASSERT_NE(changed_expectation, second_fixture.expectations.end());
    changed_expectation->sidecar_hash = computePersistedTypeReferencesSidecarHash(changed_object.references);
    const auto summary = inventorySummary(second_fixture.records, second_fixture.expectations);
    const auto state = makeAuthorityState(
        second_fixture.database_uuid,
        7,
        dependent_object_authority_capability_mask,
        summary.leaf_count,
        summary.merkle_radix_root,
        second_fixture.graph->computeRoot());
    second_fixture.root = buildRootWithProvider(
        state,
        3,
        second_fixture.definitions,
        second_fixture.records,
        second_fixture.expectations,
        second_fixture.graph,
        second_fixture.provider);
    expectPlanError(
        PhysicalizationPlanError::Code::GraphMismatch,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *second_fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = second_fixture.external,
                    .drop_unused_types = false,
                },
                second_fixture.provider));
        });
}

TEST(PhysicalizationPlan, DropUnusedBlockingPropagatesAcrossDefinitionDependencies)
{
    const UUID database_uuid = uuid(0x100, 1);
    const SchemaObjectID alpha = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(0x110, 1));
    const SchemaObjectID beta = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(0x120, 1));
    const SchemaObjectID gamma = objectID(SchemaObjectKind::TypeDefinition, database_uuid, uuid(0x130, 1));
    const SchemaObjectID selected = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x140, 1));
    const SchemaObjectID external = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, uuid(0x150, 1));

    auto definitions = TemplateChecker::checkAll({
        definitionCallInput(database_uuid, alpha.object_uuid, "db.Alpha", "Alpha", beta.object_uuid),
        definitionCallInput(database_uuid, beta.object_uuid, "db.Beta", "Beta", gamma.object_uuid),
        definitionInput(database_uuid, gamma.object_uuid, "db.Gamma", "Gamma"),
    });
    std::vector<Record> records;
    records.reserve(definitions.size());
    for (size_t index = 0; index < definitions.size(); ++index)
        records.push_back(definitionRecord(*definitions[index], static_cast<UInt8>(index + 1)));
    std::sort(
        records.begin(),
        records.end(),
        [](const auto & lhs, const auto & rhs)
        { return uuidToCanonicalBytes(lhs.identity.type_uuid) < uuidToCanonicalBytes(rhs.identity.type_uuid); });

    const auto alpha_record
        = std::find_if(records.begin(), records.end(), [&](const auto & record) { return record.identity.type_uuid == alpha.object_uuid; });
    ASSERT_NE(alpha_record, records.end());

    Provider provider;
    std::vector<SidecarExpectationRecord> expectations;
    for (const auto & [object, revision, tag] : std::array{
             std::tuple{selected, UInt64{1}, UInt8{0x80}},
             std::tuple{external, UInt64{2}, UInt8{0x90}},
         })
    {
        const Digest physical_fingerprint = digest(tag);
        auto sidecar = references(object, revision, *alpha_record, physical_fingerprint);
        expectations.push_back({
            .object = object,
            .object_schema_revision = revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar),
            .physical_schema_fingerprint = physical_fingerprint,
        });
        provider.objects.emplace(
            object,
            PhysicalizationObject{
                .object = object,
                .object_schema_revision = revision,
                .diagnostic_name = "synthetic-" + std::to_string(revision),
                .canonical_metadata_hash = digest(static_cast<UInt8>(tag + 1)),
                .references = std::move(sidecar),
                .selected_semantic_capabilities = {0},
            });
    }
    std::sort(expectations.begin(), expectations.end(), [](const auto & lhs, const auto & rhs) { return lhs.object < rhs.object; });

    std::vector<SchemaObjectID> nodes{alpha, beta, gamma, selected, external};
    std::vector<SchemaObjectDependencyEdge> edges{
        {.dependent = alpha, .dependency = beta, .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition},
        {.dependent = beta, .dependency = gamma, .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition},
        {.dependent = selected, .dependency = alpha, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
        {.dependent = external, .dependency = alpha, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition},
    };
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    const auto summary = inventorySummary(records, expectations);
    const auto state = makeAuthorityState(
        database_uuid, 1, dependent_object_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    const auto root = buildRootWithProvider(state, 1, definitions, records, expectations, graph, provider);

    const auto plan = PhysicalizationPlanner::build(
        *root,
        {
            .scope = PhysicalizationScope::Object,
            .object = selected,
            .drop_unused_types = true,
        },
        provider);
    ASSERT_EQ(plan.getDefinitions().size(), 3);
    for (const auto & definition : plan.getDefinitions())
        EXPECT_FALSE(definition.selected_for_drop) << definition.normalized_name;
}

TEST(PhysicalizationPlan, LimitsAreProspective)
{
    Fixture fixture;
    PhysicalizationPlanLimits limits;
    limits.maximum_selected_objects = 3;
    expectPlanError(
        PhysicalizationPlanError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::DependentClosure,
                    .object = fixture.seed,
                    .drop_unused_types = false,
                },
                fixture.provider,
                limits));
        });
    EXPECT_EQ(fixture.provider.calls, 0);

    limits = {};
    limits.maximum_manifest_entries = 3;
    expectPlanError(
        PhysicalizationPlanError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::DependentClosure,
                    .object = fixture.seed,
                    .drop_unused_types = false,
                },
                fixture.provider,
                limits));
        });
    EXPECT_EQ(fixture.provider.calls, 0);

    limits = {};
    limits.maximum_scope_bytes = 64;
    expectPlanError(
        PhysicalizationPlanError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = fixture.external,
                    .drop_unused_types = false,
                },
                fixture.provider,
                limits));
        });
    EXPECT_EQ(fixture.provider.calls, 0);

    limits = {};
    limits.maximum_manifest_bytes = 255;
    expectPlanError(
        PhysicalizationPlanError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = fixture.external,
                    .drop_unused_types = false,
                },
                fixture.provider,
                limits));
        });
    EXPECT_EQ(fixture.provider.calls, 0);

    limits = {};
    limits.maximum_manifest_entries = 1;
    expectPlanError(
        PhysicalizationPlanError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(PhysicalizationPlanner::build(
                *fixture.root,
                {
                    .scope = PhysicalizationScope::Object,
                    .object = fixture.external,
                    .drop_unused_types = false,
                },
                fixture.provider,
                limits));
        });
}

TEST(PhysicalizationTokenStore, TokenIsPrincipalBoundAndSingleUse)
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
    CountingEntropy entropy;
    PhysicalizationTokenStore store(fixture.database_uuid, {}, entropy);
    const UUID principal = uuid(0x7000, 1);
    const String token = store.issue(plan, principal, 1'000, 5'000);
    EXPECT_EQ(store.getOutstandingTokenCount(), 1);
    EXPECT_GT(store.getOutstandingRecordBytes(), 0);

    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.inspectForApply(token, uuid(0x7000, 2), 2'000)); });
    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.inspectForApply("UDTPT1_not-canonical", principal, 2'000)); });
    EXPECT_EQ(store.getOutstandingTokenCount(), 1);

    const auto inspected = store.inspectForApply(token, principal, 2'000);
    EXPECT_TRUE(inspected.matches(plan));
    EXPECT_EQ(store.getOutstandingTokenCount(), 1);
    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.consumeForApply(token, principal, uuid(0x7000, 99), 2'000)); });
    EXPECT_EQ(store.getOutstandingTokenCount(), 1);
    auto binding = store.consumeForApply(token, principal, inspected.getOperationID(), 2'000);
    EXPECT_TRUE(binding.matches(plan));
    EXPECT_EQ(binding.getPrincipalUUID(), principal);
    EXPECT_EQ(store.getOutstandingTokenCount(), 0);
    EXPECT_EQ(store.getOutstandingRecordBytes(), 0);
    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.inspectForApply(token, principal, 2'000)); });
}

TEST(PhysicalizationTokenStore, RecomputedPlanMustMatchEveryBoundField)
{
    Fixture fixture;
    const auto selected = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Object,
            .object = fixture.external,
            .drop_unused_types = false,
        },
        fixture.provider);
    const auto database = PhysicalizationPlanner::build(
        *fixture.root,
        {
            .scope = PhysicalizationScope::Database,
            .object = std::nullopt,
            .drop_unused_types = false,
        },
        fixture.provider);
    CountingEntropy entropy;
    PhysicalizationTokenStore store(fixture.database_uuid, {}, entropy);
    const UUID principal = uuid(0x7000, 3);
    const String token = store.issue(selected, principal, 10, 100);
    const auto binding = store.inspectForApply(token, principal, 11);
    EXPECT_TRUE(binding.matches(selected));
    EXPECT_FALSE(binding.matches(database));
    EXPECT_EQ(store.getOutstandingTokenCount(), 1);
    static_cast<void>(store.consumeForApply(token, principal, binding.getOperationID(), 11));
}

TEST(PhysicalizationTokenStore, ExpiryAndRestartInvalidateWithoutDurableState)
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
    CountingEntropy entropy;
    PhysicalizationTokenStore store(fixture.database_uuid, {}, entropy);
    const UUID principal = uuid(0x7000, 4);
    const String expired = store.issue(plan, principal, 100, 10);
    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.inspectForApply(expired, principal, 110)); });
    EXPECT_EQ(store.getOutstandingTokenCount(), 0);

    const String restarted = store.issue(plan, principal, 200, 10);
    store.invalidateAllForRestart();
    EXPECT_EQ(store.getOutstandingTokenCount(), 0);
    expectTokenError(
        PhysicalizationTokenStoreError::Code::TokenRejected,
        [&] { static_cast<void>(store.inspectForApply(restarted, principal, 201)); });
}

TEST(PhysicalizationTokenStore, QuotasAndEntropyCollisionsFailClosed)
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
    CountingEntropy entropy;
    PhysicalizationTokenStoreLimits one_token;
    one_token.maximum_outstanding_tokens = 1;
    one_token.maximum_tokens_per_principal = 1;
    PhysicalizationTokenStore limited(fixture.database_uuid, one_token, entropy);
    static_cast<void>(limited.issue(plan, uuid(0x7000, 5), 100, 10));
    expectTokenError(
        PhysicalizationTokenStoreError::Code::LimitExceeded,
        [&] { static_cast<void>(limited.issue(plan, uuid(0x7000, 6), 100, 10)); });

    ConstantEntropy constant_entropy;
    PhysicalizationTokenStore collisions(fixture.database_uuid, {}, constant_entropy);
    static_cast<void>(collisions.issue(plan, uuid(0x7000, 7), 100, 10));
    expectTokenError(
        PhysicalizationTokenStoreError::Code::EntropyFailure,
        [&] { static_cast<void>(collisions.issue(plan, uuid(0x7000, 8), 100, 10)); });

    PhysicalizationTokenStoreLimits short_record;
    short_record.maximum_record_bytes = 128;
    PhysicalizationTokenStore bounded(fixture.database_uuid, short_record, entropy);
    expectTokenError(
        PhysicalizationTokenStoreError::Code::LimitExceeded,
        [&] { static_cast<void>(bounded.issue(plan, uuid(0x7000, 10), 100, 10)); });
    EXPECT_EQ(bounded.getOutstandingTokenCount(), 0);

    CountingEntropy sizing_entropy;
    PhysicalizationTokenStore sizing(fixture.database_uuid, {}, sizing_entropy);
    const UUID sizing_principal = uuid(0x7000, 11);
    static_cast<void>(sizing.issue(plan, sizing_principal, 100, 10));
    const UInt64 record_bytes = sizing.getOutstandingRecordBytes();
    ASSERT_GT(record_bytes, 0);

    PhysicalizationTokenStoreLimits principal_bytes;
    principal_bytes.maximum_outstanding_tokens = 4;
    principal_bytes.maximum_tokens_per_principal = 4;
    principal_bytes.maximum_record_bytes = record_bytes;
    principal_bytes.maximum_record_bytes_per_principal = record_bytes;
    principal_bytes.maximum_aggregate_record_bytes = record_bytes * 2;
    PhysicalizationTokenStore principal_limited(fixture.database_uuid, principal_bytes, entropy);
    const UUID repeated_principal = uuid(0x7000, 12);
    static_cast<void>(principal_limited.issue(plan, repeated_principal, 100, 10));
    expectTokenError(
        PhysicalizationTokenStoreError::Code::LimitExceeded,
        [&] { static_cast<void>(principal_limited.issue(plan, repeated_principal, 100, 10)); });
    EXPECT_EQ(principal_limited.getOutstandingTokenCount(), 1);

    PhysicalizationTokenStoreLimits aggregate_bytes = principal_bytes;
    aggregate_bytes.maximum_record_bytes_per_principal = record_bytes;
    aggregate_bytes.maximum_aggregate_record_bytes = record_bytes;
    PhysicalizationTokenStore aggregate_limited(fixture.database_uuid, aggregate_bytes, entropy);
    static_cast<void>(aggregate_limited.issue(plan, uuid(0x7000, 13), 100, 10));
    expectTokenError(
        PhysicalizationTokenStoreError::Code::LimitExceeded,
        [&] { static_cast<void>(aggregate_limited.issue(plan, uuid(0x7000, 14), 100, 10)); });
    EXPECT_EQ(aggregate_limited.getOutstandingTokenCount(), 1);
}

TEST(PhysicalizationTokenStore, ConcurrentApplyHasExactlyOneWinner)
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
    CountingEntropy entropy;
    PhysicalizationTokenStore store(fixture.database_uuid, {}, entropy);
    const UUID principal = uuid(0x7000, 9);
    const String token = store.issue(plan, principal, 100, 10);
    const auto inspected = store.inspectForApply(token, principal, 100);
    std::atomic<UInt64> winners = 0;
    std::atomic<UInt64> rejected = 0;
    std::atomic<UInt64> unexpected = 0;

    std::array<std::thread, 8> threads;
    for (auto & thread : threads)
    {
        thread = std::thread(
            [&]
            {
                try
                {
                    auto binding = store.consumeForApply(token, principal, inspected.getOperationID(), 101);
                    if (binding.matches(plan))
                        winners.fetch_add(1, std::memory_order_relaxed);
                }
                catch (const PhysicalizationTokenStoreError & error)
                {
                    if (error.code == PhysicalizationTokenStoreError::Code::TokenRejected)
                        rejected.fetch_add(1, std::memory_order_relaxed);
                    else
                        unexpected.fetch_add(1, std::memory_order_relaxed);
                }
            });
    }
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(winners.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(rejected.load(std::memory_order_relaxed), 7);
    EXPECT_EQ(unexpected.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(store.getOutstandingTokenCount(), 0);
    EXPECT_EQ(store.getOutstandingRecordBytes(), 0);
}

}
}
