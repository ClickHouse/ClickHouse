#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRecovery.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <span>
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

String toHex(const Digest & value)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(value.size() * 2);
    for (const UInt8 byte : value)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

DefinitionInput definitionInput(UUID database_uuid, UUID type_uuid, std::string_view local_name = "Value")
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = "app." + String(local_name);
    input.normalized_local_name = local_name;
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return input;
}

Record definitionRecord(const Definition & definition, std::string_view body = "UInt64")
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE " + definition.getNormalizedName() + " UUID '"
                + toString(definition.getIdentity().type_uuid) + "' REVISION 1 AS " + String(body) + " DEFINITION HASH '"
                + toHex(definition.getDefinitionHash()) + "' COMMENT 'recovered'",
            .canonical_physical_template_sql = String(body),
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = "recovered",
            .creation_time_us_utc = 123,
        });
}

struct RecoveryFixture
{
    UUID database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    UUID type_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe01ULL);
    Definition::Ptr definition = TemplateChecker::checkAll({definitionInput(database_uuid, type_uuid)}).front();
    Record record = definitionRecord(*definition);
    String record_bytes = encodeRecord(record);
    AuthorityInventoryLeaf leaf{
        .key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = type_uuid,
        },
        .object_revision = record.identity.revision,
        .canonical_record_hash = computeRecordHash(record),
    };
    AuthorityInventorySnapshot inventory_snapshot = makeAuthorityInventorySnapshot(database_uuid, {leaf});
    String inventory_snapshot_bytes = encodeAuthorityInventorySnapshot(inventory_snapshot);
    SchemaObjectID type_object{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = type_uuid,
    };
    SchemaObjectDependencyGraph::Ptr graph = SchemaObjectDependencyGraph::build(database_uuid, {&type_object, 1}, {});
    String graph_snapshot_bytes = graph->encodeSnapshot();
    AuthorityState state = makeAuthorityState(
        database_uuid,
        1,
        definition_authority_capability_mask,
        1,
        buildAuthorityInventorySummary({&leaf, 1}).merkle_radix_root,
        graph->computeRoot());

    AuthorityRoot::Ptr recover() const { return recoverRecord(record); }

    AuthorityRoot::Ptr recoverRecord(const Record & candidate, const AuthorityRecoveryLimits & limits = {}) const
    {
        const String candidate_bytes = encodeRecord(candidate);
        AuthorityInventoryLeaf candidate_leaf{
            .key = leaf.key,
            .object_revision = candidate.identity.revision,
            .canonical_record_hash = computeRecordHash(candidate),
        };
        const auto candidate_snapshot = makeAuthorityInventorySnapshot(database_uuid, {candidate_leaf});
        const auto candidate_state = makeAuthorityState(
            database_uuid,
            1,
            definition_authority_capability_mask,
            1,
            buildAuthorityInventorySummary({&candidate_leaf, 1}).merkle_radix_root,
            graph->computeRoot());
        const std::array images{AuthorityRecordImage{
            .key = candidate_leaf.key,
            .canonical_bytes = candidate_bytes,
        }};
        return recoverAuthorityRoot(
            candidate_state, 1, encodeAuthorityInventorySnapshot(candidate_snapshot), graph_snapshot_bytes, images, limits);
    }
};

template <typename Callback>
void expectRecoveryError(
    AuthorityRecoveryError::Code expected, Callback && callback, std::optional<AuthorityInventoryKey> expected_record_key = std::nullopt)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityRecoveryError";
    }
    catch (const AuthorityRecoveryError & error)
    {
        EXPECT_EQ(error.code, expected);
        if (expected_record_key)
        {
            ASSERT_TRUE(error.record_key.has_value());
            EXPECT_EQ(*error.record_key, *expected_record_key);
        }
    }
}

AuthorityInventoryKey typeRecordKey(const Record & record)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = record.identity.type_uuid,
    };
}

AuthorityRoot::Ptr recoverAuthorityWithResourceImages(
    UUID database_uuid,
    UInt64 capability_mask,
    const std::vector<Record> & definitions,
    const std::vector<SidecarExpectationRecord> & expectations,
    const SchemaObjectDependencyGraph::Ptr & graph,
    const AuthorityRecoveryLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    const std::size_t record_count = definitions.size() + expectations.size();
    std::vector<String> encoded_records;
    std::vector<AuthorityInventoryLeaf> leaves;
    std::vector<AuthorityRecordImage> images;
    encoded_records.reserve(record_count);
    leaves.reserve(record_count);
    images.reserve(record_count);

    for (const auto & record : definitions)
    {
        encoded_records.push_back(encodeRecord(record));
        leaves.push_back({
            .key = typeRecordKey(record),
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record),
        });
        images.push_back({.key = leaves.back().key, .canonical_bytes = encoded_records.back()});
    }
    for (const auto & record : expectations)
    {
        encoded_records.push_back(encodeSidecarExpectationRecord(record));
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = record.object.object_uuid,
            },
            .object_revision = record.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(record),
        });
        images.push_back({.key = leaves.back().key, .canonical_bytes = encoded_records.back()});
    }

    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    std::reverse(images.begin(), images.end());
    const auto inventory_snapshot = makeAuthorityInventorySnapshot(database_uuid, leaves);
    const auto inventory_summary = buildAuthorityInventorySummary(leaves);
    const UInt64 database_catalog_epoch = capability_mask == dependent_object_authority_capability_mask ? 2 : 1;
    const auto state = makeAuthorityState(
        database_uuid, database_catalog_epoch, capability_mask, record_count, inventory_summary.merkle_radix_root, graph->computeRoot());
    return recoverAuthorityRoot(
        state,
        recoveredTypeIndexGeneration(state),
        encodeAuthorityInventorySnapshot(inventory_snapshot),
        graph->encodeSnapshot(),
        images,
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr recoverAuthority(
    UUID database_uuid,
    UInt64 capability_mask,
    const std::vector<Record> & definitions,
    const std::vector<SidecarExpectationRecord> & expectations,
    const SchemaObjectDependencyGraph::Ptr & graph,
    const AuthorityRecoveryLimits & limits = {},
    std::span<const PersistedTypeReferences> dependent_references = {})
{
    const Test::DependentObjectResourceImageBatch dependent_objects(
        expectations, Test::dependentObjectResourceImageInputs(dependent_references));
    return recoverAuthorityWithResourceImages(
        database_uuid, capability_mask, definitions, expectations, graph, limits, dependent_objects.get());
}

struct DependentRecoveryFixture
{
    UUID database_uuid = uuid(0x710, 0x810);
    UUID type_uuid = uuid(0x910, 1);
    Definition::Ptr definition;
    std::vector<Record> definitions;
    SchemaObjectID type_object;
    SchemaObjectID dependent_object;
    PersistedTypeReferences references;
    SidecarExpectationRecord expectation;
    std::vector<SidecarExpectationRecord> expectations;
    SchemaObjectDependencyGraph::Ptr graph;

    DependentRecoveryFixture()
    {
        definition = TemplateChecker::checkAll({definitionInput(database_uuid, type_uuid)}).front();
        definitions.push_back(definitionRecord(*definition));
        type_object = {
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = type_uuid,
        };
        dependent_object = {
            .kind = SchemaObjectKind::SyntheticTestObject,
            .database_uuid = database_uuid,
            .object_uuid = uuid(0xa10, 1),
        };
        references = Test::singleDefinitionPersistedTypeReferences(
            dependent_object, 7, {}, definition, std::make_shared<DataTypeUInt64>(), PersistedTypePathSection::SyntheticPayload);
        expectation = Test::sidecarExpectationFor(references);
        expectations.push_back(expectation);
        const std::array nodes{type_object, dependent_object};
        const std::array edge{SchemaObjectDependencyEdge{
            .dependent = dependent_object,
            .dependency = type_object,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        }};
        graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edge);
    }
};

TEST(AuthorityRecovery, EmptyDefinitionOnlyAuthorityRecoversAsOneRoot)
{
    const UUID database_uuid = uuid(1, 2);
    const auto inventory_snapshot = makeAuthorityInventorySnapshot(database_uuid, {});
    const auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto state = makeAuthorityState(
        database_uuid,
        1,
        definition_authority_capability_mask,
        0,
        buildAuthorityInventorySummary({}).merkle_radix_root,
        graph->computeRoot());

    auto root = recoverAuthorityRoot(
        state, recoveredTypeIndexGeneration(state), encodeAuthorityInventorySnapshot(inventory_snapshot), graph->encodeSnapshot(), {});
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->getDatabaseUUID(), database_uuid);
    EXPECT_EQ(root->getDatabaseCatalogEpoch(), 1);
    EXPECT_EQ(root->getTypeIndexGeneration(), 1);
    EXPECT_TRUE(root->getDefinitionRecords().empty());
}

TEST(AuthorityRecovery, AnchoredRecordIsParsedCheckedAndPublished)
{
    RecoveryFixture fixture;
    auto root = fixture.recover();
    ASSERT_NE(root, nullptr);
    ASSERT_TRUE(root->findByIdentity(fixture.record.identity));
    EXPECT_EQ(root->findByIdentity(fixture.record.identity)->getDefinitionHash(), fixture.record.definition_hash);
    EXPECT_EQ(root->findByName("Value")->getIdentity(), fixture.record.identity);
    EXPECT_EQ(root->getInventorySummary().merkle_radix_root, fixture.state.inventory_root);
    EXPECT_EQ(root->getSchemaObjectDependencyGraph().computeRoot(), fixture.state.schema_graph_root);
}

TEST(AuthorityRecovery, AtomicLimitsAreCoherentAndDefinitionCountIsProspective)
{
    const AuthorityRecoveryLimits limits;
    EXPECT_EQ(limits.root.maximum_definition_records, 10'000);
    EXPECT_EQ(limits.root.type_catalog.maximum_definitions, 10'000);
    EXPECT_EQ(limits.lowering.maximum_definitions, 10'000);
    EXPECT_EQ(limits.checker.maximum_definitions, 10'000);
    EXPECT_EQ(limits.root.maximum_expectation_records, 100'000);
    EXPECT_EQ(limits.maximum_record_images, 110'000);
    EXPECT_EQ(limits.root.inventory.maximum_leaves, 110'000);
    EXPECT_EQ(limits.root.authority_state.maximum_leaves, 110'000);
    EXPECT_EQ(limits.inventory_snapshot.inventory.maximum_leaves, 110'000);

    const UUID database_uuid = uuid(0x100, 0x200);
    std::vector<AuthorityInventoryLeaf> leaves;
    std::vector<AuthorityRecordImage> images;
    leaves.reserve(10'001);
    images.reserve(10'001);
    for (UInt64 index = 0; index < 10'001; ++index)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = uuid(0x300, index + 1),
            },
            .object_revision = 1,
        });
        images.push_back({.key = leaves.back().key, .canonical_bytes = {}});
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    const auto inventory_snapshot = makeAuthorityInventorySnapshot(database_uuid, leaves);
    const auto inventory_summary = buildAuthorityInventorySummary(leaves);
    const auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto state = makeAuthorityState(
        database_uuid, 1, definition_authority_capability_mask, leaves.size(), inventory_summary.merkle_radix_root, graph->computeRoot());
    expectRecoveryError(
        AuthorityRecoveryError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                recoverAuthorityRoot(state, 1, encodeAuthorityInventorySnapshot(inventory_snapshot), graph->encodeSnapshot(), images));
        });
}

TEST(AuthorityRecovery, AuthorityStateAndFrozenLimitsAreValidatedBeforeRecordParsing)
{
    RecoveryFixture fixture;
    auto invalid_state = fixture.state;
    invalid_state.anchor_hash.front() ^= 0xff;
    const std::array malformed_image{AuthorityRecordImage{
        .key = fixture.leaf.key,
        .canonical_bytes = {},
    }};
    expectRecoveryError(
        AuthorityRecoveryError::Code::AuthorityStateMismatch,
        [&]
        {
            static_cast<void>(
                recoverAuthorityRoot(invalid_state, 1, fixture.inventory_snapshot_bytes, fixture.graph_snapshot_bytes, malformed_image));
        });

    AuthorityRecoveryLimits limits;
    limits.maximum_parser_depth = authority_recovery_maximum_parser_depth + 1;
    expectRecoveryError(
        AuthorityRecoveryError::Code::InvalidConfiguration, [&] { static_cast<void>(fixture.recoverRecord(fixture.record, limits)); });

    const std::array image{AuthorityRecordImage{
        .key = fixture.leaf.key,
        .canonical_bytes = fixture.record_bytes,
    }};
    expectRecoveryError(
        AuthorityRecoveryError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(
                recoverAuthorityRoot(fixture.state, 2, fixture.inventory_snapshot_bytes, fixture.graph_snapshot_bytes, image));
        });
}

TEST(AuthorityRecovery, RecordImagesMustExactlyMatchAnchoredInventory)
{
    RecoveryFixture fixture;
    expectRecoveryError(
        AuthorityRecoveryError::Code::InventoryMismatch,
        [&]
        { static_cast<void>(recoverAuthorityRoot(fixture.state, 1, fixture.inventory_snapshot_bytes, fixture.graph_snapshot_bytes, {})); });

    const std::array duplicate_images{
        AuthorityRecordImage{.key = fixture.leaf.key, .canonical_bytes = fixture.record_bytes},
        AuthorityRecordImage{.key = fixture.leaf.key, .canonical_bytes = fixture.record_bytes},
    };
    expectRecoveryError(
        AuthorityRecoveryError::Code::InventoryMismatch,
        [&]
        {
            static_cast<void>(
                recoverAuthorityRoot(fixture.state, 1, fixture.inventory_snapshot_bytes, fixture.graph_snapshot_bytes, duplicate_images));
        });
}

TEST(AuthorityRecovery, CanonicalAttachIdentityAndExecutableSemanticsAreRechecked)
{
    RecoveryFixture fixture;
    auto wrong_hash_record = fixture.record;
    wrong_hash_record.canonical_definition_sql.replace(
        wrong_hash_record.canonical_definition_sql.find(toHex(wrong_hash_record.definition_hash)), 64, String(64, '0'));
    const String wrong_hash_bytes = encodeRecord(wrong_hash_record);
    auto wrong_hash_leaf = fixture.leaf;
    wrong_hash_leaf.canonical_record_hash = computeRecordHash(wrong_hash_record);
    const auto wrong_hash_snapshot = makeAuthorityInventorySnapshot(fixture.database_uuid, {wrong_hash_leaf});
    auto wrong_hash_state = makeAuthorityState(
        fixture.database_uuid,
        1,
        definition_authority_capability_mask,
        1,
        buildAuthorityInventorySummary({&wrong_hash_leaf, 1}).merkle_radix_root,
        fixture.graph->computeRoot());
    const std::array wrong_hash_images{AuthorityRecordImage{
        .key = wrong_hash_leaf.key,
        .canonical_bytes = wrong_hash_bytes,
    }};
    expectRecoveryError(
        AuthorityRecoveryError::Code::CanonicalSQLMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityRoot(
                wrong_hash_state,
                1,
                encodeAuthorityInventorySnapshot(wrong_hash_snapshot),
                fixture.graph_snapshot_bytes,
                wrong_hash_images));
        });

    auto wrong_body_record = fixture.record;
    wrong_body_record.canonical_definition_sql.replace(wrong_body_record.canonical_definition_sql.find("UInt64"), 6, "String");
    wrong_body_record.canonical_physical_template_sql = "String";
    const String wrong_body_bytes = encodeRecord(wrong_body_record);
    auto wrong_body_leaf = fixture.leaf;
    wrong_body_leaf.canonical_record_hash = computeRecordHash(wrong_body_record);
    const auto wrong_body_snapshot = makeAuthorityInventorySnapshot(fixture.database_uuid, {wrong_body_leaf});
    auto wrong_body_state = makeAuthorityState(
        fixture.database_uuid,
        1,
        definition_authority_capability_mask,
        1,
        buildAuthorityInventorySummary({&wrong_body_leaf, 1}).merkle_radix_root,
        fixture.graph->computeRoot());
    const std::array wrong_body_images{AuthorityRecordImage{
        .key = wrong_body_leaf.key,
        .canonical_bytes = wrong_body_bytes,
    }};
    expectRecoveryError(
        AuthorityRecoveryError::Code::DefinitionMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityRoot(
                wrong_body_state,
                1,
                encodeAuthorityInventorySnapshot(wrong_body_snapshot),
                fixture.graph_snapshot_bytes,
                wrong_body_images));
        },
        fixture.leaf.key);
}

TEST(AuthorityRecovery, CanonicalAttachMustBeQualifiedCanonicalAndMatchTheRecordName)
{
    RecoveryFixture fixture;

    auto unqualified = fixture.record;
    unqualified.canonical_definition_sql.replace(unqualified.canonical_definition_sql.find("app.Value"), 9, "Value");
    expectRecoveryError(
        AuthorityRecoveryError::Code::CanonicalSQLMismatch,
        [&] { static_cast<void>(fixture.recoverRecord(unqualified)); },
        fixture.leaf.key);

    auto malformed = fixture.record;
    malformed.canonical_definition_sql = "ATTACH TYPE";
    expectRecoveryError(
        AuthorityRecoveryError::Code::CanonicalSQLMismatch, [&] { static_cast<void>(fixture.recoverRecord(malformed)); }, fixture.leaf.key);

    auto execution_clause = fixture.record;
    execution_clause.canonical_definition_sql.insert(execution_clause.canonical_definition_sql.find("app.Value"), "IF NOT EXISTS ");
    expectRecoveryError(
        AuthorityRecoveryError::Code::CanonicalSQLMismatch,
        [&] { static_cast<void>(fixture.recoverRecord(execution_clause)); },
        fixture.leaf.key);

    auto wrong_name = fixture.record;
    wrong_name.canonical_definition_sql.replace(wrong_name.canonical_definition_sql.find("app.Value"), 9, "app.Other");
    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch, [&] { static_cast<void>(fixture.recoverRecord(wrong_name)); }, fixture.leaf.key);
}

TEST(AuthorityRecovery, AnchoredComponentFailuresHaveStableRecoveryCodesAndRecordKeys)
{
    RecoveryFixture fixture;
    const std::array truncated_image{AuthorityRecordImage{
        .key = fixture.leaf.key,
        .canonical_bytes = std::string_view(fixture.record_bytes).substr(0, 7),
    }};
    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch,
        [&]
        {
            static_cast<void>(
                recoverAuthorityRoot(fixture.state, 1, fixture.inventory_snapshot_bytes, fixture.graph_snapshot_bytes, truncated_image));
        },
        fixture.leaf.key);

    expectRecoveryError(
        AuthorityRecoveryError::Code::InventoryMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityRoot(
                fixture.state,
                1,
                std::string_view(fixture.inventory_snapshot_bytes).substr(0, 7),
                fixture.graph_snapshot_bytes,
                truncated_image));
        });

    expectRecoveryError(
        AuthorityRecoveryError::Code::SchemaGraphMismatch,
        [&]
        {
            const std::array image{AuthorityRecordImage{
                .key = fixture.leaf.key,
                .canonical_bytes = fixture.record_bytes,
            }};
            static_cast<void>(recoverAuthorityRoot(
                fixture.state, 1, fixture.inventory_snapshot_bytes, std::string_view(fixture.graph_snapshot_bytes).substr(0, 7), image));
        });
}

TEST(AuthorityRecovery, AggregateLoweredInputBudgetIsChargedBeforeTheSecondDefinitionIsRetained)
{
    const UUID database_uuid = uuid(0x400, 0x500);
    const UUID first_uuid = uuid(0x600, 1);
    const UUID second_uuid = uuid(0x600, 2);
    const auto checked = TemplateChecker::checkAll(
        {definitionInput(database_uuid, first_uuid, "First"), definitionInput(database_uuid, second_uuid, "Second")});
    const std::vector records{definitionRecord(*checked[0]), definitionRecord(*checked[1])};
    const std::array type_nodes{
        SchemaObjectID{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = first_uuid,
        },
        SchemaObjectID{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = second_uuid,
        },
    };
    const auto graph = SchemaObjectDependencyGraph::build(database_uuid, type_nodes, {});
    auto root = recoverAuthority(database_uuid, definition_authority_capability_mask, records, {}, graph);
    ASSERT_NE(root, nullptr);
    EXPECT_TRUE(root->findByName("First"));
    EXPECT_TRUE(root->findByName("Second"));

    AuthorityRecoveryLimits limits;
    limits.checker.maximum_catalog_nodes = 1;
    expectRecoveryError(
        AuthorityRecoveryError::Code::LimitExceeded,
        [&] { static_cast<void>(recoverAuthority(database_uuid, definition_authority_capability_mask, records, {}, graph, limits)); },
        typeRecordKey(records[1]));
}

TEST(AuthorityRecovery, DependentObjectResourceImagesMustExactlyCloseTheExpectationSet)
{
    DependentRecoveryFixture fixture;
    const std::array references{fixture.references};
    const ::DB::UDT::Test::DependentObjectResourceImageBatch exact_images(
        fixture.expectations, ::DB::UDT::Test::dependentObjectResourceImageInputs(references));

    ASSERT_NO_THROW(
        static_cast<void>(recoverAuthorityWithResourceImages(
            fixture.database_uuid,
            dependent_object_authority_capability_mask,
            fixture.definitions,
            fixture.expectations,
            fixture.graph,
            AuthorityRecoveryLimits{},
            exact_images.get())));

    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityWithResourceImages(
                fixture.database_uuid,
                dependent_object_authority_capability_mask,
                fixture.definitions,
                fixture.expectations,
                fixture.graph,
                AuthorityRecoveryLimits{},
                {}));
        });

    std::vector<AuthorityDependentObjectResourceImage> foreign_images(exact_images.get().begin(), exact_images.get().end());
    foreign_images.front().object.object_uuid = uuid(0xa10, 2);
    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityWithResourceImages(
                fixture.database_uuid,
                dependent_object_authority_capability_mask,
                fixture.definitions,
                fixture.expectations,
                fixture.graph,
                AuthorityRecoveryLimits{},
                foreign_images));
        });
}

TEST(AuthorityRecovery, TamperedSidecarOrMetadataInstallationLinkFailsClosed)
{
    DependentRecoveryFixture fixture;
    const String canonical_metadata = "synthetic canonical metadata";
    const DependentObjectMetadataInstallationRecord installation{
        .object = fixture.dependent_object,
        .object_schema_revision = fixture.references.object_schema_revision,
        .object_name = "dependent",
        .metadata_artifact_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, canonical_metadata),
    };
    fixture.expectation.installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation);
    fixture.expectations = {fixture.expectation};
    std::vector<::DB::UDT::Test::DependentObjectResourceImageInput> inputs;
    inputs.push_back({
        .canonical_metadata_bytes = canonical_metadata,
        .references = fixture.references,
        .canonical_installation_record_bytes = encodeDependentObjectMetadataInstallationRecord(installation),
    });
    const ::DB::UDT::Test::DependentObjectResourceImageBatch exact_images(fixture.expectations, std::move(inputs));

    ASSERT_NO_THROW(
        static_cast<void>(recoverAuthorityWithResourceImages(
            fixture.database_uuid,
            dependent_object_authority_capability_mask,
            fixture.definitions,
            fixture.expectations,
            fixture.graph,
            AuthorityRecoveryLimits{},
            exact_images.get())));

    String damaged_sidecar(exact_images.get().front().canonical_sidecar_bytes);
    ASSERT_FALSE(damaged_sidecar.empty());
    damaged_sidecar.front() ^= 0x01;
    std::vector<AuthorityDependentObjectResourceImage> damaged_images(exact_images.get().begin(), exact_images.get().end());
    damaged_images.front().canonical_sidecar_bytes = damaged_sidecar;
    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityWithResourceImages(
                fixture.database_uuid,
                dependent_object_authority_capability_mask,
                fixture.definitions,
                fixture.expectations,
                fixture.graph,
                AuthorityRecoveryLimits{},
                damaged_images));
        });

    damaged_images.assign(exact_images.get().begin(), exact_images.get().end());
    damaged_images.front().canonical_metadata_bytes = "different metadata";
    expectRecoveryError(
        AuthorityRecoveryError::Code::RecordMismatch,
        [&]
        {
            static_cast<void>(recoverAuthorityWithResourceImages(
                fixture.database_uuid,
                dependent_object_authority_capability_mask,
                fixture.definitions,
                fixture.expectations,
                fixture.graph,
                AuthorityRecoveryLimits{},
                damaged_images));
        });
}

TEST(AuthorityRecovery, DependentObjectSidecarLimitIsEnforcedDuringRecovery)
{
    DependentRecoveryFixture fixture;
    auto second_occurrence = fixture.references.occurrence_paths.front();
    second_occurrence.occurrence_ordinal = 1;
    fixture.references.occurrence_paths.push_back(std::move(second_occurrence));
    fixture.references.uses.push_back({.path_id = 1, .descriptor_id = 0});
    fixture.expectation = ::DB::UDT::Test::sidecarExpectationFor(fixture.references);
    fixture.expectations = {fixture.expectation};
    const std::array references{fixture.references};
    const ::DB::UDT::Test::DependentObjectResourceImageBatch exact_images(
        fixture.expectations, ::DB::UDT::Test::dependentObjectResourceImageInputs(references));

    ASSERT_NO_THROW(
        static_cast<void>(recoverAuthorityWithResourceImages(
            fixture.database_uuid,
            dependent_object_authority_capability_mask,
            fixture.definitions,
            fixture.expectations,
            fixture.graph,
            AuthorityRecoveryLimits{},
            exact_images.get())));

    AuthorityRecoveryLimits limits;
    limits.root.resource_usage_index.persisted_references.maximum_occurrence_paths = 1;
    expectRecoveryError(
        AuthorityRecoveryError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(recoverAuthorityWithResourceImages(
                fixture.database_uuid,
                dependent_object_authority_capability_mask,
                fixture.definitions,
                fixture.expectations,
                fixture.graph,
                limits,
                exact_images.get()));
        });
}

TEST(AuthorityRecovery, DependentObjectExpectationAndGraphMustDescribeTheSameAnchoredObject)
{
    const UUID database_uuid = uuid(0x700, 0x800);
    const UUID type_uuid = uuid(0x900, 1);
    const auto definition = TemplateChecker::checkAll({definitionInput(database_uuid, type_uuid)}).front();
    const std::vector definitions{definitionRecord(*definition)};
    const SchemaObjectID type_object{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = type_uuid,
    };
    const SchemaObjectID dependent_object{
        .kind = SchemaObjectKind::SyntheticTestObject,
        .database_uuid = database_uuid,
        .object_uuid = uuid(0xa00, 1),
    };
    const auto references = ::DB::UDT::Test::singleDefinitionPersistedTypeReferences(
        dependent_object, 7, {}, definition, std::make_shared<DataTypeUInt64>(), PersistedTypePathSection::SyntheticPayload);
    const auto expectation = ::DB::UDT::Test::sidecarExpectationFor(references);
    const std::vector expectations{expectation};
    const std::vector dependent_references{references};
    const std::array nodes{type_object, expectation.object};
    const std::array edge{SchemaObjectDependencyEdge{
        .dependent = expectation.object,
        .dependency = type_object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    }};
    const std::array definition_only_nodes{type_object};
    const auto definition_only_graph = SchemaObjectDependencyGraph::build(database_uuid, definition_only_nodes, {});
    auto definition_only_root
        = recoverAuthority(database_uuid, definition_authority_capability_mask, definitions, {}, definition_only_graph);
    const auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edge);
    auto root = recoverAuthority(
        database_uuid, dependent_object_authority_capability_mask, definitions, expectations, graph, {}, dependent_references);
    ASSERT_NE(root, nullptr);
    ASSERT_NE(definition_only_root, nullptr);
    EXPECT_EQ(definition_only_root->getTypeIndexGeneration(), 1);
    EXPECT_EQ(root->getTypeIndexGeneration(), 2);
    EXPECT_EQ(definition_only_root->getTypeIndexContentDigest(), root->getTypeIndexContentDigest());
    ASSERT_EQ(root->getExpectationRecords().size(), 1);
    EXPECT_EQ(root->getExpectationRecords().front(), expectation);

    const auto graph_without_edge = SchemaObjectDependencyGraph::build(database_uuid, nodes, {});
    expectRecoveryError(
        AuthorityRecoveryError::Code::SchemaGraphMismatch,
        [&]
        {
            static_cast<void>(recoverAuthority(
                database_uuid,
                dependent_object_authority_capability_mask,
                definitions,
                expectations,
                graph_without_edge,
                {},
                dependent_references));
        });
}

}
}
