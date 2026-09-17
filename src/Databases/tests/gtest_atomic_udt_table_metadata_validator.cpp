#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AtomicTableMetadataValidator.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Storages/ConstraintsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
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

SchemaObjectID tableObject(UUID database_uuid = uuid(1, 2))
{
    return {
        .kind = SchemaObjectKind::Table,
        .database_uuid = database_uuid,
        .object_uuid = uuid(3, 4),
    };
}

DefinitionInput aliasInput(UUID database_uuid, String local_name = "UserId", String physical_type = "UInt64")
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = uuid(0x10, 1), .revision = 1};
    input.normalized_name = "app." + local_name;
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = std::move(physical_type);
    input.nodes.push_back(std::move(root));
    return input;
}

BoundDeclaredTypeResult rootAlias(const Definition::Ptr & definition, DataTypePtr physical_type = std::make_shared<DataTypeUInt64>())
{
    const auto descriptor
        = InstantiatedTypeDescriptor::create(definition, CanonicalTypeArguments::validate(definition->getParameters(), {}), physical_type);
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = std::move(physical_type)}},
        {{.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0}},
        {});
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}

SchemaObjectID definitionObject(const Definition & definition)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = definition.getIdentity().database_uuid,
        .object_uuid = definition.getIdentity().type_uuid,
    };
}

Record
definitionRecord(const Definition & definition, const String & canonical_physical_template_sql = "UInt64")
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE " + definition.getNormalizedName() + " AS " + canonical_physical_template_sql,
            .canonical_physical_template_sql = canonical_physical_template_sql,
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = "startup binding fixture",
            .creation_time_us_utc = 123,
        });
}

AuthorityRoot::Ptr
buildDefinitionRoot(const Definition::Ptr & definition, const String & canonical_physical_template_sql = "UInt64")
{
    const std::vector<Definition::Ptr> definitions{definition};
    const std::vector<Record> records{definitionRecord(*definition, canonical_physical_template_sql)};
    std::vector<AuthorityInventoryLeaf> leaves{{
        .key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = definition->getIdentity().type_uuid,
        },
        .object_revision = definition->getIdentity().revision,
        .canonical_record_hash = computeRecordHash(records.front()),
    }};
    const auto inventory = buildAuthorityInventorySummary(leaves);
    const std::vector<SchemaObjectID> graph_nodes{definitionObject(*definition)};
    auto graph = SchemaObjectDependencyGraph::build(definition->getIdentity().database_uuid, graph_nodes, {});
    const auto state = makeAuthorityState(
        definition->getIdentity().database_uuid,
        7,
        dependent_object_authority_capability_mask,
        inventory.leaf_count,
        inventory.merkle_radix_root,
        graph->computeRoot());
    return AuthorityRootBuilder::build(state, 3, definitions, records, {}, std::move(graph));
}

AuthorityRoot::Ptr buildRootWithExpectation(
    const AuthorityRoot & base,
    const Definition & definition,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    std::string_view canonical_installation_record_bytes)
{
    const SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {expectation.object},
        .node_removals = {},
        .edge_additions = {{
            .dependent = expectation.object,
            .dependency = definitionObject(definition),
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        }},
        .edge_removals = {},
    };
    const AuthorityDependentObjectResourceImage dependent_object{
        .object = expectation.object,
        .canonical_metadata_bytes = canonical_metadata_bytes,
        .canonical_sidecar_bytes = canonical_sidecar_bytes,
        .canonical_installation_record_bytes = canonical_installation_record_bytes,
    };
    return AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
        base, base.getDatabaseCatalogEpoch() + 1, expectation, graph_delta, dependent_object, AuthorityRootBuildLimits{}, nullptr);
}

struct Fixture
{
    UUID database_uuid = uuid(1, 2);
    PreparedTableColumnTypeBindings bindings;
    String sidecar;
    SidecarExpectationRecord expectation;
    Definition::Ptr definition;
    AuthorityRoot::Ptr stale_root;
    AuthorityRoot::Ptr recovered_root;
    ASTPtr trusted_create_query;
    StoragePtr trusted_table;
    std::unique_ptr<AtomicTableMetadataValidator> validator;
    String canonical_metadata;
    String authority_metadata;
    String installation_record;

    Fixture()
    {
        definition = TemplateChecker::checkAll({aliasInput(database_uuid)}).front();
        std::vector<TableColumnTypeBindingInput> columns;
        columns.push_back({"id", rootAlias(definition)});
        columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
        columns.push_back({"materialized_id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
        bindings = prepareTableColumnTypeBindings(tableObject(database_uuid), 1, columns);
        sidecar = encodePersistedTypeReferences(*bindings.persisted_references);
        expectation = *bindings.sidecar_expectation;
        trusted_create_query = parseMetadata(validMetadata());
        MemorySettings memory_settings;
        trusted_table = std::make_shared<StorageMemory>(
            StorageID("app", "events", tableObject(database_uuid).object_uuid),
            ColumnsDescription(
                NamesAndTypesList{
                    {"id", std::make_shared<DataTypeUInt64>()},
                    {"label", std::make_shared<DataTypeString>()},
                    {"materialized_id", std::make_shared<DataTypeUInt64>()},
                }),
            ConstraintsDescription{},
            String{},
            memory_settings);
        validator = std::make_unique<AtomicTableMetadataValidator>(database_uuid, trusted_create_query, trusted_table);
        auto validated = validator->validateAndCanonicalize(expectation, validMetadata(), sidecar);
        canonical_metadata = validated.releaseCanonicalMetadataBytes();
        authority_metadata = canonical_metadata;
        const DependentObjectMetadataInstallationRecord installation{
            .object = expectation.object,
            .object_schema_revision = expectation.object_schema_revision,
            .object_name = "events",
            .metadata_artifact_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, authority_metadata),
            .semantic_extension_version = expectation.semantic_extension_version,
            .semantic_extension_flags = expectation.semantic_extension_flags,
        };
        installation_record = encodeDependentObjectMetadataInstallationRecord(installation);
        expectation.installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation);
        stale_root = buildDefinitionRoot(definition);
        recovered_root = buildRootWithExpectation(*stale_root, *definition, expectation, authority_metadata, sidecar, installation_record);
    }

    static ASTPtr parseMetadata(const String & metadata_bytes)
    {
        ParserCreateQuery parser;
        return parseQuery(
            parser,
            metadata_bytes.data(),
            metadata_bytes.data() + metadata_bytes.size(),
            "Atomic user-defined type trusted table metadata",
            16ULL << 20,
            256,
            100'000);
    }

    static String metadata(std::string_view columns)
    {
        return "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (" + String(columns) + ") ENGINE = Memory";
    }

    String validMetadata() const
    {
        return metadata(
            "id UInt64, label String, materialized_id UInt64 MATERIALIZED id, alias_id UInt64 ALIAS id, "
            "ephemeral_id UInt64 EPHEMERAL");
    }
};

template <typename Callback>
void expectValidationError(AtomicTableMetadataValidationError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected Atomic table metadata validation error";
    }
    catch (const AtomicTableMetadataValidationError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

template <typename Callback>
void expectStartupBindingFailureLeavesMetadataUnchanged(Fixture & fixture, Callback && callback)
{
    auto before_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(before_handle);
    StorageMetadataPtr before = before_handle;
    ASSERT_FALSE(before->getBoundUDTReferences());
    ASSERT_FALSE(before->getBoundUDTExpectation());

    EXPECT_THROW(callback(), std::exception);

    auto after_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(after_handle);
    StorageMetadataPtr after = after_handle;
    EXPECT_EQ(after, before);
    EXPECT_FALSE(after->getBoundUDTReferences());
    EXPECT_FALSE(after->getBoundUDTExpectation());
}

}

TEST(AtomicTableMetadataValidator, CanonicalizesFullAttachAndPreservesPhysicalColumnPolicy)
{
    Fixture fixture;
    auto validated = fixture.validator->validateAndCanonicalize(fixture.expectation, fixture.validMetadata(), fixture.sidecar);

    EXPECT_EQ(validated.getObject(), tableObject(fixture.database_uuid));
    EXPECT_EQ(validated.getObjectSchemaRevision(), 1);
    EXPECT_EQ(validated.getPhysicalSchemaFingerprint(), fixture.bindings.physical_schema_fingerprint);
    const String canonical = validated.releaseCanonicalMetadataBytes();
    EXPECT_NE(canonical.find("ATTACH TABLE _ UUID '00000000-0000-0003-0000-000000000004'"), String::npos);
    EXPECT_EQ(canonical.find("app.events"), String::npos);

    auto repeated = fixture.validator->validateAndCanonicalize(fixture.expectation, canonical, fixture.sidecar);
    EXPECT_EQ(repeated.releaseCanonicalMetadataBytes(), canonical);

    const String differently_formatted = "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004'\n"
                                         "( id UInt64,label String,materialized_id UInt64 MATERIALIZED id,alias_id UInt64 ALIAS id,"
                                         "ephemeral_id UInt64 EPHEMERAL ) ENGINE=Memory;";
    auto reformatted = fixture.validator->validateAndCanonicalize(fixture.expectation, differently_formatted, fixture.sidecar);
    EXPECT_EQ(reformatted.releaseCanonicalMetadataBytes(), canonical);
}

TEST(AtomicTableMetadataValidator, RejectsBrokenDatabaseObjectRevisionAndSidecarHashBindings)
{
    Fixture fixture;
    AtomicTableMetadataValidator foreign_validator(uuid(9, 9), fixture.trusted_create_query, fixture.trusted_table);
    expectValidationError(
        AtomicTableMetadataValidationError::Code::DatabaseMismatch,
        [&]
        { static_cast<void>(foreign_validator.validateAndCanonicalize(fixture.expectation, fixture.validMetadata(), fixture.sidecar)); });

    expectValidationError(
        AtomicTableMetadataValidationError::Code::ObjectMismatch,
        [&]
        {
            String wrong_uuid = fixture.validMetadata();
            const auto position = wrong_uuid.find("00000000-0000-0003-0000-000000000004");
            ASSERT_NE(position, String::npos);
            wrong_uuid.replace(position, 36, "00000000-0000-0003-0000-000000000005");
            static_cast<void>(fixture.validator->validateAndCanonicalize(fixture.expectation, wrong_uuid, fixture.sidecar));
        });

    auto wrong_revision = fixture.expectation;
    wrong_revision.object_schema_revision = 2;
    EXPECT_THROW(
        static_cast<void>(fixture.validator->validateAndCanonicalize(wrong_revision, fixture.validMetadata(), fixture.sidecar)),
        std::invalid_argument);

    auto wrong_hash = fixture.expectation;
    wrong_hash.sidecar_hash.front() ^= 0xff;
    EXPECT_THROW(
        static_cast<void>(fixture.validator->validateAndCanonicalize(wrong_hash, fixture.validMetadata(), fixture.sidecar)),
        std::invalid_argument);
}

TEST(AtomicTableMetadataValidator, RejectsChangedOrderedPhysicalSchema)
{
    Fixture fixture;
    for (const String & columns : std::vector<String>{
             "id UInt32, label String, materialized_id UInt64 MATERIALIZED id",
             "label String, id UInt64, materialized_id UInt64 MATERIALIZED id",
             "renamed UInt64, label String, materialized_id UInt64 MATERIALIZED renamed",
             "id UInt64, label String, materialized_id UInt64 ALIAS id",
         })
    {
        expectValidationError(
            AtomicTableMetadataValidationError::Code::PhysicalSchemaMismatch,
            [&]
            {
                static_cast<void>(
                    fixture.validator->validateAndCanonicalize(fixture.expectation, Fixture::metadata(columns), fixture.sidecar));
            });
    }
}

TEST(AtomicTableMetadataValidator, RejectsNonStoredOrNonPhysicalTableDefinitions)
{
    Fixture fixture;
    for (const String & metadata : std::vector<String>{
             "CREATE TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id UInt64) ENGINE=Memory",
             "ATTACH TABLE app.events (id UInt64) ENGINE=Memory",
             "ATTACH VIEW app.events UUID '00000000-0000-0003-0000-000000000004' AS SELECT 1",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id UInt64)",
             "ATTACH TABLE IF NOT EXISTS app.events UUID '00000000-0000-0003-0000-000000000004' (id UInt64) ENGINE=Memory",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' ON CLUSTER cluster (id UInt64) ENGINE=Memory",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id DEFAULT 1) ENGINE=Memory",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id app.UserId) ENGINE=Memory",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id UInt64, hidden app.UserId ALIAS id) ENGINE=Memory",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' (id UInt64, hidden app.UserId EPHEMERAL) ENGINE=Memory",
             fixture.validMetadata() + "; ATTACH TABLE second (id UInt64) ENGINE=Memory",
         })
    {
        expectValidationError(
            AtomicTableMetadataValidationError::Code::InvalidMetadata,
            [&] { static_cast<void>(fixture.validator->validateAndCanonicalize(fixture.expectation, metadata, fixture.sidecar)); });
    }
}

TEST(AtomicTableMetadataValidator, RejectsMetadataNotProvenByTheAlreadyCreatedTable)
{
    Fixture fixture;
    for (const String & metadata : std::vector<String>{
             Fixture::metadata(
                 "id UInt64, label String, materialized_id UInt64 MATERIALIZED id, alias_id UInt64 ALIAS id, "
                 "ephemeral_id UInt64 EPHEMERAL")
                 + " SETTINGS min_bytes_to_keep = 1",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' "
             "(id UInt64, label String, materialized_id UInt64 MATERIALIZED id, alias_id UInt64 ALIAS id, "
             "ephemeral_id UInt64 EPHEMERAL) ENGINE = DefinitelyUnknown",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' "
             "(id UInt64, label String, materialized_id UInt64 MATERIALIZED id, alias_id UInt64 ALIAS id, "
             "ephemeral_id UInt64 EPHEMERAL) ENGINE = Memory(1)",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' "
             "(id UInt64, label String, materialized_id UInt64 MATERIALIZED id, alias_id UInt64 ALIAS id, "
             "ephemeral_id UInt64 EPHEMERAL) ENGINE = Memory ORDER BY id",
             "ATTACH TABLE app.events UUID '00000000-0000-0003-0000-000000000004' "
             "(id UInt64, label String, materialized_id UInt64 MATERIALIZED id + 1, alias_id UInt64 ALIAS id, "
             "ephemeral_id UInt64 EPHEMERAL) ENGINE = Memory",
         })
    {
        expectValidationError(
            AtomicTableMetadataValidationError::Code::TrustedMetadataMismatch,
            [&] { static_cast<void>(fixture.validator->validateAndCanonicalize(fixture.expectation, metadata, fixture.sidecar)); });
    }
}

TEST(AtomicTableMetadataValidator, RejectsANameOutsideTheTrustedInstallationIdentity)
{
    Fixture fixture;
    String wrong_name = fixture.validMetadata();
    const auto position = wrong_name.find("app.events");
    ASSERT_NE(position, String::npos);
    wrong_name.replace(position, String("app.events").size(), "app.other");
    expectValidationError(
        AtomicTableMetadataValidationError::Code::ObjectMismatch,
        [&] { static_cast<void>(fixture.validator->validateAndCanonicalize(fixture.expectation, wrong_name, fixture.sidecar)); });
}

TEST(AtomicTableMetadataValidator, RejectsAMismatchedTrustedQueryAndCreatedTable)
{
    Fixture fixture;
    expectValidationError(
        AtomicTableMetadataValidationError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(
                AtomicTableMetadataValidator(fixture.database_uuid, fixture.trusted_create_query, StoragePtr{}));
        });

    MemorySettings memory_settings;
    const auto wrong_identity = std::make_shared<StorageMemory>(
        StorageID("app", "other", tableObject(fixture.database_uuid).object_uuid),
        ColumnsDescription(
            NamesAndTypesList{
                {"id", std::make_shared<DataTypeUInt64>()},
                {"label", std::make_shared<DataTypeString>()},
                {"materialized_id", std::make_shared<DataTypeUInt64>()},
            }),
        ConstraintsDescription{},
        String{},
        memory_settings);
    expectValidationError(
        AtomicTableMetadataValidationError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(
                AtomicTableMetadataValidator(fixture.database_uuid, fixture.trusted_create_query, wrong_identity));
        });

    const auto wrong_schema = std::make_shared<StorageMemory>(
        StorageID("app", "events", tableObject(fixture.database_uuid).object_uuid),
        ColumnsDescription(
            NamesAndTypesList{
                {"id", std::make_shared<DataTypeUInt32>()},
                {"label", std::make_shared<DataTypeString>()},
                {"materialized_id", std::make_shared<DataTypeUInt64>()},
            }),
        ConstraintsDescription{},
        String{},
        memory_settings);
    expectValidationError(
        AtomicTableMetadataValidationError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(
                AtomicTableMetadataValidator(fixture.database_uuid, fixture.trusted_create_query, wrong_schema));
        });
}

TEST(AtomicTableMetadataValidator, EnforcesConstructionAndInputLimits)
{
    Fixture fixture;
    expectValidationError(
        AtomicTableMetadataValidationError::Code::InvalidConfiguration,
        [&]
        {
            static_cast<void>(
                AtomicTableMetadataValidator(UUIDHelpers::Nil, fixture.trusted_create_query, fixture.trusted_table));
        });

    AtomicTableMetadataValidatorLimits limits;
    auto trusted_validation = fixture.validator->validateAndCanonicalize(fixture.expectation, fixture.validMetadata(), fixture.sidecar);
    limits.maximum_metadata_bytes = trusted_validation.releaseCanonicalMetadataBytes().size();
    AtomicTableMetadataValidator limited(
        fixture.database_uuid, fixture.trusted_create_query, fixture.trusted_table, limits);
    String oversized_metadata = fixture.validMetadata() + String(limits.maximum_metadata_bytes, ' ');
    expectValidationError(
        AtomicTableMetadataValidationError::Code::LimitExceeded,
        [&] { static_cast<void>(limited.validateAndCanonicalize(fixture.expectation, oversized_metadata, fixture.sidecar)); });
}

TEST(AtomicTableMetadataValidator, StartupBindingPublishesExactRootBoundReferencesAsOneMetadataSnapshot)
{
    Fixture fixture;
    auto initial_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(initial_handle);
    StorageInMemoryMetadata marked_metadata(*initial_handle);
    marked_metadata.setComment("retained startup metadata");
    fixture.trusted_table->setInMemoryMetadata(marked_metadata);

    auto before_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(before_handle);
    StorageMetadataPtr before = before_handle;
    fixture.validator->validateAndBindStartupMetadata(
        *fixture.recovered_root, fixture.expectation, fixture.canonical_metadata, fixture.sidecar);

    auto after_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(after_handle);
    StorageMetadataPtr after = after_handle;
    EXPECT_NE(after, before);
    EXPECT_EQ(after->comment, "retained startup metadata");
    ASSERT_TRUE(after->getBoundUDTReferences());
    ASSERT_TRUE(after->getBoundUDTExpectation());
    EXPECT_EQ(*after->getBoundUDTExpectation(), fixture.expectation);

    const auto & bound = *after->getBoundUDTReferences();
    EXPECT_EQ(bound.getObject(), tableObject(fixture.database_uuid));
    EXPECT_EQ(bound.getObjectSchemaRevision(), fixture.expectation.object_schema_revision);
    EXPECT_EQ(bound.getSidecarHash(), fixture.expectation.sidecar_hash);
    EXPECT_EQ(bound.getPhysicalSchemaFingerprint(), fixture.expectation.physical_schema_fingerprint);
    ASSERT_EQ(bound.getDefinitionHandles().size(), 1);
    EXPECT_EQ(bound.getDefinitionHandles().front(), fixture.recovered_root->findByIdentity(fixture.definition->getIdentity()));
    EXPECT_EQ(
        computeTableColumnPhysicalSchemaFingerprint(after->getColumns().getAllPhysical()),
        fixture.expectation.physical_schema_fingerprint);
}

TEST(AtomicTableMetadataValidator, StartupBindingRejectsStaleOrWrongAuthorityWithoutPublishing)
{
    Fixture fixture;
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.stale_root, fixture.expectation, fixture.canonical_metadata, fixture.sidecar);
        });

    auto wrong_expectation = fixture.expectation;
    auto wrong_installation = decodeDependentObjectMetadataInstallationRecord(fixture.installation_record);
    wrong_installation.object_name = "other";
    const String wrong_installation_record = encodeDependentObjectMetadataInstallationRecord(wrong_installation);
    wrong_expectation.installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(wrong_installation);
    auto wrong_expectation_root = buildRootWithExpectation(
        *fixture.stale_root,
        *fixture.definition,
        wrong_expectation,
        fixture.authority_metadata,
        fixture.sidecar,
        wrong_installation_record);
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *wrong_expectation_root, fixture.expectation, fixture.canonical_metadata, fixture.sidecar);
        });

    const auto wrong_definition = TemplateChecker::checkAll({aliasInput(fixture.database_uuid, "UserId", "UInt32")}).front();
    const auto wrong_definition_base = buildDefinitionRoot(wrong_definition, "UInt32");
    std::vector<TableColumnTypeBindingInput> wrong_columns;
    wrong_columns.push_back({"id", rootAlias(wrong_definition, std::make_shared<DataTypeUInt32>())});
    wrong_columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    wrong_columns.push_back({"materialized_id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    const auto wrong_bindings = prepareTableColumnTypeBindings(tableObject(fixture.database_uuid), 1, wrong_columns);
    const String wrong_sidecar = encodePersistedTypeReferences(*wrong_bindings.persisted_references);
    auto wrong_definition_expectation = *wrong_bindings.sidecar_expectation;
    const DependentObjectMetadataInstallationRecord wrong_definition_installation{
        .object = wrong_definition_expectation.object,
        .object_schema_revision = wrong_definition_expectation.object_schema_revision,
        .object_name = "events",
        .metadata_artifact_hash = computeDatabaseSchemaWALStagedArtifactHash(
            DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, fixture.authority_metadata),
        .semantic_extension_version = wrong_definition_expectation.semantic_extension_version,
        .semantic_extension_flags = wrong_definition_expectation.semantic_extension_flags,
    };
    const String wrong_definition_installation_record = encodeDependentObjectMetadataInstallationRecord(wrong_definition_installation);
    wrong_definition_expectation.installation_record_hash
        = computeDependentObjectMetadataInstallationRecordHash(wrong_definition_installation);
    const auto wrong_definition_root = buildRootWithExpectation(
        *wrong_definition_base,
        *wrong_definition,
        wrong_definition_expectation,
        fixture.authority_metadata,
        wrong_sidecar,
        wrong_definition_installation_record);
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *wrong_definition_root, wrong_definition_expectation, fixture.canonical_metadata, wrong_sidecar);
        });

    const auto foreign_definition = TemplateChecker::checkAll({aliasInput(uuid(9, 9))}).front();
    const auto foreign_root = buildDefinitionRoot(foreign_definition);
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *foreign_root, fixture.expectation, fixture.canonical_metadata, fixture.sidecar);
        });
}

TEST(AtomicTableMetadataValidator, StartupBindingRejectsUUIDRevisionAndSidecarMismatchWithoutPublishing)
{
    Fixture fixture;

    String wrong_uuid_metadata = fixture.canonical_metadata;
    const auto uuid_position = wrong_uuid_metadata.find("00000000-0000-0003-0000-000000000004");
    ASSERT_NE(uuid_position, String::npos);
    wrong_uuid_metadata.replace(uuid_position, 36, "00000000-0000-0003-0000-000000000005");
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.recovered_root, fixture.expectation, wrong_uuid_metadata, fixture.sidecar);
        });

    auto wrong_revision_references = *fixture.bindings.persisted_references;
    wrong_revision_references.object_schema_revision = 2;
    const String wrong_revision_sidecar = encodePersistedTypeReferences(wrong_revision_references);
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.recovered_root, fixture.expectation, fixture.canonical_metadata, wrong_revision_sidecar);
        });

    String broken_sidecar = fixture.sidecar;
    broken_sidecar.push_back('\0');
    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.recovered_root, fixture.expectation, fixture.canonical_metadata, broken_sidecar);
        });

    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.recovered_root, fixture.expectation, fixture.validMetadata(), fixture.sidecar);
        });
}

TEST(AtomicTableMetadataValidator, StartupBindingRejectsChangedPhysicalStorageWithoutPublishing)
{
    Fixture fixture;
    auto current_handle = fixture.trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(current_handle);
    StorageInMemoryMetadata changed_metadata(*current_handle);
    changed_metadata.setColumns(ColumnsDescription(
        NamesAndTypesList{
            {"id", std::make_shared<DataTypeUInt32>()},
            {"label", std::make_shared<DataTypeString>()},
            {"materialized_id", std::make_shared<DataTypeUInt64>()},
        }));
    fixture.trusted_table->setInMemoryMetadata(changed_metadata);

    expectStartupBindingFailureLeavesMetadataUnchanged(
        fixture,
        [&]
        {
            fixture.validator->validateAndBindStartupMetadata(
                *fixture.recovered_root, fixture.expectation, fixture.canonical_metadata, fixture.sidecar);
        });
}

}
