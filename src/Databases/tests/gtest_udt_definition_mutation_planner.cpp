#include <Databases/UDT/DefinitionMutationPlanner.h>

#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <IO/WriteHelpers.h>

#include <Common/quoteString.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <ranges>
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

String lowerHexDigest(const Digest & value)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(value.size() * 2, '\0');
    for (size_t index = 0; index < value.size(); ++index)
    {
        result[2 * index] = digits[value[index] >> 4];
        result[2 * index + 1] = digits[value[index] & 0x0f];
    }
    return result;
}

DefinitionInput
scalarInput(UUID database_uuid, UUID type_uuid, UInt64 revision, String qualified_name, String local_name, String family = "UInt64")
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = revision};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = std::move(family);
    input.nodes.push_back(std::move(root));
    return input;
}

DefinitionInput callInput(
    UUID database_uuid,
    UUID type_uuid,
    UInt64 revision,
    String qualified_name,
    String local_name,
    UUID dependency_uuid,
    UInt64 dependency_revision)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = revision};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::DefinitionCall;
    root.dependency_ordinal = 0;
    input.nodes.push_back(std::move(root));
    input.dependencies.push_back({
        .type_uuid = dependency_uuid,
        .revision = dependency_revision,
        .target_definition_hash = {},
    });
    return input;
}

DefinitionInput recursiveInput(UUID database_uuid, UUID type_uuid, UInt64 revision, String qualified_name, String local_name)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = revision};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    input.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt64},
    };
    input.decreasing_parameter = 1;
    input.checker_abi = 2;

    TemplateNode type_if;
    type_if.kind = TemplateNodeKind::TypeIfZero;
    type_if.parameter = 1;
    type_if.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    TemplateNode type_formal;
    type_formal.kind = TemplateNodeKind::TypeParameter;
    type_formal.parameter = 0;
    TemplateNode array;
    array.kind = TemplateNodeKind::BuiltIn;
    array.atom = "Array";
    array.children = {{.reference = 3, .label = {}}};
    TemplateNode self_call;
    self_call.kind = TemplateNodeKind::SelfCall;
    self_call.parameter = 1;
    self_call.decrement = 1;
    input.nodes = {std::move(type_if), std::move(type_formal), std::move(array), std::move(self_call)};
    return input;
}

Definition::Ptr byLocalName(std::span<const Definition::Ptr> definitions, std::string_view local_name)
{
    const auto found = std::ranges::find_if(
        definitions, [&](const Definition::Ptr & definition) { return definition->getNormalizedLocalName() == local_name; });
    if (found == definitions.end())
        throw std::logic_error("test definition is absent");
    return *found;
}

Record definitionRecord(const Definition & definition, String comment = "stable-comment")
{
    String physical = "checked-template";
    if (!definition.getNodes().empty() && !definition.getNodes().front().atom.empty())
        physical = definition.getNodes().front().atom;
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "ATTACH TYPE " + definition.getNormalizedName() + " AS " + physical,
            .canonical_physical_template_sql = std::move(physical),
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = std::move(comment),
            .creation_time_us_utc = 123456,
        });
}

Record canonicalScalarDefinitionRecord(const Definition & definition, String comment)
{
    if (definition.getNodes().empty() || definition.getNodes().front().atom.empty())
        throw std::logic_error("canonical scalar fixture requires one built-in root");

    const String physical = definition.getNodes().front().atom;
    String canonical_sql = "ATTACH TYPE " + definition.getNormalizedName() + " UUID "
        + quoteString(toString(definition.getIdentity().type_uuid)) + " REVISION " + std::to_string(definition.getIdentity().revision)
        + " AS " + physical + " DEFINITION HASH " + quoteString(lowerHexDigest(definition.getDefinitionHash()));
    if (!comment.empty())
        canonical_sql += " COMMENT " + quoteString(comment);
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = std::move(canonical_sql),
            .canonical_physical_template_sql = physical,
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = std::move(comment),
            .creation_time_us_utc = 123456,
        });
}

Record commentRecord(Record record, String comment)
{
    const auto comment_clause = record.canonical_definition_sql.find(" COMMENT ");
    if (comment_clause != String::npos)
        record.canonical_definition_sql.erase(comment_clause);
    if (!comment.empty())
        record.canonical_definition_sql += " COMMENT " + quoteString(comment);
    record.comment = std::move(comment);
    return record;
}

Record presentationRewrite(Record record, std::string_view suffix)
{
    record.canonical_definition_sql += suffix;
    record.canonical_physical_template_sql += suffix;
    return record;
}

SidecarExpectationRecord expectation(UUID database_uuid, UUID object_uuid, const Definition::Ptr & definition)
{
    const auto references = Test::singleDefinitionPersistedTypeReferences(
        {
            .kind = SchemaObjectKind::Table,
            .database_uuid = database_uuid,
            .object_uuid = object_uuid,
        },
        3,
        digest(50),
        definition,
        std::make_shared<DataTypeUInt64>(),
        PersistedTypePathSection::ColumnType);
    return Test::sidecarExpectationFor(references);
}

EffectiveResourceLimits defaultDatabaseResourceLimits()
{
    const std::array layers{
        makeServerDefaultResourceLimitLayer(64ULL << 30),
        makeDatabaseDefaultResourceLimitLayer(),
    };
    return calculateEffectiveResourceLimits(layers);
}

SchemaObjectID definitionObject(const Definition & definition)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = definition.getIdentity().database_uuid,
        .object_uuid = definition.getIdentity().type_uuid,
    };
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

AuthorityRoot::Ptr buildRoot(
    UUID database_uuid,
    UInt64 epoch,
    UInt64 generation,
    std::vector<Definition::Ptr> definitions,
    std::vector<Record> records,
    std::vector<SidecarExpectationRecord> expectations = {},
    std::optional<UUID> expectation_target = std::nullopt)
{
    std::vector<SchemaObjectID> nodes;
    std::vector<SchemaObjectDependencyEdge> edges;
    nodes.reserve(definitions.size() + expectations.size());
    for (const auto & definition : definitions)
    {
        const auto dependent = definitionObject(*definition);
        nodes.push_back(dependent);
        for (const auto & dependency : definition->getDependencies())
        {
            edges.push_back({
                .dependent = dependent,
                .dependency = {
                    .kind = SchemaObjectKind::TypeDefinition,
                    .database_uuid = database_uuid,
                    .object_uuid = dependency.type_uuid,
                },
                .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition,
            });
        }
    }
    if (!expectations.empty())
    {
        if (!expectation_target)
            throw std::logic_error("test expectation target is absent");
        for (const auto & record : expectations)
        {
            nodes.push_back(record.object);
            edges.push_back({
                .dependent = record.object,
                .dependency = {
                    .kind = SchemaObjectKind::TypeDefinition,
                    .database_uuid = database_uuid,
                    .object_uuid = *expectation_target,
                },
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            });
        }
    }
    std::vector<PersistedTypeReferences> dependent_references;
    if (!expectations.empty())
    {
        const auto target = std::ranges::find_if(
            definitions, [&](const auto & definition) { return definition->getIdentity().type_uuid == *expectation_target; });
        if (target == definitions.end())
            throw std::logic_error("test expectation target definition is absent");
        dependent_references.reserve(expectations.size());
        for (const auto & record : expectations)
        {
            auto references = Test::singleDefinitionPersistedTypeReferences(
                record.object,
                record.object_schema_revision,
                record.physical_schema_fingerprint,
                *target,
                std::make_shared<DataTypeUInt64>(),
                PersistedTypePathSection::ColumnType,
                record.semantic_extension_version,
                record.semantic_extension_flags);
            if (Test::sidecarExpectationFor(references) != record)
                throw std::logic_error("test expectation differs from its exact sidecar image");
            dependent_references.push_back(std::move(references));
        }
    }
    auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    const auto summary = inventorySummary(records, expectations);
    const UInt64 capability_mask = expectations.empty() ? definition_authority_capability_mask : dependent_object_authority_capability_mask;
    auto state
        = makeAuthorityState(database_uuid, epoch, capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    const Test::DependentObjectResourceImageBatch dependent_objects(
        expectations, Test::dependentObjectResourceImageInputs(dependent_references));
    return AuthorityRootBuilder::build(
        state, generation, definitions, records, expectations, std::move(graph), AuthorityRootBuildLimits{}, dependent_objects.get());
}

DefinitionMutationRequest request(
    DefinitionMutationKind kind,
    UUID database_uuid,
    UInt64 transaction_id,
    UInt64 expected_epoch,
    std::optional<DefinitionIdentity> before,
    Definition::Ptr after_definition = {},
    std::optional<Record> after_record = std::nullopt,
    bool if_not_exists = false,
    bool require_exact_type_uuid_on_noop = false)
{
    return {
        .kind = kind,
        .database_uuid = database_uuid,
        .transaction_id = transaction_id,
        .expected_database_catalog_epoch = expected_epoch,
        .expected_before_identity = before,
        .after_definition = std::move(after_definition),
        .after_record = std::move(after_record),
        .if_not_exists = if_not_exists,
        .require_exact_type_uuid_on_noop = require_exact_type_uuid_on_noop,
        .rename_dependent_record_rewrites = {},
    };
}

template <typename Callback>
void expectPlannerError(DefinitionMutationPlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected DefinitionMutationPlannerError";
    }
    catch (const DefinitionMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(DefinitionMutationPlanner, PreparesFirstCreateFromTheNeverEnabledState)
{
    const UUID database_uuid = uuid(1, 1);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(2, 1), 1, "db.Alpha", "Alpha")}).front();
    const auto record = definitionRecord(*definition);

    DefinitionMutationPlannerLimits limits;
    limits.initial_effective_database_limits = defaultDatabaseResourceLimits();
    auto prepared = DefinitionMutationPlanner::plan(
        nullptr, request(DefinitionMutationKind::Create, database_uuid, 1, 0, std::nullopt, definition, record), limits);

    const auto & root = prepared.getReplacementRoot();
    EXPECT_EQ(root.getDatabaseCatalogEpoch(), 1);
    EXPECT_EQ(root.getPersistentCapabilityMask(), definition_authority_capability_mask);
    EXPECT_EQ(root.getTypeIndexGeneration(), 1);
    EXPECT_EQ(root.findByIdentity(definition->getIdentity()), definition);
    EXPECT_TRUE(root.getExpectationRecords().empty());
    EXPECT_TRUE(root.getSchemaObjectDependencyGraph().containsNode(definitionObject(*definition)));

    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    EXPECT_FALSE(prepare.before_authority_state);
    ASSERT_EQ(prepare.authority_record_deltas.size(), 1);
    EXPECT_FALSE(prepare.authority_record_deltas.front().before);
    ASSERT_TRUE(prepare.authority_record_deltas.front().after);
    EXPECT_EQ(prepare.authority_record_deltas.front().after->object_revision, 1);
    ASSERT_EQ(prepare.staged_artifacts.size(), 1);
    EXPECT_EQ(prepare.staged_artifacts.front().image, DatabaseSchemaWALStagedArtifactImage::After);

    auto released = prepared.releaseReplacementRoot();
    EXPECT_TRUE(released);
    EXPECT_FALSE(prepared.hasReplacementRoot());
    EXPECT_EQ(prepared.getValidatedTransition().getPrepare().transaction_id, 1);
}

TEST(DefinitionMutationPlanner, RejectsStableUUIDNameCollisionsAndANonInitialCreateRevision)
{
    const UUID database_uuid = uuid(3, 1);
    const UUID existing_uuid = uuid(3, 2);
    const auto existing = TemplateChecker::checkAll({scalarInput(database_uuid, existing_uuid, 1, "db.Alpha", "Alpha")}).front();
    const auto existing_record = definitionRecord(*existing);
    const auto root = buildRoot(database_uuid, 4, 7, {existing}, {existing_record});

    const auto duplicate_name = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(3, 3), 1, "db.Alpha", "Alpha")}).front();
    expectPlannerError(
        DefinitionMutationPlannerError::Code::DuplicateName,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::Create, database_uuid, 5, 4, std::nullopt, duplicate_name, definitionRecord(*duplicate_name))));
        });

    const auto duplicate_uuid = TemplateChecker::checkAll({scalarInput(database_uuid, existing_uuid, 1, "db.Beta", "Beta")}).front();
    expectPlannerError(
        DefinitionMutationPlannerError::Code::DuplicateTypeUUID,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::Create, database_uuid, 6, 4, std::nullopt, duplicate_uuid, definitionRecord(*duplicate_uuid))));
        });

    const auto non_initial_revision = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(3, 4), 2, "db.Beta", "Beta")}).front();
    expectPlannerError(
        DefinitionMutationPlannerError::Code::InvalidRevision,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::Create,
                    database_uuid,
                    7,
                    4,
                    std::nullopt,
                    non_initial_revision,
                    definitionRecord(*non_initial_revision))));
        });
}

TEST(DefinitionMutationPlanner, CreateIfNotExistsNoOpIgnoresNewIdentityAndAdministrativePresentation)
{
    const UUID database_uuid = uuid(30, 1);
    const auto existing = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(30, 2), 2, "db.Alpha", "Alpha")}).front();
    const auto root = buildRoot(database_uuid, 40, 70, {existing}, {definitionRecord(*existing, "existing-comment")});
    const auto requested = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(30, 3), 1, "db.Alpha", "Alpha")}).front();
    auto requested_record = definitionRecord(*requested, "new-comment");
    requested_record.owner_uuid = uuid(30, 4);
    requested_record.owner_display_name = "different-owner";
    requested_record.creation_time_us_utc = 987654;
    requested_record.canonical_definition_sql = "CREATE TYPE db.Alpha AS UInt64";
    requested_record.canonical_physical_template_sql = "UInt64 /* presentation */";

    auto prepared = DefinitionMutationPlanner::plan(
        root.get(), request(DefinitionMutationKind::Create, database_uuid, 0, 40, std::nullopt, requested, requested_record, true));

    EXPECT_TRUE(prepared.isNoOp());
    EXPECT_FALSE(prepared.hasReplacementRoot());
    EXPECT_FALSE(prepared.hasValidatedTransition());
    EXPECT_FALSE(prepared.releaseReplacementRoot());
    EXPECT_EQ(root->getDatabaseCatalogEpoch(), 40);
    EXPECT_EQ(root->getTypeIndexGeneration(), 70);
    expectPlannerError(DefinitionMutationPlannerError::Code::InvalidRequest, [&] { static_cast<void>(prepared.getValidatedTransition()); });
}

TEST(DefinitionMutationPlanner, CreateIfNotExistsRejectsSemanticAndABIMismatches)
{
    const UUID database_uuid = uuid(31, 1);
    const auto existing = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(31, 2), 1, "db.Alpha", "Alpha")}).front();
    const auto root = buildRoot(database_uuid, 41, 71, {existing}, {definitionRecord(*existing)});
    const auto semantic_mismatch
        = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(31, 3), 1, "db.Alpha", "Alpha", "UInt32")}).front();
    const auto abi_mismatch = TemplateChecker::checkAll({recursiveInput(database_uuid, uuid(31, 4), 1, "db.Alpha", "Alpha")}).front();

    for (const auto & mismatch : {semantic_mismatch, abi_mismatch})
    {
        expectPlannerError(
            DefinitionMutationPlannerError::Code::DefinitionConflict,
            [&]
            {
                static_cast<void>(DefinitionMutationPlanner::plan(
                    root.get(),
                    request(
                        DefinitionMutationKind::Create, database_uuid, 42, 41, std::nullopt, mismatch, definitionRecord(*mismatch), true)));
            });
    }
}

TEST(DefinitionMutationPlanner, CreateIfNotExistsRejectsAnExactDependencyMismatch)
{
    const UUID database_uuid = uuid(32, 1);
    const UUID first_leaf_uuid = uuid(32, 2);
    const UUID second_leaf_uuid = uuid(32, 3);
    const auto existing_definitions = TemplateChecker::checkAll({
        callInput(database_uuid, uuid(32, 4), 1, "db.Caller", "Caller", first_leaf_uuid, 1),
        scalarInput(database_uuid, first_leaf_uuid, 1, "db.First", "First"),
        scalarInput(database_uuid, second_leaf_uuid, 1, "db.Second", "Second"),
    });
    std::vector<Record> existing_records;
    for (const auto & definition : existing_definitions)
        existing_records.push_back(definitionRecord(*definition));
    const auto root = buildRoot(database_uuid, 42, 72, existing_definitions, existing_records);
    const auto requested_definitions = TemplateChecker::checkAll({
        callInput(database_uuid, uuid(32, 5), 1, "db.Caller", "Caller", second_leaf_uuid, 1),
        scalarInput(database_uuid, first_leaf_uuid, 1, "db.First", "First"),
        scalarInput(database_uuid, second_leaf_uuid, 1, "db.Second", "Second"),
    });
    const auto requested = byLocalName(requested_definitions, "Caller");

    expectPlannerError(
        DefinitionMutationPlannerError::Code::DefinitionConflict,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::Create, database_uuid, 43, 42, std::nullopt, requested, definitionRecord(*requested), true)));
        });
}

TEST(DefinitionMutationPlanner, InternalCreateIfNotExistsBindsTheExactStableUUID)
{
    const UUID database_uuid = uuid(33, 1);
    const UUID existing_uuid = uuid(33, 2);
    const auto existing = TemplateChecker::checkAll({scalarInput(database_uuid, existing_uuid, 1, "db.Alpha", "Alpha")}).front();
    const auto root = buildRoot(database_uuid, 43, 73, {existing}, {definitionRecord(*existing)});
    const auto exact_replay = TemplateChecker::checkAll({scalarInput(database_uuid, existing_uuid, 1, "db.Alpha", "Alpha")}).front();

    auto prepared = DefinitionMutationPlanner::plan(
        root.get(),
        request(
            DefinitionMutationKind::Create, database_uuid, 0, 43, std::nullopt, exact_replay, definitionRecord(*exact_replay), true, true));
    EXPECT_TRUE(prepared.isNoOp());

    const auto wrong_identity = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(33, 3), 1, "db.Alpha", "Alpha")}).front();
    expectPlannerError(
        DefinitionMutationPlannerError::Code::DefinitionConflict,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::Create,
                    database_uuid,
                    0,
                    43,
                    std::nullopt,
                    wrong_identity,
                    definitionRecord(*wrong_identity),
                    true,
                    true)));
        });
}

TEST(DefinitionMutationPlanner, DefaultLimitsMatchAtomicPublicationProfile)
{
    const DefinitionMutationPlannerLimits limits;
    const auto atomic_limits = atomicDatabaseAuthorityCapabilities().limits;
    EXPECT_EQ(limits.authority_root.type_catalog.maximum_definitions, atomic_limits.maximum_definitions);
    EXPECT_EQ(limits.authority_root.maximum_definition_records, atomic_limits.maximum_definitions);
    EXPECT_EQ(limits.maximum_definition_retained_bytes, atomic_limits.maximum_definition_bytes);
}

TEST(DefinitionMutationPlanner, ReplacesSemanticRevisionAndUpdatesOnlyOutgoingEdges)
{
    const UUID database_uuid = uuid(4, 1);
    const UUID first_leaf_uuid = uuid(4, 2);
    const UUID second_leaf_uuid = uuid(4, 3);
    const UUID mutable_uuid = uuid(4, 4);
    const auto before_definitions = TemplateChecker::checkAll({
        callInput(database_uuid, mutable_uuid, 1, "db.Mutable", "Mutable", first_leaf_uuid, 1),
        scalarInput(database_uuid, first_leaf_uuid, 1, "db.First", "First"),
        scalarInput(database_uuid, second_leaf_uuid, 1, "db.Second", "Second"),
    });
    std::vector<Record> before_records;
    for (const auto & definition : before_definitions)
        before_records.push_back(definitionRecord(*definition));
    const auto root = buildRoot(database_uuid, 8, 11, before_definitions, before_records);
    const auto before_mutable = byLocalName(before_definitions, "Mutable");
    const auto after_definitions = TemplateChecker::checkAll({
        callInput(database_uuid, mutable_uuid, 2, "db.Mutable", "Mutable", second_leaf_uuid, 1),
        scalarInput(database_uuid, first_leaf_uuid, 1, "db.First", "First"),
        scalarInput(database_uuid, second_leaf_uuid, 1, "db.Second", "Second"),
    });
    const auto after_mutable = byLocalName(after_definitions, "Mutable");

    auto prepared = DefinitionMutationPlanner::plan(
        root.get(),
        request(
            DefinitionMutationKind::ReplaceSemantic,
            database_uuid,
            9,
            8,
            before_mutable->getIdentity(),
            after_mutable,
            definitionRecord(*after_mutable)));

    EXPECT_EQ(prepared.getReplacementRoot().getTypeIndexGeneration(), 12);
    EXPECT_EQ(prepared.getReplacementRoot().findByName("Mutable")->getIdentity().revision, 2);
    const auto & delta = prepared.getValidatedTransition().getPrepare().graph_delta;
    ASSERT_EQ(delta.edge_removals.size(), 1);
    ASSERT_EQ(delta.edge_additions.size(), 1);
    EXPECT_EQ(delta.edge_removals.front().dependency.object_uuid, first_leaf_uuid);
    EXPECT_EQ(delta.edge_additions.front().dependency.object_uuid, second_leaf_uuid);
    EXPECT_TRUE(prepared.getValidatedTransition().getAfterGraph().containsNode(definitionObject(*after_mutable)));
}

TEST(DefinitionMutationPlanner, RenamePreservesIdentityRevisionDependenciesAndCheckedBody)
{
    const UUID database_uuid = uuid(5, 1);
    const UUID leaf_uuid = uuid(5, 2);
    const UUID caller_uuid = uuid(5, 3);
    const auto definitions = TemplateChecker::checkAll({
        callInput(database_uuid, caller_uuid, 1, "db.Caller", "Caller", leaf_uuid, 1),
        scalarInput(database_uuid, leaf_uuid, 1, "db.Leaf", "Leaf"),
    });
    std::vector<Record> records;
    for (const auto & definition : definitions)
        records.push_back(definitionRecord(*definition));
    const auto root = buildRoot(database_uuid, 10, 15, definitions, records);
    const auto before_leaf = byLocalName(definitions, "Leaf");
    const auto before_caller = byLocalName(definitions, "Caller");
    const auto renamed_leaf = TemplateChecker::checkAll({scalarInput(database_uuid, leaf_uuid, 1, "db.Renamed", "Renamed")}).front();

    auto rename_request = request(
        DefinitionMutationKind::Rename, database_uuid, 11, 10, before_leaf->getIdentity(), renamed_leaf, definitionRecord(*renamed_leaf));
    rename_request.rename_dependent_record_rewrites.push_back(presentationRewrite(
        *std::ranges::find_if(records, [&](const auto & record) { return record.identity == before_caller->getIdentity(); }), " renamed"));
    auto prepared = DefinitionMutationPlanner::plan(root.get(), std::move(rename_request));

    EXPECT_FALSE(prepared.getReplacementRoot().findByName("Leaf"));
    ASSERT_TRUE(prepared.getReplacementRoot().findByName("Renamed"));
    EXPECT_EQ(prepared.getReplacementRoot().findByName("Renamed")->getIdentity(), before_leaf->getIdentity());
    EXPECT_EQ(prepared.getReplacementRoot().getTypeIndexGeneration(), 16);
    const auto rewritten_caller = std::ranges::find_if(
        prepared.getReplacementRoot().getDefinitionRecords(),
        [&](const auto & record) { return record.identity == before_caller->getIdentity(); });
    ASSERT_NE(rewritten_caller, prepared.getReplacementRoot().getDefinitionRecords().end());
    const auto original_caller
        = std::ranges::find_if(records, [&](const auto & record) { return record.identity == before_caller->getIdentity(); });
    ASSERT_NE(original_caller, records.end());
    EXPECT_NE(rewritten_caller->canonical_definition_sql, original_caller->canonical_definition_sql);
    EXPECT_NE(rewritten_caller->canonical_physical_template_sql, original_caller->canonical_physical_template_sql);
    EXPECT_EQ(rewritten_caller->identity, original_caller->identity);
    EXPECT_EQ(rewritten_caller->definition_hash, original_caller->definition_hash);
    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    EXPECT_EQ(prepare.authority_record_deltas.size(), 2);
    EXPECT_EQ(prepare.staged_artifacts.size(), 4);
    EXPECT_TRUE(prepare.graph_delta.node_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_removals.empty());
    EXPECT_EQ(
        prepared.getReplacementRoot().getSchemaObjectDependencyGraph().computeRoot(), root->getSchemaObjectDependencyGraph().computeRoot());
}

TEST(DefinitionMutationPlanner, CommentChangesOnlyDurableAdministrationAndRetainsGeneration)
{
    const UUID database_uuid = uuid(6, 1);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(6, 2), 1, "db.Alpha", "Alpha")}).front();
    const auto before_record = canonicalScalarDefinitionRecord(*definition, "before");
    const auto root = buildRoot(database_uuid, 12, 19, {definition}, {before_record});
    auto after_record = commentRecord(before_record, "after");

    auto prepared = DefinitionMutationPlanner::plan(
        root.get(), request(DefinitionMutationKind::Comment, database_uuid, 13, 12, definition->getIdentity(), definition, after_record));

    EXPECT_EQ(prepared.getReplacementRoot().getTypeIndexGeneration(), root->getTypeIndexGeneration());
    EXPECT_EQ(prepared.getReplacementRoot().getTypeIndexContentDigest(), root->getTypeIndexContentDigest());
    ASSERT_EQ(prepared.getReplacementRoot().getDefinitionRecords().size(), 1);
    const auto & stored_record = prepared.getReplacementRoot().getDefinitionRecords().front();
    EXPECT_EQ(stored_record.comment, "after");
    EXPECT_EQ(stored_record.owner_uuid, before_record.owner_uuid);
    EXPECT_EQ(stored_record.owner_display_name, before_record.owner_display_name);
    EXPECT_EQ(stored_record.creation_time_us_utc, before_record.creation_time_us_utc);
    EXPECT_EQ(prepared.getReplacementRoot().getDatabaseCatalogEpoch(), 13);

    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    ASSERT_EQ(prepare.authority_record_deltas.size(), 1);
    ASSERT_TRUE(prepare.authority_record_deltas.front().before);
    ASSERT_TRUE(prepare.authority_record_deltas.front().after);
    EXPECT_EQ(
        prepare.authority_record_deltas.front().before->object_revision, prepare.authority_record_deltas.front().after->object_revision);
    EXPECT_NE(
        prepare.authority_record_deltas.front().before->canonical_record_hash,
        prepare.authority_record_deltas.front().after->canonical_record_hash);
    ASSERT_EQ(prepare.staged_artifacts.size(), 2);
    UInt64 before_artifacts = 0;
    UInt64 after_artifacts = 0;
    for (const auto & artifact : prepare.staged_artifacts)
    {
        EXPECT_EQ(artifact.kind, DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord);
        before_artifacts += artifact.image == DatabaseSchemaWALStagedArtifactImage::Before;
        after_artifacts += artifact.image == DatabaseSchemaWALStagedArtifactImage::After;
    }
    EXPECT_EQ(before_artifacts, 1);
    EXPECT_EQ(after_artifacts, 1);
    EXPECT_TRUE(prepare.graph_delta.node_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_removals.empty());
}

TEST(DefinitionMutationPlanner, CommentRejectsCanonicalAttachWhoseLiteralDisagreesWithTheRecord)
{
    const UUID database_uuid = uuid(6, 11);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(6, 12), 1, "db.Alpha", "Alpha")}).front();
    const auto before_record = canonicalScalarDefinitionRecord(*definition, "before");
    const auto root = buildRoot(database_uuid, 12, 19, {definition}, {before_record});
    auto after_record = commentRecord(before_record, "after");
    after_record.canonical_definition_sql = commentRecord(before_record, "different").canonical_definition_sql;

    expectPlannerError(
        DefinitionMutationPlannerError::Code::DefinitionRecordMismatch,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(DefinitionMutationKind::Comment, database_uuid, 13, 12, definition->getIdentity(), definition, after_record)));
        });
}

TEST(DefinitionMutationPlanner, DropsOnlyAnUnreferencedDefinition)
{
    const UUID database_uuid = uuid(7, 1);
    const auto definitions = TemplateChecker::checkAll({
        scalarInput(database_uuid, uuid(7, 2), 1, "db.Alpha", "Alpha"),
        scalarInput(database_uuid, uuid(7, 3), 1, "db.Beta", "Beta"),
    });
    std::vector<Record> records;
    for (const auto & definition : definitions)
        records.push_back(definitionRecord(*definition));
    const auto root = buildRoot(database_uuid, 14, 22, definitions, records);
    const auto alpha = byLocalName(definitions, "Alpha");

    auto prepared
        = DefinitionMutationPlanner::plan(root.get(), request(DefinitionMutationKind::Drop, database_uuid, 15, 14, alpha->getIdentity()));

    EXPECT_FALSE(prepared.getReplacementRoot().findByName("Alpha"));
    EXPECT_TRUE(prepared.getReplacementRoot().findByName("Beta"));
    EXPECT_EQ(prepared.getReplacementRoot().getTypeIndexGeneration(), 23);
    EXPECT_FALSE(prepared.getReplacementRoot().getSchemaObjectDependencyGraph().containsNode(definitionObject(*alpha)));
    ASSERT_EQ(prepared.getValidatedTransition().getPrepare().staged_artifacts.size(), 1);
    EXPECT_EQ(prepared.getValidatedTransition().getPrepare().staged_artifacts.front().image, DatabaseSchemaWALStagedArtifactImage::Before);
}

TEST(DefinitionMutationPlanner, SelfRecursionDoesNotRestrictRenameOrDrop)
{
    const UUID database_uuid = uuid(71, 1);
    const UUID type_uuid = uuid(71, 2);
    const auto before = TemplateChecker::checkAll({recursiveInput(database_uuid, type_uuid, 1, "db.Tree", "Tree")}).front();
    const auto before_record = definitionRecord(*before);
    const auto root = buildRoot(database_uuid, 1, 1, {before}, {before_record});
    const auto after = TemplateChecker::checkAll({recursiveInput(database_uuid, type_uuid, 1, "db.Node", "Node")}).front();
    auto after_record = definitionRecord(*after);
    after_record.canonical_physical_template_sql += " renamed-self";

    auto renamed = DefinitionMutationPlanner::plan(
        root.get(), request(DefinitionMutationKind::Rename, database_uuid, 2, 1, before->getIdentity(), after, after_record));
    ASSERT_TRUE(renamed.getReplacementRoot().findByName("Node"));

    auto dropped = DefinitionMutationPlanner::plan(
        &renamed.getReplacementRoot(), request(DefinitionMutationKind::Drop, database_uuid, 3, 2, after->getIdentity()));
    EXPECT_TRUE(dropped.getReplacementRoot().getDefinitionRecords().empty());
}

TEST(DefinitionMutationPlanner, RejectsDropAndSemanticReplaceWhenGraphHasDependents)
{
    const UUID database_uuid = uuid(8, 1);
    const UUID leaf_uuid = uuid(8, 2);
    const auto definitions = TemplateChecker::checkAll({
        callInput(database_uuid, uuid(8, 3), 1, "db.Caller", "Caller", leaf_uuid, 1),
        scalarInput(database_uuid, leaf_uuid, 1, "db.Leaf", "Leaf"),
    });
    std::vector<Record> records;
    for (const auto & definition : definitions)
        records.push_back(definitionRecord(*definition));
    const auto root = buildRoot(database_uuid, 16, 25, definitions, records);
    const auto leaf = byLocalName(definitions, "Leaf");

    expectPlannerError(
        DefinitionMutationPlannerError::Code::ReferencedDefinition,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(), request(DefinitionMutationKind::Drop, database_uuid, 17, 16, leaf->getIdentity())));
        });

    const auto replacement = TemplateChecker::checkAll({scalarInput(database_uuid, leaf_uuid, 2, "db.Leaf", "Leaf", "UInt32")}).front();
    expectPlannerError(
        DefinitionMutationPlannerError::Code::ReferencedDefinition,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(
                    DefinitionMutationKind::ReplaceSemantic,
                    database_uuid,
                    18,
                    16,
                    leaf->getIdentity(),
                    replacement,
                    definitionRecord(*replacement))));
        });
}

TEST(DefinitionMutationPlanner, PreservesDependentObjectExpectationRecordsAndNonTypeGraph)
{
    const UUID database_uuid = uuid(9, 1);
    const UUID type_uuid = uuid(9, 2);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, type_uuid, 1, "db.Alpha", "Alpha")}).front();
    const auto before_record = canonicalScalarDefinitionRecord(*definition, "before");
    const auto expected = expectation(database_uuid, uuid(9, 3), definition);
    const auto root = buildRoot(database_uuid, 18, 29, {definition}, {before_record}, {expected}, type_uuid);
    auto after_record = commentRecord(before_record, "after");

    auto prepared = DefinitionMutationPlanner::plan(
        root.get(), request(DefinitionMutationKind::Comment, database_uuid, 19, 18, definition->getIdentity(), definition, after_record));

    EXPECT_EQ(prepared.getReplacementRoot().getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    ASSERT_EQ(prepared.getReplacementRoot().getExpectationRecords().size(), 1);
    EXPECT_EQ(prepared.getReplacementRoot().getExpectationRecords().front(), expected);
    const SchemaObjectDependencyEdge object_edge{
        .dependent = expected.object,
        .dependency = definitionObject(*definition),
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    EXPECT_TRUE(prepared.getReplacementRoot().getSchemaObjectDependencyGraph().containsEdge(object_edge));
}

TEST(DefinitionMutationPlanner, RejectsAStaleExpectedEpoch)
{
    const UUID database_uuid = uuid(10, 1);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(10, 2), 1, "db.Alpha", "Alpha")}).front();
    const auto before_record = canonicalScalarDefinitionRecord(*definition, "before");
    const auto root = buildRoot(database_uuid, 20, 31, {definition}, {before_record});
    auto after_record = commentRecord(before_record, "after");

    expectPlannerError(
        DefinitionMutationPlannerError::Code::ExpectedEpochMismatch,
        [&]
        {
            static_cast<void>(DefinitionMutationPlanner::plan(
                root.get(),
                request(DefinitionMutationKind::Comment, database_uuid, 21, 19, definition->getIdentity(), definition, after_record)));
        });
}

TEST(DefinitionMutationPlanner, ProducesDeterministicRootAndWALBytes)
{
    const UUID database_uuid = uuid(11, 1);
    const auto definition = TemplateChecker::checkAll({scalarInput(database_uuid, uuid(11, 2), 1, "db.Alpha", "Alpha")}).front();
    const auto before_record = canonicalScalarDefinitionRecord(*definition, "before");
    const auto root = buildRoot(database_uuid, 22, 35, {definition}, {before_record});
    auto after_record = commentRecord(before_record, "after");
    const auto mutation_request
        = request(DefinitionMutationKind::Comment, database_uuid, 23, 22, definition->getIdentity(), definition, after_record);

    auto first = DefinitionMutationPlanner::plan(root.get(), mutation_request);
    auto second = DefinitionMutationPlanner::plan(root.get(), mutation_request);

    EXPECT_EQ(first.getValidatedTransition().getPrepare(), second.getValidatedTransition().getPrepare());
    EXPECT_TRUE(
        std::ranges::equal(
            first.getValidatedTransition().getStagedArtifactBytes(), second.getValidatedTransition().getStagedArtifactBytes()));
    EXPECT_EQ(first.getReplacementRoot().getAuthorityState(), second.getReplacementRoot().getAuthorityState());
    EXPECT_EQ(first.getReplacementRoot().getTypeIndexContentDigest(), second.getReplacementRoot().getTypeIndexContentDigest());
    EXPECT_TRUE(std::ranges::equal(first.getReplacementRoot().getDefinitionRecords(), second.getReplacementRoot().getDefinitionRecords()));
}

}
}
