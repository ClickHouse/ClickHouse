#include <Databases/UDT/DependentObjectActivationPlanner.h>

#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/UDT/TemplateChecker.h>

#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <algorithm>
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

String lowerHexDigest(const Digest & value)
{
    static constexpr std::string_view digits = "0123456789abcdef";
    String result;
    result.reserve(value.size() * 2);
    for (const CanonicalByte byte : value)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

DefinitionInput definitionInput(UUID database_uuid, UUID type_uuid)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = "db.Value";
    input.normalized_local_name = "Value";
    TemplateNode node;
    node.kind = TemplateNodeKind::BuiltIn;
    node.atom = "UInt64";
    input.nodes.push_back(std::move(node));
    return input;
}

AuthorityRoot::Ptr definition_onlyRoot(UUID database_uuid)
{
    auto definitions = TemplateChecker::checkAll({definitionInput(database_uuid, uuid(3, 4))});
    const String definition_hash = lowerHexDigest(definitions.front()->getDefinitionHash());
    std::vector<Record> records{
        makeRecord(
            *definitions.front(),
            {
                .canonical_definition_sql
                = "ATTACH TYPE db.Value UUID '00000000-0000-0003-0000-000000000004' REVISION 1 AS UInt64 DEFINITION HASH '"
                    + definition_hash + "'",
                .canonical_physical_template_sql = "UInt64",
                .owner_uuid = uuid(5, 6),
                .owner_display_name = "owner",
                .comment = {},
                .creation_time_us_utc = 7,
            }),
    };
    std::vector<AuthorityInventoryLeaf> leaves{
        {
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = records.front().identity.type_uuid,
            },
            .object_revision = records.front().identity.revision,
            .canonical_record_hash = computeRecordHash(records.front()),
        },
    };
    const auto summary = buildAuthorityInventorySummary(leaves);
    auto graph = SchemaObjectDependencyGraph::build(
        database_uuid,
        std::vector{SchemaObjectID{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = records.front().identity.type_uuid,
        }},
        {});
    const auto state = makeAuthorityState(
        database_uuid, 1, definition_authority_capability_mask, summary.leaf_count, summary.merkle_radix_root, graph->computeRoot());
    return AuthorityRootBuilder::build(state, 1, definitions, records, {}, graph);
}

template <typename Callback>
void expectActivationError(DependentObjectActivationPlannerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected DependentObjectActivationPlannerError";
    }
    catch (const DependentObjectActivationPlannerError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(DependentObjectActivationPlanner, ExactContentNeutralTransitionPublishesOnce)
{
    const UUID database_uuid = uuid(1, 2);
    auto root = definition_onlyRoot(database_uuid);
    const auto original_state = root->getAuthorityState();
    const UInt64 original_generation = root->getTypeIndexGeneration();
    const Digest original_digest = root->getTypeIndexContentDigest();
    const auto original_inventory = root->pinAuthorityInventory();
    const auto original_graph = root->pinSchemaObjectDependencyGraph();

    const auto records = root->getDefinitionRecords();
    ASSERT_EQ(records.size(), 1);
    ParserCreateTypeQuery parser;
    const ASTPtr attach_ast = parseQuery(parser, records.front().canonical_definition_sql, "dependent-object authority activation test record", 0, 200, 0);
    const auto * attach = attach_ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(attach, nullptr);
    ASSERT_TRUE(attach->attach);
    ASSERT_TRUE(attach->definition_hash.has_value());
    EXPECT_EQ(*attach->definition_hash, lowerHexDigest(records.front().definition_hash));
    EXPECT_EQ(attach_ast->formatWithSecretsOneLine(), records.front().canonical_definition_sql);

    DatabaseSchemaWALDependentObjectActivationStatistics activation_statistics{
        .inventory_leaves_visited = 1,
        .graph_nodes_visited = 1,
        .graph_edges_visited = 1,
    };
    auto specialized_transition = DatabaseSchemaWALTransitionBuilder::buildDependentObjectActivation(11, *root, {}, &activation_statistics);
    EXPECT_EQ(activation_statistics.inventory_leaves_visited, 0);
    EXPECT_EQ(activation_statistics.graph_nodes_visited, 0);
    EXPECT_EQ(activation_statistics.graph_edges_visited, 0);
    EXPECT_EQ(specialized_transition.pinAfterInventory().get(), original_inventory.get());
    EXPECT_EQ(specialized_transition.pinAfterGraph().get(), original_graph.get());

    auto generic_transition = DatabaseSchemaWALTransitionBuilder::build(
        11,
        {
            .authority_state = original_state,
            .authority_inventory = original_inventory,
            .schema_graph = original_graph,
        },
        activateDependentObjectAuthority(original_state),
        {},
        {},
        {},
        {});
    EXPECT_EQ(specialized_transition.getPrepare(), generic_transition.getPrepare());
    const String canonical_prepare_bytes = encodeDatabaseSchemaWALPrepare(specialized_transition.getPrepare());
    EXPECT_EQ(canonical_prepare_bytes, encodeDatabaseSchemaWALPrepare(generic_transition.getPrepare()));
    auto decoded_transition = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(canonical_prepare_bytes),
        {
            .authority_state = original_state,
            .authority_inventory = original_inventory,
            .schema_graph = original_graph,
        },
        {});
    EXPECT_EQ(decoded_transition.getPrepare(), specialized_transition.getPrepare());
    const auto commit = makeDatabaseSchemaWALCommit(specialized_transition);
    const auto decoded_commit = decodeDatabaseSchemaWALCommit(encodeDatabaseSchemaWALCommit(commit));
    validateDatabaseSchemaWALCommit(decoded_transition, decoded_commit);
    EXPECT_EQ(
        decideDatabaseSchemaWALRecovery(decoded_transition, decoded_commit), DatabaseSchemaWALRecoveryDecision::CompleteCommitted);

    auto prepared = DependentObjectActivationPlanner::plan(*root, 11, 1);
    const auto & replacement = prepared.getReplacementRoot();
    EXPECT_EQ(prepared.getValidatedTransition().pinAfterInventory().get(), original_inventory.get());
    EXPECT_EQ(prepared.getValidatedTransition().pinAfterGraph().get(), original_graph.get());
    EXPECT_TRUE(root->sharesContentPayloadWith(replacement));
    EXPECT_EQ(replacement.getDatabaseCatalogEpoch(), 2);
    EXPECT_EQ(replacement.getPersistentCapabilityMask(), dependent_object_authority_capability_mask);
    EXPECT_EQ(replacement.getInventorySummary(), root->getInventorySummary());
    EXPECT_EQ(replacement.getSchemaObjectDependencyGraph().computeRoot(), root->getSchemaObjectDependencyGraph().computeRoot());
    EXPECT_EQ(replacement.getTypeIndexGeneration(), original_generation);
    EXPECT_EQ(replacement.getTypeIndexContentDigest(), original_digest);

    const auto & prepare = prepared.getValidatedTransition().getPrepare();
    EXPECT_EQ(prepare.transaction_id, 11);
    EXPECT_EQ(prepare.before_authority_state, original_state);
    EXPECT_EQ(prepare.after_authority_state, replacement.getAuthorityState());
    EXPECT_TRUE(prepare.authority_record_deltas.empty());
    EXPECT_TRUE(prepare.dependent_object_deltas.empty());
    EXPECT_TRUE(prepare.graph_delta.node_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.node_removals.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_additions.empty());
    EXPECT_TRUE(prepare.graph_delta.edge_removals.empty());
    EXPECT_TRUE(prepare.staged_artifacts.empty());
    EXPECT_TRUE(prepared.getValidatedTransition().getStagedArtifactBytes().empty());

    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(root));
    auto publication = authority.preparePublication(prepared.releaseReplacementRoot());
    authority.publish(std::move(publication));
    auto active = authority.acquireCurrentRoot();
    ASSERT_TRUE(active);
    EXPECT_EQ(active.get().getAuthorityState(), prepare.after_authority_state);
    EXPECT_EQ(active.get().getTypeIndexGeneration(), original_generation);
    EXPECT_EQ(active.get().getTypeIndexContentDigest(), original_digest);
}

TEST(DependentObjectActivationPlanner, InvalidSourceEpochAndLimitsFailBeforePublication)
{
    const UUID database_uuid = uuid(10, 20);
    auto root = definition_onlyRoot(database_uuid);
    expectActivationError(
        DependentObjectActivationPlannerError::Code::InvalidRequest,
        [&] { static_cast<void>(DependentObjectActivationPlanner::plan(*root, 0)); });
    expectActivationError(
        DependentObjectActivationPlannerError::Code::ExpectedEpochMismatch,
        [&] { static_cast<void>(DependentObjectActivationPlanner::plan(*root, 1, 2)); });

    auto activated = DependentObjectActivationPlanner::plan(*root, 1).releaseReplacementRoot();
    expectActivationError(
        DependentObjectActivationPlannerError::Code::CapabilityMismatch,
        [&] { static_cast<void>(DependentObjectActivationPlanner::plan(*activated, 2)); });

    DependentObjectActivationPlannerLimits limits;
    limits.schema_wal.authority_state.maximum_encoded_bytes = 1;
    expectActivationError(
        DependentObjectActivationPlannerError::Code::LimitExceeded,
        [&] { static_cast<void>(DependentObjectActivationPlanner::plan(*root, 1, 1, limits)); });

    limits = {};
    limits.schema_wal.inventory_snapshot.inventory.maximum_leaf_bytes = 1;
    expectActivationError(
        DependentObjectActivationPlannerError::Code::LimitExceeded,
        [&] { static_cast<void>(DependentObjectActivationPlanner::plan(*root, 1, 1, limits)); });
}

}
}
