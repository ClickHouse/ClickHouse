#include <Databases/SchemaObjectDependencyGraph.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string_view>
#include <utility>
#include <vector>


namespace DB::UDT
{
namespace
{

UUID makeUUID(UInt64 high, UInt64 low)
{
    UUID result;
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

SchemaObjectID makeObject(SchemaObjectKind kind, UInt64 object, UUID database = makeUUID(0xD000, 1))
{
    return {
        .kind = kind,
        .database_uuid = database,
        .object_uuid = makeUUID(0xA000, object),
    };
}

SchemaObjectDependencyEdge dependsOn(const SchemaObjectID & dependent, const SchemaObjectID & dependency)
{
    SchemaObjectDependencyEdgeKind kind;
    if (dependent.kind == SchemaObjectKind::TypeDefinition)
        kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition;
    else if (dependency.kind == SchemaObjectKind::TypeDefinition)
        kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition;
    else
        kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject;
    return {.dependent = dependent, .dependency = dependency, .kind = kind};
}

SchemaObjectDependencyNeighbor neighbor(const SchemaObjectID & object, SchemaObjectDependencyEdgeKind kind)
{
    return {.object = object, .kind = kind};
}

std::vector<SchemaObjectID> copyNodes(std::span<const SchemaObjectID> nodes)
{
    return {nodes.begin(), nodes.end()};
}

std::vector<SchemaObjectDependencyNeighbor> copyNeighbors(std::span<const SchemaObjectDependencyNeighbor> neighbors)
{
    return {neighbors.begin(), neighbors.end()};
}

String toHex(std::string_view bytes)
{
    constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(bytes.size() * 2);
    for (const unsigned char byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

String toHex(const Digest & bytes)
{
    return toHex({reinterpret_cast<const char *>(bytes.data()), bytes.size()});
}

template <typename F>
void expectGraphError(SchemaObjectDependencyGraphError::Code code, F && function)
{
    try
    {
        std::invoke(std::forward<F>(function));
        FAIL() << "Expected SchemaObjectDependencyGraphError";
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        EXPECT_EQ(error.code, code);
        EXPECT_LT(std::string_view{error.what()}.size(), 160u);
    }
}

TEST(SchemaObjectDependencyGraph, BuildsExactDeterministicForwardAndReverseIndexes)
{
    const UUID database_uuid = makeUUID(0xD000, 1);
    const auto type_a = makeObject(SchemaObjectKind::TypeDefinition, 10, database_uuid);
    const auto type_b = makeObject(SchemaObjectKind::TypeDefinition, 20, database_uuid);
    const auto table = makeObject(SchemaObjectKind::Table, 5, database_uuid);
    const auto view = makeObject(SchemaObjectKind::View, 7, database_uuid);

    std::vector<SchemaObjectID> nodes{type_b, view, type_a, table};
    std::vector<SchemaObjectDependencyEdge> edges{
        dependsOn(view, table),
        dependsOn(table, type_b),
        dependsOn(type_b, type_a),
        dependsOn(type_a, type_b),
        dependsOn(table, type_a),
        dependsOn(type_a, type_a),
    };

    const auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    EXPECT_EQ(graph->getDatabaseUUID(), database_uuid);
    EXPECT_EQ(graph->getEdgeCount(), edges.size());
    EXPECT_EQ(graph->getMaximumForwardDegree(), 2u);
    EXPECT_EQ(graph->getMaximumReverseDegree(), 3u);
    EXPECT_EQ(copyNodes(graph->getNodes()), (std::vector<SchemaObjectID>{table, view, type_a, type_b}));
    EXPECT_EQ(
        copyNeighbors(graph->getDependencies(table)),
        (std::vector{
            neighbor(type_a, SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition),
            neighbor(type_b, SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition),
        }));
    EXPECT_EQ(
        copyNeighbors(graph->getDependencies(type_a)),
        (std::vector{
            neighbor(type_a, SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition),
            neighbor(type_b, SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition),
        }));
    EXPECT_EQ(
        copyNeighbors(graph->getDependents(type_a)),
        (std::vector{
            neighbor(table, SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition),
            neighbor(type_a, SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition),
            neighbor(type_b, SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition),
        }));
    EXPECT_EQ(
        copyNeighbors(graph->getDependents(table)), (std::vector{neighbor(view, SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)}));
    EXPECT_TRUE(graph->containsEdge(dependsOn(type_a, type_a)));
    EXPECT_FALSE(graph->containsEdge(dependsOn(view, type_a)));

    std::reverse(nodes.begin(), nodes.end());
    std::reverse(edges.begin(), edges.end());
    const auto permuted = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    EXPECT_EQ(permuted->encodeSnapshot(), graph->encodeSnapshot());
    EXPECT_EQ(permuted->computeRoot(), graph->computeRoot());
}

TEST(SchemaObjectDependencyGraph, SnapshotV1HasFrozenBytesAndRoot)
{
    const UUID database_uuid = makeUUID(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    const SchemaObjectID table{
        .kind = SchemaObjectKind::Table,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 1),
    };
    const SchemaObjectID type{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 2),
    };
    const auto graph = SchemaObjectDependencyGraph::build(database_uuid, std::vector{type, table}, std::vector{dependsOn(table, type)});

    const String encoded = graph->encodeSnapshot();
    EXPECT_EQ(
        toHex(encoded),
        "010000112233445566778899aabbccddeeff02"
        "0100112233445566778899aabbccddeeff00000000000000000000000000000001"
        "0400112233445566778899aabbccddeeff00000000000000000000000000000002"
        "01"
        "0100112233445566778899aabbccddeeff00000000000000000000000000000001"
        "0400112233445566778899aabbccddeeff00000000000000000000000000000002"
        "02");
    EXPECT_EQ(toHex(graph->computeRoot()), "747109a9c74760264d6d9519dd976c85e3d8c518411a9a20c51ece19128396a6");

    const auto decoded = SchemaObjectDependencyGraph::decodeSnapshot(encoded);
    EXPECT_EQ(decoded->encodeSnapshot(), encoded);
    EXPECT_EQ(decoded->computeRoot(), graph->computeRoot());
}

TEST(SchemaObjectDependencyGraph, RootV1HasFrozenEmptyAndEdgeBranchHashes)
{
    const UUID database_uuid = makeUUID(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    const SchemaObjectID table{
        .kind = SchemaObjectKind::Table,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 1),
    };
    const SchemaObjectID type_a{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 2),
    };
    const SchemaObjectID type_b{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 3),
    };

    const auto empty = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    EXPECT_EQ(toHex(empty->computeRoot()), "6754e1ca0a6bc6cfb62bcaad38cbaccadd62b3d3f74defc9fdbce0eeb37617f1");
    EXPECT_EQ(empty->getNodeCount(), 0u);
    EXPECT_EQ(empty->getMaximumForwardDegree(), 0u);
    EXPECT_EQ(empty->getMaximumReverseDegree(), 0u);

    const auto first_edge = dependsOn(table, type_a);
    const auto second_edge = dependsOn(table, type_b);
    const auto multi_edge
        = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a, type_b}, std::vector{first_edge, second_edge});
    EXPECT_EQ(toHex(multi_edge->computeRoot()), "3906859b45e85069454e1f20cc0f26cea6676d1b599d3044441e12aa68d24f7c");
    EXPECT_EQ(multi_edge->getNodeCount(), 3u);
    EXPECT_EQ(multi_edge->getMaximumForwardDegree(), 2u);
    EXPECT_EQ(multi_edge->getMaximumReverseDegree(), 1u);

    auto narrow_limits = multi_edge->getLimits();
    narrow_limits.maximum_edges_per_node = 1;
    expectGraphError(SchemaObjectDependencyGraphError::Code::LimitExceeded, [&] { multi_edge->validateAgainstLimits(narrow_limits); });
    SchemaObjectDependencyGraphMutation mutation;
    mutation.edge_removals = {second_edge};
    const auto reduced = SchemaObjectDependencyGraph::applyMutation(multi_edge, mutation);
    EXPECT_EQ(reduced->getMaximumForwardDegree(), 1u);
    EXPECT_EQ(reduced->getMaximumReverseDegree(), 1u);
    EXPECT_NO_THROW(reduced->validateAgainstLimits(narrow_limits));
}

TEST(SchemaObjectDependencyGraph, PreservesIsolatedNodesAcrossSnapshotAndMutation)
{
    const UUID database_uuid = makeUUID(7, 8);
    const auto isolated = makeObject(SchemaObjectKind::SyntheticTestObject, 1, database_uuid);
    const auto graph = SchemaObjectDependencyGraph::build(database_uuid, std::vector{isolated}, {});

    EXPECT_TRUE(graph->containsNode(isolated));
    EXPECT_EQ(graph->getEdgeCount(), 0u);
    const auto decoded = SchemaObjectDependencyGraph::decodeSnapshot(graph->encodeSnapshot());
    EXPECT_EQ(copyNodes(decoded->getNodes()), (std::vector{isolated}));

    SchemaObjectDependencyGraphMutation mutation;
    mutation.node_removals = {isolated};
    const auto empty = SchemaObjectDependencyGraph::applyMutation(decoded, mutation);
    EXPECT_TRUE(empty->getNodes().empty());
    EXPECT_EQ(empty->encodeSnapshot(), SchemaObjectDependencyGraph::createEmpty(database_uuid)->encodeSnapshot());
}

TEST(SchemaObjectDependencyGraph, AppliesExactImmutableNodeAndEdgeBatch)
{
    const UUID database_uuid = makeUUID(0xD000, 1);
    const auto type_a = makeObject(SchemaObjectKind::TypeDefinition, 10, database_uuid);
    const auto type_b = makeObject(SchemaObjectKind::TypeDefinition, 20, database_uuid);
    const auto table_c = makeObject(SchemaObjectKind::Table, 30, database_uuid);
    const auto table_d = makeObject(SchemaObjectKind::Table, 40, database_uuid);
    const auto a_to_b = dependsOn(type_a, type_b);
    const auto c_to_a = dependsOn(table_c, type_a);
    const auto c_to_b = dependsOn(table_c, type_b);
    const auto b_to_a = dependsOn(type_b, type_a);
    const auto d_to_b = dependsOn(table_d, type_b);

    const auto base
        = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table_c, type_b, type_a}, std::vector{c_to_b, a_to_b, c_to_a});
    const SchemaObjectDependencyGraphMutation mutation{
        .node_additions = {table_d},
        .node_removals = {},
        .edge_additions = {d_to_b, b_to_a},
        .edge_removals = {c_to_b},
    };
    const auto next = SchemaObjectDependencyGraph::applyMutation(base, mutation);

    EXPECT_NE(next.get(), base.get());
    EXPECT_EQ(base->getEdgeCount(), 3u);
    EXPECT_FALSE(base->containsNode(table_d));
    EXPECT_TRUE(base->containsEdge(c_to_b));
    EXPECT_FALSE(base->containsEdge(b_to_a));

    EXPECT_EQ(next->getEdgeCount(), 4u);
    EXPECT_EQ(base->getMaximumForwardDegree(), 2u);
    EXPECT_EQ(base->getMaximumReverseDegree(), 2u);
    EXPECT_EQ(next->getMaximumForwardDegree(), 1u);
    EXPECT_EQ(next->getMaximumReverseDegree(), 2u);
    EXPECT_TRUE(next->containsNode(table_d));
    EXPECT_FALSE(next->containsEdge(c_to_b));
    EXPECT_TRUE(next->containsEdge(a_to_b));
    EXPECT_TRUE(next->containsEdge(c_to_a));
    EXPECT_TRUE(next->containsEdge(b_to_a));
    EXPECT_TRUE(next->containsEdge(d_to_b));
    EXPECT_EQ(
        copyNeighbors(next->getDependents(type_b)),
        (std::vector{
            neighbor(table_d, SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition),
            neighbor(type_a, SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition),
        }));

    const auto unchanged = SchemaObjectDependencyGraph::applyMutation(next, {});
    EXPECT_EQ(unchanged.get(), next.get());
}

TEST(SchemaObjectDependencyGraph, PersistentMerkleMutationMatchesFullAndSequentialBuildWithFixedWork)
{
    constexpr UInt64 pair_count = 2'048;
    const UUID database_uuid = makeUUID(0xD000, 1);
    std::vector<SchemaObjectID> nodes;
    std::vector<SchemaObjectDependencyEdge> edges;
    nodes.reserve(pair_count + 1);
    edges.reserve(pair_count);
    const auto shared_type = makeObject(SchemaObjectKind::TypeDefinition, pair_count + 1, database_uuid);
    nodes.push_back(shared_type);
    for (UInt64 index = 1; index <= pair_count; ++index)
    {
        const auto table = makeObject(SchemaObjectKind::Table, index, database_uuid);
        nodes.push_back(table);
        edges.push_back(dependsOn(table, shared_type));
    }
    const auto base = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);

    const auto first_removed = edges[511];
    const auto second_removed = edges[1'535];
    SchemaObjectDependencyGraphMutation mutation;
    mutation.edge_removals = {first_removed, second_removed};
    SchemaObjectDependencyGraphMutationStatistics statistics;
    const auto next = SchemaObjectDependencyGraph::applyMutation(base, mutation, &statistics);

    EXPECT_EQ(statistics.edge_deltas_applied, 2u);
    EXPECT_EQ(statistics.node_deltas_applied, 0u);
    EXPECT_EQ(statistics.snapshot_nodes_materialized, 0u);
    EXPECT_EQ(statistics.snapshot_edges_materialized, 0u);
    EXPECT_LE(statistics.set_nodes_visited, 2u * 135u);
    EXPECT_LE(statistics.set_nodes_hashed, 2u * 134u);
    EXPECT_LE(statistics.adjacency_nodes_visited, 6u * 67u);
    EXPECT_LE(statistics.neighbor_nodes_visited, 6u * 69u);
    EXPECT_LE(statistics.neighbor_nodes_created, 4u * 68u);
    EXPECT_EQ(statistics.adjacency_neighbors_copied, 0u);
    EXPECT_EQ(statistics.neighbors_materialized, 0u);

    std::erase_if(edges, [&](const auto & edge) { return edge == first_removed || edge == second_removed; });
    const auto rebuilt = SchemaObjectDependencyGraph::build(database_uuid, nodes, edges);
    EXPECT_EQ(next->computeRoot(), rebuilt->computeRoot());
    EXPECT_EQ(next->encodeSnapshot(), rebuilt->encodeSnapshot());

    SchemaObjectDependencyGraphMutation first_mutation;
    first_mutation.edge_removals = {first_removed};
    const auto first = SchemaObjectDependencyGraph::applyMutation(base, first_mutation);
    SchemaObjectDependencyGraphMutation second_mutation;
    second_mutation.edge_removals = {second_removed};
    const auto sequential = SchemaObjectDependencyGraph::applyMutation(first, second_mutation);
    EXPECT_EQ(sequential->computeRoot(), rebuilt->computeRoot());
}

TEST(SchemaObjectDependencyGraph, RejectsAmbiguousOrInexactMutations)
{
    const UUID database_uuid = makeUUID(0xD000, 1);
    const auto type_a = makeObject(SchemaObjectKind::TypeDefinition, 10, database_uuid);
    const auto type_b = makeObject(SchemaObjectKind::TypeDefinition, 20, database_uuid);
    const auto table = makeObject(SchemaObjectKind::Table, 30, database_uuid);
    const auto view = makeObject(SchemaObjectKind::View, 40, database_uuid);
    const auto existing = dependsOn(table, type_a);
    const auto absent = dependsOn(table, type_b);
    const auto base = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a, type_b}, std::vector{existing});

    expectGraphError(
        SchemaObjectDependencyGraphError::Code::ExistingEdge,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.edge_additions = {existing};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingEdge,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.edge_removals = {absent};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::DuplicateEdge,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.edge_additions = {absent, absent};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::ConflictingMutation,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.edge_additions = {existing};
            mutation.edge_removals = {existing};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::ExistingNode,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.node_additions = {table};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingNode,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.node_removals = {view};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::DuplicateNode,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.node_additions = {view, view};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingNode,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.node_removals = {table};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingNode,
        [&]
        {
            SchemaObjectDependencyGraphMutation mutation;
            mutation.edge_additions = {dependsOn(view, type_a)};
            static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, mutation));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidConfiguration,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::applyMutation({}, {})); });
}

TEST(SchemaObjectDependencyGraph, EnforcesIdentitySemanticAndResourceBounds)
{
    const UUID database_uuid = makeUUID(0xD000, 1);
    const auto type_a = makeObject(SchemaObjectKind::TypeDefinition, 10, database_uuid);
    const auto type_b = makeObject(SchemaObjectKind::TypeDefinition, 20, database_uuid);
    const auto table = makeObject(SchemaObjectKind::Table, 30, database_uuid);
    const auto view = makeObject(SchemaObjectKind::View, 40, database_uuid);
    const auto a_to_b = dependsOn(type_a, type_b);
    const auto table_to_a = dependsOn(table, type_a);

    SchemaObjectID invalid = type_a;
    invalid.kind = static_cast<SchemaObjectKind>(0);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{invalid}, {})); });

    invalid = type_a;
    invalid.object_uuid = UUIDHelpers::Nil;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{invalid}, {})); });

    invalid = type_a;
    invalid.database_uuid = makeUUID(0xD000, 2);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{invalid}, {})); });

    auto invalid_edge = table_to_a;
    invalid_edge.kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&]
        { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a}, std::vector{invalid_edge})); });

    expectGraphError(
        SchemaObjectDependencyGraphError::Code::DuplicateNode,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{type_a, type_a}, {})); });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::DuplicateEdge,
        [&]
        {
            static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{type_a, type_b}, std::vector{a_to_b, a_to_b}));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingNode,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{type_a}, std::vector{a_to_b})); });

    auto limits = SchemaObjectDependencyGraphLimits{};
    limits.maximum_edges = 1;
    limits.maximum_edges_per_node = 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(SchemaObjectDependencyGraph::build(
                database_uuid, std::vector{table, type_a, type_b}, std::vector{a_to_b, table_to_a}, limits));
        });

    limits = {};
    limits.maximum_nodes = 2;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a, type_b}, {}, limits)); });

    limits = {};
    limits.maximum_edges_per_node = 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(SchemaObjectDependencyGraph::build(
                database_uuid,
                std::vector{table, type_a, type_b},
                std::vector{dependsOn(table, type_a), dependsOn(table, type_b)},
                limits));
        });
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(SchemaObjectDependencyGraph::build(
                database_uuid, std::vector{table, view, type_a}, std::vector{dependsOn(table, type_a), dependsOn(view, type_a)}, limits));
        });

    limits = {};
    limits.maximum_retained_bytes = 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::createEmpty(database_uuid, limits)); });
}

TEST(SchemaObjectDependencyGraph, EnforcesConfigurationAndProspectiveMutationBounds)
{
    const UUID database_uuid = makeUUID(0xD000, 1);
    const auto type_a = makeObject(SchemaObjectKind::TypeDefinition, 10, database_uuid);
    const auto type_b = makeObject(SchemaObjectKind::TypeDefinition, 20, database_uuid);
    const auto table = makeObject(SchemaObjectKind::Table, 30, database_uuid);
    const auto view = makeObject(SchemaObjectKind::View, 40, database_uuid);
    const auto old_edge = dependsOn(table, type_a);
    const auto replacement_edge = dependsOn(view, type_b);

    auto limits = SchemaObjectDependencyGraphLimits{};
    limits.maximum_nodes = schema_object_dependency_graph_maximum_nodes + 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidConfiguration,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::createEmpty(database_uuid, limits)); });

    const auto measured = SchemaObjectDependencyGraph::build(
        database_uuid, std::vector{table, type_a, type_b}, std::vector{dependsOn(table, type_a), dependsOn(table, type_b)});
    limits = {};
    limits.maximum_retained_bytes = measured->getAccountedBytes();
    const auto exact_fit = SchemaObjectDependencyGraph::build(
        database_uuid, std::vector{table, type_a, type_b}, std::vector{dependsOn(table, type_a), dependsOn(table, type_b)}, limits);
    EXPECT_EQ(exact_fit->getAccountedBytes(), measured->getAccountedBytes());
    --limits.maximum_retained_bytes;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(SchemaObjectDependencyGraph::build(
                database_uuid,
                std::vector{table, type_a, type_b},
                std::vector{dependsOn(table, type_a), dependsOn(table, type_b)},
                limits));
        });

    limits = {};
    limits.maximum_nodes = 3;
    limits.maximum_edges = 1;
    limits.maximum_edges_per_node = 1;
    const auto base = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a}, std::vector{old_edge}, limits);
    const SchemaObjectDependencyGraphMutation replacement{
        .node_additions = {view, type_b},
        .node_removals = {table, type_a},
        .edge_additions = {replacement_edge},
        .edge_removals = {old_edge},
    };
    const auto replaced = SchemaObjectDependencyGraph::applyMutation(base, replacement);
    EXPECT_EQ(replaced->getEdgeCount(), 1u);
    EXPECT_FALSE(replaced->containsNode(table));
    EXPECT_TRUE(replaced->containsEdge(replacement_edge));

    SchemaObjectDependencyGraphMutation growth;
    growth.node_additions = {view, type_b};
    growth.edge_additions = {replacement_edge};
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::applyMutation(base, growth)); });

    limits = {};
    limits.maximum_mutation_edges = 1;
    const auto mutation_limited = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type_a, type_b}, {}, limits);
    SchemaObjectDependencyGraphMutation two_edges;
    two_edges.edge_additions = {dependsOn(table, type_a), dependsOn(table, type_b)};
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::applyMutation(mutation_limited, two_edges)); });
}

TEST(SchemaObjectDependencyGraph, RejectsMalformedAndNoncanonicalSnapshots)
{
    constexpr size_t header_bytes = sizeof(UInt16) + sizeof(CanonicalUUID);
    constexpr size_t wire_node_bytes = sizeof(UInt8) + 2 * sizeof(CanonicalUUID);
    constexpr size_t wire_edge_bytes = 2 * wire_node_bytes + sizeof(UInt8);
    constexpr size_t first_node_offset = header_bytes + 1;
    constexpr size_t edge_count_offset = first_node_offset + 2 * wire_node_bytes;
    constexpr size_t first_edge_offset = edge_count_offset + 1;

    const UUID database_uuid = makeUUID(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    const SchemaObjectID table{
        .kind = SchemaObjectKind::Table,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 1),
    };
    const SchemaObjectID type{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = makeUUID(0, 2),
    };
    const String valid = SchemaObjectDependencyGraph::build(database_uuid, std::vector{table, type}, std::vector{dependsOn(table, type)})
                             ->encodeSnapshot();
    ASSERT_EQ(valid.size(), first_edge_offset + wire_edge_bytes);

    String declared_nodes{valid.data(), header_bytes};
    declared_nodes.append("\xc0\x9a\x0c", 3); // maximum_nodes, but no declared node bytes
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::Truncated,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(declared_nodes)); });

    String declared_edges{valid.data(), edge_count_offset};
    declared_edges.append("\x80\x80\x80\x02", 4); // maximum_edges, but no declared edge bytes
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::Truncated,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(declared_edges)); });

    expectGraphError(
        SchemaObjectDependencyGraphError::Code::Truncated,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(std::string_view{valid}.substr(0, valid.size() - 1))); });

    String trailing = valid;
    trailing.push_back('\0');
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::TrailingData,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(trailing)); });

    String unknown_version = valid;
    unknown_version[0] = 2;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::UnsupportedVersion,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unknown_version)); });

    String nil_database = valid;
    std::fill_n(nil_database.begin() + sizeof(UInt16), sizeof(CanonicalUUID), '\0');
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(nil_database)); });

    String unknown_node_kind = valid;
    unknown_node_kind[first_node_offset] = static_cast<char>(0x7f);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unknown_node_kind)); });

    String cross_database_node = valid;
    cross_database_node[first_node_offset + 1] ^= 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(cross_database_node)); });

    String nil_object = valid;
    std::fill_n(nil_object.begin() + first_node_offset + sizeof(UInt8) + sizeof(CanonicalUUID), sizeof(CanonicalUUID), '\0');
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(nil_object)); });

    String nonminimal_nodes;
    nonminimal_nodes.reserve(valid.size() + 1);
    nonminimal_nodes.append(valid.data(), header_bytes);
    nonminimal_nodes.push_back(static_cast<char>(0x82));
    nonminimal_nodes.push_back('\0');
    nonminimal_nodes.append(valid.data() + header_bytes + 1, valid.size() - header_bytes - 1);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(nonminimal_nodes)); });

    String unterminated_count{valid.data(), header_bytes};
    unterminated_count.push_back(static_cast<char>(0x80));
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::Truncated,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unterminated_count)); });

    String overflowing_count{valid.data(), header_bytes};
    overflowing_count.append(10, static_cast<char>(0x80));
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(overflowing_count)); });

    String unsorted_nodes = valid;
    std::swap_ranges(
        unsorted_nodes.begin() + first_node_offset,
        unsorted_nodes.begin() + first_node_offset + wire_node_bytes,
        unsorted_nodes.begin() + first_node_offset + wire_node_bytes);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unsorted_nodes)); });

    String duplicate_nodes = valid;
    std::copy_n(
        duplicate_nodes.begin() + first_node_offset, wire_node_bytes, duplicate_nodes.begin() + first_node_offset + wire_node_bytes);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(duplicate_nodes)); });

    String unknown_edge_kind = valid;
    unknown_edge_kind.back() = static_cast<char>(0x7f);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unknown_edge_kind)); });

    String incompatible_edge_kind = valid;
    incompatible_edge_kind.back() = static_cast<char>(SchemaObjectDependencyEdgeKind::ObjectDependsOnObject);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(incompatible_edge_kind)); });

    String cross_database_edge = valid;
    cross_database_edge[first_edge_offset + 1] ^= 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::InvalidValue,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(cross_database_edge)); });

    String missing_endpoint = valid;
    missing_endpoint[first_edge_offset + wire_node_bytes + sizeof(UInt8) + sizeof(CanonicalUUID) + sizeof(CanonicalUUID) - 1] = 3;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::MissingNode,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(missing_endpoint)); });

    String nonminimal_edges;
    nonminimal_edges.reserve(valid.size() + 1);
    nonminimal_edges.append(valid.data(), edge_count_offset);
    nonminimal_edges.push_back(static_cast<char>(0x81));
    nonminimal_edges.push_back('\0');
    nonminimal_edges.append(valid.data() + edge_count_offset + 1, valid.size() - edge_count_offset - 1);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(nonminimal_edges)); });

    auto limits = SchemaObjectDependencyGraphLimits{};
    limits.maximum_nodes = 1;
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::LimitExceeded,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(valid, limits)); });
}

TEST(SchemaObjectDependencyGraph, RejectsUnsortedAndDuplicateWireEdges)
{
    constexpr size_t header_bytes = sizeof(UInt16) + sizeof(CanonicalUUID);
    constexpr size_t wire_node_bytes = sizeof(UInt8) + 2 * sizeof(CanonicalUUID);
    constexpr size_t wire_edge_bytes = 2 * wire_node_bytes + sizeof(UInt8);
    constexpr size_t first_edge_offset = header_bytes + 1 + 3 * wire_node_bytes + 1;

    const UUID database_uuid = makeUUID(9, 10);
    const auto table = makeObject(SchemaObjectKind::Table, 1, database_uuid);
    const auto view = makeObject(SchemaObjectKind::View, 2, database_uuid);
    const auto type = makeObject(SchemaObjectKind::TypeDefinition, 3, database_uuid);
    const String valid = SchemaObjectDependencyGraph::build(
                             database_uuid, std::vector{type, view, table}, std::vector{dependsOn(view, type), dependsOn(table, type)})
                             ->encodeSnapshot();
    ASSERT_EQ(valid.size(), first_edge_offset + 2 * wire_edge_bytes);

    String unsorted = valid;
    std::swap_ranges(
        unsorted.begin() + first_edge_offset,
        unsorted.begin() + first_edge_offset + wire_edge_bytes,
        unsorted.begin() + first_edge_offset + wire_edge_bytes);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(unsorted)); });

    String duplicate = valid;
    std::copy_n(duplicate.begin() + first_edge_offset, wire_edge_bytes, duplicate.begin() + first_edge_offset + wire_edge_bytes);
    expectGraphError(
        SchemaObjectDependencyGraphError::Code::NonCanonical,
        [&] { static_cast<void>(SchemaObjectDependencyGraph::decodeSnapshot(duplicate)); });
}

}
}
