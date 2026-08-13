#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Common/ProfileEvents.h>
#include <Common/tests/gtest_global_register.h>

#include <array>
#include <string_view>
#include <utility>
#include <vector>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace ProfileEvents
{
extern const Event JoinOrderDPhypExplicitHyperedges;
extern const Event JoinOrderDPhypProvenEdgeCandidatesAssessed;
extern const Event JoinOrderDPhypProvenEdgeCandidatesRejected;
extern const Event JoinOrderDPhypProvenSyntheticEdges;
extern const Event JoinOrderDPhypCandidatesAdmitted;
extern const Event JoinOrderDPhypCandidatesRejected;
extern const Event JoinOrderDPhypSearchedPlans;
extern const Event JoinOrderDPhypFallbacks;
}

namespace
{

template <typename DataType>
Block makeTypedHeader(std::initializer_list<String> names)
{
    auto type = std::make_shared<DataType>();
    Block result;
    for (const auto & name : names)
        result.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
    return result;
}

Block makeHeader(std::initializer_list<String> names)
{
    return makeTypedHeader<DataTypeUInt64>(names);
}

enum class UniqueKeyEvidence : UInt8
{
    AggregationGrouping,
    StorageDeclaration,
};

DataPropertySet makeProperties(
    const Block & header, std::vector<size_t> unique_key = {}, UniqueKeyEvidence evidence = UniqueKeyEvidence::AggregationGrouping)
{
    DataPropertySet result;
    for (size_t position = 0; position < header.columns(); ++position)
        result.addNonNullColumn({position, header.getByPosition(position).name});
    if (!unique_key.empty())
    {
        QueryPlanOptimizations::ColumnSet columns;
        for (const auto position : unique_key)
            columns.push_back({position, header.getByPosition(position).name});
        if (evidence == UniqueKeyEvidence::AggregationGrouping)
            result.addUniqueKey(UniqueKeyFact::fromAggregationGrouping(std::move(columns)));
        else
            result.addUniqueKey(UniqueKeyFact::fromStorageDeclaration(std::move(columns)));
    }
    return result;
}

std::shared_ptr<const JoinOrderDataPropertyCatalog>
makeCatalog(const std::vector<std::pair<DataPropertySet, Block>> & leaves, bool diagnostics = true)
{
    JoinOrderDataPropertyCatalogBuilder builder;
    for (const auto & [properties, header] : leaves)
        builder.appendLeaf(properties, header);
    return std::move(builder).finalize(
        diagnostics ? JoinOrderDataPropertyCatalogMode::Diagnostics : JoinOrderDataPropertyCatalogMode::Costing);
}

struct DPhypCounterSnapshot
{
    ProfileEvents::Count explicit_hyperedges = 0;
    ProfileEvents::Count edge_candidates_assessed = 0;
    ProfileEvents::Count edge_candidates_rejected = 0;
    ProfileEvents::Count proven_synthetic_edges = 0;
    ProfileEvents::Count candidates_admitted = 0;
    ProfileEvents::Count candidates_rejected = 0;
    ProfileEvents::Count searched_plans = 0;
    ProfileEvents::Count fallbacks = 0;
};

DPhypCounterSnapshot getDPhypCounterSnapshot()
{
    return {
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypExplicitHyperedges],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypProvenEdgeCandidatesAssessed],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypProvenEdgeCandidatesRejected],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypProvenSyntheticEdges],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypCandidatesAdmitted],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypCandidatesRejected],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypSearchedPlans],
        ProfileEvents::global_counters[ProfileEvents::JoinOrderDPhypFallbacks]};
}

DPhypCounterSnapshot operator-(const DPhypCounterSnapshot & after, const DPhypCounterSnapshot & before)
{
    return {
        after.explicit_hyperedges - before.explicit_hyperedges,
        after.edge_candidates_assessed - before.edge_candidates_assessed,
        after.edge_candidates_rejected - before.edge_candidates_rejected,
        after.proven_synthetic_edges - before.proven_synthetic_edges,
        after.candidates_admitted - before.candidates_admitted,
        after.candidates_rejected - before.candidates_rejected,
        after.searched_plans - before.searched_plans,
        after.fallbacks - before.fallbacks};
}

void expectCanonicalCapAssessments(
    const JoinOrderOptimizationDebugInfo & debug_info,
    UInt64 proven,
    UInt64 missing_input_rows,
    UInt64 not_proven,
    UInt64 unsupported,
    std::string_view context)
{
    EXPECT_EQ(debug_info.cap_assessments.proven, proven) << context;
    EXPECT_EQ(debug_info.cap_assessments.missing_input_rows, missing_input_rows) << context;
    EXPECT_EQ(debug_info.cap_assessments.not_proven, not_proven) << context;
    EXPECT_EQ(debug_info.cap_assessments.unsupported, unsupported) << context;
}

void expectCanonicalDebugInfoEmpty(const JoinOrderOptimizationDebugInfo & debug_info, std::string_view context)
{
    EXPECT_FALSE(debug_info.canonical_metrics.has_value()) << context;
    expectCanonicalCapAssessments(debug_info, 0, 0, 0, 0, context);
}

void expectCanonicalInferenceMetricsEmpty(const JoinOrderOptimizationDebugInfo & debug_info, std::string_view context)
{
    ASSERT_TRUE(debug_info.canonical_metrics.has_value()) << context;
    const auto & metrics = *debug_info.canonical_metrics;
    EXPECT_EQ(metrics.groups, 0) << context;
    EXPECT_EQ(metrics.demands, 0) << context;
    EXPECT_EQ(metrics.cuts, 0) << context;
    EXPECT_EQ(metrics.cache_hits, 0) << context;
    EXPECT_EQ(metrics.cache_misses, 0) << context;
    EXPECT_EQ(metrics.equality_members_visited, 0) << context;
    EXPECT_EQ(metrics.key_checks, 0) << context;
    EXPECT_EQ(metrics.key_firings, 0) << context;
    EXPECT_EQ(metrics.proofs, 0) << context;
}

std::pair<std::optional<UInt64>, double> optimizeTwoRelationJoin(
    const QueryPlanOptimizationSettings & settings,
    bool right_unique = true,
    UniqueKeyEvidence right_key_evidence = UniqueKeyEvidence::AggregationGrouping,
    std::optional<UInt64> left_rows = 100,
    std::optional<UInt64> right_rows = 20,
    std::optional<JoinOrderPropertyUnsupportedReason> region_rejection = {},
    JoinOrderOptimizationDebugInfo * debug_info = nullptr)
{
    tryRegisterFunctions();
    auto left_header = makeHeader({"left_id"});
    auto right_header = makeHeader({"right_id"});
    JoinExpressionActions actions(left_header, right_header);
    auto left_action = actions.findNode("left_id", true);
    auto right_action = actions.findNode("right_id", true);
    left_action.setSourceRelations(BitSet().set(0));
    right_action.setSourceRelations(BitSet().set(1));
    auto predicate = JoinActionRef::transform({left_action, right_action}, JoinActionRef::AddFunction(JoinConditionOperator::Equals));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = left_rows, .column_stats = {{"left_id", ColumnStats{.num_distinct_values = 1}}}, .table_name = "left"},
           {.estimated_rows = right_rows, .column_stats = {{"right_id", ColumnStats{.num_distinct_values = 1}}}, .table_name = "right"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(left_header), left_header},
         {makeProperties(right_header, right_unique ? std::vector<size_t>{0} : std::vector<size_t>{}, right_key_evidence), right_header}});
    graph.canonical_property_region_rejection = region_rejection;
    graph.edges.push_back(predicate);

    auto result = optimizeJoinOrder(std::move(graph), settings, debug_info);
    return {result->estimated_rows, result->cost};
}

DPJoinEntryPtr optimizeTriangleJoin(const QueryPlanOptimizationSettings & settings, JoinOrderOptimizationDebugInfo * debug_info = nullptr)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_id"});
    auto b_header = makeHeader({"b_id"});
    auto c_header = makeHeader({"c_id"});
    auto right_header = makeHeader({"b_id", "c_id"});
    JoinExpressionActions actions(a_header, right_header);
    auto a = actions.findNode("a_id", true);
    auto b = actions.findNode("b_id", true);
    auto c = actions.findNode("c_id", true);
    a.setSourceRelations(BitSet().set(0));
    b.setSourceRelations(BitSet().set(1));
    c.setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 100, .column_stats = {{"a_id", ColumnStats{.num_distinct_values = 1}}}, .table_name = "a"},
           {.estimated_rows = 20, .column_stats = {{"b_id", ColumnStats{.num_distinct_values = 1}}}, .table_name = "b"},
           {.estimated_rows = 5, .column_stats = {{"c_id", ColumnStats{.num_distinct_values = 1}}}, .table_name = "c"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {0}), a_header}, {makeProperties(b_header, {0}), b_header}, {makeProperties(c_header, {0}), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a, b}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b, c}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a, c}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.column_equivalences.add(a, b);
    graph.column_equivalences.add(b, c);
    return optimizeJoinOrder(std::exchange(graph, {}), settings, debug_info);
}

struct OptimizedJoinWithActions
{
    std::unique_ptr<JoinExpressionActions> actions;
    DPJoinEntryPtr plan;
};

enum class TransitiveChainCanonicalOutcome : UInt8
{
    NotProven,
    MissingInputRows,
    Unsupported,
    Proven,
};

OptimizedJoinWithActions optimizeTransitiveChain(
    const QueryPlanOptimizationSettings & settings,
    TransitiveChainCanonicalOutcome outcome,
    bool add_disconnected_relation = false,
    JoinOrderOptimizationDebugInfo * debug_info = nullptr)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_x"});
    auto d_header = makeHeader({"d_x"});
    auto right_header = add_disconnected_relation ? makeHeader({"b_x", "c_x", "d_x"}) : makeHeader({"b_x", "c_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a = actions->findNode("a_x", true);
    auto b = actions->findNode("b_x", true);
    auto c = actions->findNode("c_x", true);
    a.setSourceRelations(BitSet().set(0));
    b.setSourceRelations(BitSet().set(1));
    c.setSourceRelations(BitSet().set(2));

    const bool proven = outcome == TransitiveChainCanonicalOutcome::Proven;
    std::optional<UInt64> a_rows = 10;
    if (outcome == TransitiveChainCanonicalOutcome::MissingInputRows)
        a_rows.reset();

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = a_rows, .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "b"},
           {.estimated_rows = 10, .column_stats = {{"c_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "c"}};
    std::vector<std::pair<DataPropertySet, Block>> leaves{
        {makeProperties(a_header, proven ? std::vector<size_t>{0} : std::vector<size_t>{}), a_header},
        {makeProperties(b_header), b_header},
        {makeProperties(c_header, proven ? std::vector<size_t>{0} : std::vector<size_t>{}), c_header}};
    if (add_disconnected_relation)
    {
        auto d = actions->findNode("d_x", true);
        d.setSourceRelations(BitSet().set(3));
        graph.relation_stats.push_back(
            {.estimated_rows = 2, .column_stats = {{"d_x", ColumnStats{.num_distinct_values = 2}}}, .table_name = "d"});
        leaves.emplace_back(makeProperties(d_header), d_header);
    }
    graph.data_property_catalog = makeCatalog(leaves);
    if (outcome == TransitiveChainCanonicalOutcome::Unsupported)
        graph.canonical_property_region_rejection = JoinOrderPropertyUnsupportedReason::CrossOrCommaRegion;
    graph.edges.push_back(JoinActionRef::transform({a, b}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b, c}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings, debug_info);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Four-relation mixed region where one singleton transitive edge is Proven while
/// other class-connected singleton and larger splits remain proofless. It exposes the
/// topology-induced searched-plan delta and candidate-specific rejection path.
OptimizedJoinWithActions optimizeDPhypMixedTransitiveChain(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_k", "a_x"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_x"});
    auto e_header = makeHeader({"e_x"});
    auto right_header = makeHeader({"b_x", "c_x", "e_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_k = actions->findNode("a_k", true);
    auto a_x = actions->findNode("a_x", true);
    auto b_x = actions->findNode("b_x", true);
    auto c_x = actions->findNode("c_x", true);
    auto e_x = actions->findNode("e_x", true);
    for (auto * action : {&a_k, &a_x})
        action->setSourceRelations(BitSet().set(0));
    b_x.setSourceRelations(BitSet().set(1));
    c_x.setSourceRelations(BitSet().set(2));
    e_x.setSourceRelations(BitSet().set(3));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 10,
            .column_stats = {{"a_k", ColumnStats{.num_distinct_values = 10}}, {"a_x", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "b"},
           {.estimated_rows = 10, .column_stats = {{"c_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "c"},
           {.estimated_rows = 1000000, .column_stats = {{"e_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "e"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {0}), a_header},
         {makeProperties(b_header), b_header},
         {makeProperties(c_header), c_header},
         {makeProperties(e_header), e_header}});
    graph.edges.push_back(JoinActionRef::transform({a_x, b_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_k, e_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({e_x, a_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Composite-only transitive opportunity: A is unique on (a_x, a_y), while the
/// two key components reach different relations C and D through external bridges.
/// Neither singleton A-C nor A-D cut proves the composite key; A-{C,D} does.
OptimizedJoinWithActions optimizeCompositeOnlyTransitiveTopology(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x", "a_y"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_x", "c_z"});
    auto d_header = makeHeader({"d_y", "d_z"});
    auto e_header = makeHeader({"e_y"});
    auto right_header = makeHeader({"b_x", "c_x", "c_z", "d_y", "d_z", "e_y"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_x = actions->findNode("a_x", true);
    auto a_y = actions->findNode("a_y", true);
    auto b_x = actions->findNode("b_x", true);
    auto c_x = actions->findNode("c_x", true);
    auto c_z = actions->findNode("c_z", true);
    auto d_y = actions->findNode("d_y", true);
    auto d_z = actions->findNode("d_z", true);
    auto e_y = actions->findNode("e_y", true);
    for (auto * action : {&a_x, &a_y})
        action->setSourceRelations(BitSet().set(0));
    b_x.setSourceRelations(BitSet().set(1));
    for (auto * action : {&c_x, &c_z})
        action->setSourceRelations(BitSet().set(2));
    for (auto * action : {&d_y, &d_z})
        action->setSourceRelations(BitSet().set(3));
    e_y.setSourceRelations(BitSet().set(4));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 10,
            .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 10}}, {"a_y", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "b"},
           {.estimated_rows = 10,
            .column_stats = {{"c_x", ColumnStats{.num_distinct_values = 10}}, {"c_z", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "c"},
           {.estimated_rows = 10,
            .column_stats = {{"d_y", ColumnStats{.num_distinct_values = 10}}, {"d_z", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "d"},
           {.estimated_rows = 1000000, .column_stats = {{"e_y", ColumnStats{.num_distinct_values = 10}}}, .table_name = "e"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {0, 1}), a_header},
         {makeProperties(b_header), b_header},
         {makeProperties(c_header), c_header},
         {makeProperties(d_header), d_header},
         {makeProperties(e_header), e_header}});
    graph.edges.push_back(JoinActionRef::transform({a_x, b_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_y, e_y}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({e_y, d_y}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({c_z, d_z}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

OptimizedJoinWithActions optimizeUnaryPredicateJoin(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x"});
    auto b_header = makeHeader({"b_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, b_header);
    auto a = actions->findNode("a_x", true);
    a.setSourceRelations(BitSet().set(0));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 100, .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 10}}}, .table_name = "a"},
           {.estimated_rows = 20, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 20}}}, .table_name = "b"}};
    graph.data_property_catalog = makeCatalog({{makeProperties(a_header), a_header}, {makeProperties(b_header), b_header}});
    graph.edges.push_back(JoinActionRef::transform({a, a}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

OptimizedJoinWithActions optimizeLosingProvenCandidate(
    const QueryPlanOptimizationSettings & settings,
    bool add_disconnected_relation = false,
    JoinOrderOptimizationDebugInfo * debug_info = nullptr)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_u", "a_x"});
    auto b_header = makeHeader({"b_u"});
    auto c_header = makeHeader({"c_u"});
    auto d_header = makeHeader({"d_x"});
    auto e_header = makeHeader({"e_z"});
    auto right_header = add_disconnected_relation ? makeHeader({"b_u", "c_u", "d_x", "e_z"}) : makeHeader({"b_u", "c_u", "d_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_u = actions->findNode("a_u", true);
    auto a_x = actions->findNode("a_x", true);
    auto b_u = actions->findNode("b_u", true);
    auto c_u = actions->findNode("c_u", true);
    auto d_x = actions->findNode("d_x", true);
    a_u.setSourceRelations(BitSet().set(0));
    a_x.setSourceRelations(BitSet().set(0));
    b_u.setSourceRelations(BitSet().set(1));
    c_u.setSourceRelations(BitSet().set(2));
    d_x.setSourceRelations(BitSet().set(3));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 100,
            .column_stats = {{"a_u", ColumnStats{.num_distinct_values = 100}}, {"a_x", ColumnStats{.num_distinct_values = 100}}},
            .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_u", ColumnStats{.num_distinct_values = 1000000}}}, .table_name = "b"},
           {.estimated_rows = 10, .column_stats = {{"c_u", ColumnStats{.num_distinct_values = 10}}}, .table_name = "c"},
           {.estimated_rows = 1, .column_stats = {{"d_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "d"}};
    std::vector<std::pair<DataPropertySet, Block>> leaves{
        {makeProperties(a_header, {0}), a_header},
        {makeProperties(b_header), b_header},
        {makeProperties(c_header), c_header},
        {makeProperties(d_header), d_header}};
    if (add_disconnected_relation)
    {
        auto e_z = actions->findNode("e_z", true);
        e_z.setSourceRelations(BitSet().set(4));
        graph.relation_stats.push_back(
            {.estimated_rows = 2, .column_stats = {{"e_z", ColumnStats{.num_distinct_values = 2}}}, .table_name = "e"});
        leaves.emplace_back(makeProperties(e_header), e_header);
    }
    graph.data_property_catalog = makeCatalog(leaves);
    graph.edges.push_back(JoinActionRef::transform({a_u, b_u}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_u, c_u}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_x, d_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings, debug_info);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

void expectSamePlan(const DPJoinEntryPtr & expected, const DPJoinEntryPtr & actual, std::string_view path = "root")
{
    SCOPED_TRACE(path);
    ASSERT_EQ(bool(expected), bool(actual));
    if (!expected)
        return;

    EXPECT_EQ(expected->relations, actual->relations);
    EXPECT_EQ(expected->estimated_rows, actual->estimated_rows);
    EXPECT_DOUBLE_EQ(expected->cost, actual->cost);
    EXPECT_EQ(expected->relation_id, actual->relation_id);
    EXPECT_EQ(expected->join_operator.kind, actual->join_operator.kind);
    EXPECT_EQ(expected->join_operator.strictness, actual->join_operator.strictness);
    EXPECT_EQ(expected->join_operator.locality, actual->join_operator.locality);
    EXPECT_EQ(expected->join_operator.dump(), actual->join_operator.dump());

    expectSamePlan(expected->left, actual->left, "left");
    expectSamePlan(expected->right, actual->right, "right");
}

bool planContainsEquality(const DPJoinEntryPtr & plan, std::string_view first, std::string_view second)
{
    if (!plan)
        return false;
    for (const auto & expression : plan->join_operator.expression)
    {
        const auto [op, lhs, rhs] = expression.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        const auto & lhs_name = lhs.getColumnName();
        const auto & rhs_name = rhs.getColumnName();
        if ((lhs_name == first && rhs_name == second) || (lhs_name == second && rhs_name == first))
            return true;
    }
    return planContainsEquality(plan->left, first, second) || planContainsEquality(plan->right, first, second);
}

/// An inner join selected through transitive connectivity must carry a synthesized predicate
/// after optimization; an empty expression would execute as a cross product while having been
/// costed as an equijoin.
void expectInnerJoinsHaveExpressions(const DPJoinEntryPtr & plan)
{
    if (!plan || !plan->left || !plan->right)
        return;
    if (plan->join_operator.kind == JoinKind::Inner)
        EXPECT_FALSE(plan->join_operator.expression.empty()) << plan->dump();
    expectInnerJoinsHaveExpressions(plan->left);
    expectInnerJoinsHaveExpressions(plan->right);
}

OptimizedJoinWithActions optimizeCompositeTransitiveCut(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_u", "a_x", "a_payload"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_u", "c_x", "c_payload"});
    auto right_header = makeHeader({"b_x", "c_u", "c_x", "c_payload"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_u = actions->findNode("a_u", true);
    auto a_x = actions->findNode("a_x", true);
    auto a_payload = actions->findNode("a_payload", true);
    auto b_x = actions->findNode("b_x", true);
    auto c_u = actions->findNode("c_u", true);
    auto c_x = actions->findNode("c_x", true);
    auto c_payload = actions->findNode("c_payload", true);
    for (auto * action : {&a_u, &a_x, &a_payload})
        action->setSourceRelations(BitSet().set(0));
    b_x.setSourceRelations(BitSet().set(1));
    for (auto * action : {&c_u, &c_x, &c_payload})
        action->setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 10,
            .column_stats = {{"a_u", ColumnStats{.num_distinct_values = 1}}, {"a_x", ColumnStats{.num_distinct_values = 1}}},
            .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "b"},
           {.estimated_rows = 10,
            .column_stats = {{"c_u", ColumnStats{.num_distinct_values = 1}}, {"c_x", ColumnStats{.num_distinct_values = 1}}},
            .table_name = "c"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {0, 1}), a_header}, {makeProperties(b_header), b_header}, {makeProperties(c_header, {0, 1}), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a_u, c_u}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_x, b_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_payload, c_payload}, JoinActionRef::AddFunction(JoinConditionOperator::Less)));
    graph.column_equivalences.add(a_u, c_u);
    graph.column_equivalences.add(a_x, b_x);
    graph.column_equivalences.add(b_x, c_x);
    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

OptimizedJoinWithActions optimizeMultiMemberEqualityCut(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x", "a_y"});
    auto b_header = makeHeader({"b_z"});
    auto c_header = makeHeader({"c_z"});
    auto right_header = makeHeader({"b_z", "c_z"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_x = actions->findNode("a_x", true);
    auto a_y = actions->findNode("a_y", true);
    auto b_z = actions->findNode("b_z", true);
    auto c_z = actions->findNode("c_z", true);
    a_x.setSourceRelations(BitSet().set(0));
    a_y.setSourceRelations(BitSet().set(0));
    b_z.setSourceRelations(BitSet().set(1));
    c_z.setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 2,
            .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 1}}, {"a_y", ColumnStats{.num_distinct_values = 2}}},
            .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_z", ColumnStats{.num_distinct_values = 1}}}, .table_name = "b"},
           {.estimated_rows = 1, .column_stats = {{"c_z", ColumnStats{.num_distinct_values = 1}}}, .table_name = "c"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {1}), a_header}, {makeProperties(b_header), b_header}, {makeProperties(c_header), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a_x, b_z}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_y, b_z}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_x, c_z}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.column_equivalences.add(a_x, b_z);
    graph.column_equivalences.add(a_y, b_z);
    graph.column_equivalences.add(a_x, c_z);
    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Chain over two independent equality classes: `a_x = b_x = c_x` and `a_y = b_y = c_y`,
/// with a large middle relation so the transitive `A-C` pair wins and both classes
/// synthesize a predicate at the same join.
OptimizedJoinWithActions optimizeTwoClassTransitiveChain(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x", "a_y"});
    auto b_header = makeHeader({"b_x", "b_y"});
    auto c_header = makeHeader({"c_x", "c_y"});
    auto right_header = makeHeader({"b_x", "b_y", "c_x", "c_y"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_x = actions->findNode("a_x", true);
    auto a_y = actions->findNode("a_y", true);
    auto b_x = actions->findNode("b_x", true);
    auto b_y = actions->findNode("b_y", true);
    auto c_x = actions->findNode("c_x", true);
    auto c_y = actions->findNode("c_y", true);
    for (auto * action : {&a_x, &a_y})
        action->setSourceRelations(BitSet().set(0));
    for (auto * action : {&b_x, &b_y})
        action->setSourceRelations(BitSet().set(1));
    for (auto * action : {&c_x, &c_y})
        action->setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 10,
            .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 10}}, {"a_y", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "a"},
           {.estimated_rows = 1000000,
            .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 10}}, {"b_y", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "b"},
           {.estimated_rows = 10,
            .column_stats = {{"c_x", ColumnStats{.num_distinct_values = 10}}, {"c_y", ColumnStats{.num_distinct_values = 10}}},
            .table_name = "c"}};
    graph.data_property_catalog
        = makeCatalog({{makeProperties(a_header), a_header}, {makeProperties(b_header), b_header}, {makeProperties(c_header), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a_x, b_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_y, b_y}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_y, c_y}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Chain whose equality predicates resolve through incompatible comparison domains:
/// `a_u` is a `UUID` and `c_e` an `Enum8`, each compared against `FixedString` `b_s`.
/// Resolvability alone must not let these predicates form a transitive equality class.
OptimizedJoinWithActions optimizeCrossDomainEqualityChain(const QueryPlanOptimizationSettings & settings, bool trusted_keys = false)
{
    tryRegisterFunctions();
    auto uuid_type = std::make_shared<DataTypeUUID>();
    auto fixed_string_type = std::make_shared<DataTypeFixedString>(36);
    auto enum_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{{"a", 1}});

    Block a_header;
    a_header.insert(ColumnWithTypeAndName(uuid_type->createColumn(), uuid_type, "a_u"));
    Block b_header;
    b_header.insert(ColumnWithTypeAndName(fixed_string_type->createColumn(), fixed_string_type, "b_s"));
    Block c_header;
    c_header.insert(ColumnWithTypeAndName(enum_type->createColumn(), enum_type, "c_e"));
    Block right_header;
    right_header.insert(b_header.getByPosition(0));
    right_header.insert(c_header.getByPosition(0));

    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_u = actions->findNode("a_u", true);
    auto b_s = actions->findNode("b_s", true);
    auto c_e = actions->findNode("c_e", true);
    a_u.setSourceRelations(BitSet().set(0));
    b_s.setSourceRelations(BitSet().set(1));
    c_e.setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 10, .column_stats = {{"a_u", ColumnStats{.num_distinct_values = 10}}}, .table_name = "a"},
           {.estimated_rows = 1000000, .column_stats = {{"b_s", ColumnStats{.num_distinct_values = 10}}}, .table_name = "b"},
           {.estimated_rows = 10, .column_stats = {{"c_e", ColumnStats{.num_distinct_values = 10}}}, .table_name = "c"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, trusted_keys ? std::vector<size_t>{0} : std::vector<size_t>{}), a_header},
         {makeProperties(b_header), b_header},
         {makeProperties(c_header, trusted_keys ? std::vector<size_t>{0} : std::vector<size_t>{}), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a_u, b_s}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_s, c_e}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Region where greedy's disconnected-pair `Cross` fallback wins the first round while a
/// canonical cap is used by the selected plan: `a` and `b` are tiny and connected only to the
/// huge keyed `c`, so `(a x b)` is picked first and `(a x b) JOIN c` gets a proven cap whose
/// equality class spans the cross pair.
OptimizedJoinWithActions optimizeCrossFallbackUnderCap(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_x"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_x"});
    auto right_header = makeHeader({"b_x", "c_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_x = actions->findNode("a_x", true);
    auto b_x = actions->findNode("b_x", true);
    auto c_x = actions->findNode("c_x", true);
    a_x.setSourceRelations(BitSet().set(0));
    b_x.setSourceRelations(BitSet().set(1));
    c_x.setSourceRelations(BitSet().set(2));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 1, .column_stats = {{"a_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "a"},
           {.estimated_rows = 1, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "b"},
           {.estimated_rows = 1000000, .column_stats = {{"c_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "c"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header), a_header}, {makeProperties(b_header), b_header}, {makeProperties(c_header, {0}), c_header}});
    graph.edges.push_back(JoinActionRef::transform({a_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({b_x, c_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

/// Four relations where the proof of `{a,b}` uniqueness against `c` relies on the class
/// `{a_x, b_x, e_x}` bridged through relation `e` outside the group: the selected cap must
/// carry that class as an obligation, and cleanup must discharge it by synthesizing
/// `a_x = b_x` inside the `{a,b}` subtree.
OptimizedJoinWithActions optimizeExternalBridgeCap(const QueryPlanOptimizationSettings & settings)
{
    tryRegisterFunctions();
    auto a_header = makeHeader({"a_k", "a_x"});
    auto b_header = makeHeader({"b_x"});
    auto c_header = makeHeader({"c_k"});
    auto e_header = makeHeader({"e_x"});
    auto right_header = makeHeader({"b_x", "c_k", "e_x"});
    auto actions = std::make_unique<JoinExpressionActions>(a_header, right_header);
    auto a_k = actions->findNode("a_k", true);
    auto a_x = actions->findNode("a_x", true);
    auto b_x = actions->findNode("b_x", true);
    auto c_k = actions->findNode("c_k", true);
    auto e_x = actions->findNode("e_x", true);
    for (auto * action : {&a_k, &a_x})
        action->setSourceRelations(BitSet().set(0));
    b_x.setSourceRelations(BitSet().set(1));
    c_k.setSourceRelations(BitSet().set(2));
    e_x.setSourceRelations(BitSet().set(3));

    QueryGraph graph;
    graph.relation_stats
        = {{.estimated_rows = 1,
            .column_stats = {{"a_k", ColumnStats{.num_distinct_values = 1}}, {"a_x", ColumnStats{.num_distinct_values = 1}}},
            .table_name = "a"},
           {.estimated_rows = 1, .column_stats = {{"b_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "b"},
           {.estimated_rows = 1000000, .column_stats = {{"c_k", ColumnStats{.num_distinct_values = 1}}}, .table_name = "c"},
           {.estimated_rows = 1000000000, .column_stats = {{"e_x", ColumnStats{.num_distinct_values = 1}}}, .table_name = "e"}};
    graph.data_property_catalog = makeCatalog(
        {{makeProperties(a_header, {0}), a_header},
         {makeProperties(b_header, {0}), b_header},
         {makeProperties(c_header), c_header},
         {makeProperties(e_header), e_header}});
    graph.edges.push_back(JoinActionRef::transform({a_k, c_k}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({a_x, e_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    graph.edges.push_back(JoinActionRef::transform({e_x, b_x}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

    auto plan = optimizeJoinOrder(std::exchange(graph, {}), settings);
    return {std::exchange(actions, {}), std::exchange(plan, {})};
}

DPJoinEntryPtr findEntry(const DPJoinEntryPtr & entry, const BitSet & relations)
{
    if (!entry || entry->relations == relations)
        return entry;
    if (const auto left = findEntry(entry->left, relations))
        return left;
    return findEntry(entry->right, relations);
}

DPJoinEntryPtr optimizeDisconnectedTwoRelationJoin(const QueryPlanOptimizationSettings & settings)
{
    auto left_header = makeHeader({"left_id"});
    auto right_header = makeHeader({"right_id"});

    QueryGraph graph;
    graph.relation_stats = {{.estimated_rows = 100, .table_name = "left"}, {.estimated_rows = 20, .table_name = "right"}};
    graph.data_property_catalog
        = makeCatalog({{makeProperties(left_header, {0}), left_header}, {makeProperties(right_header, {0}), right_header}});
    graph.canonical_property_region_rejection = JoinOrderPropertyUnsupportedReason::CrossOrCommaRegion;
    return optimizeJoinOrder(std::move(graph), settings);
}

}

TEST(JoinOrderDataProperties, CatalogStoresTypedSourceQualifiedColumnsAndTrustedAggregationKey)
{
    auto first_header = makeHeader({"id", "payload"});
    auto second_header = makeHeader({"other_id"});
    auto catalog = makeCatalog({{makeProperties(first_header, {0}), first_header}, {makeProperties(second_header), second_header}}, false);

    ASSERT_EQ(catalog->relationCount(), 2u);
    ASSERT_EQ(catalog->columnCount(), 3u);
    EXPECT_EQ(catalog->column(JoinOrderColumnId{0}).relation, 0u);
    EXPECT_EQ(catalog->column(JoinOrderColumnId{2}).relation, 1u);
    EXPECT_EQ(catalog->typeName(JoinOrderColumnId{0}), "UInt64");
    ASSERT_EQ(catalog->uniqueKeyCount(), 1u);
    EXPECT_TRUE(catalog->isTrustedUniqueKey(JoinOrderUniqueKeyId{0}));
}

TEST(JoinOrderDataProperties, StorageDeclarationIsDiagnosticOnly)
{
    auto header = makeHeader({"id"});
    auto properties = makeProperties(header, {0}, UniqueKeyEvidence::StorageDeclaration);

    auto costing_catalog = makeCatalog({{properties, header}}, false);
    EXPECT_EQ(costing_catalog->uniqueKeyCount(), 0u);

    auto diagnostic_catalog = makeCatalog({{properties, header}}, true);
    ASSERT_EQ(diagnostic_catalog->uniqueKeyCount(), 1u);
    EXPECT_FALSE(diagnostic_catalog->isTrustedUniqueKey(JoinOrderUniqueKeyId{0}));
    EXPECT_EQ(diagnostic_catalog->uniqueKey(JoinOrderUniqueKeyId{0}).provenance.confidence, DataPropertyConfidence::DiagnosticOnly);
}

TEST(JoinOrderDataProperties, SourceQualifiedPredicateBindingIsOrientationIndependent)
{
    tryRegisterFunctions();
    auto left_header = makeHeader({"left_id"});
    auto right_header = makeHeader({"right_id"});
    auto catalog = makeCatalog({{makeProperties(left_header, {0}), left_header}, {makeProperties(right_header, {0}), right_header}});

    JoinExpressionActions actions(left_header, right_header);
    auto left = actions.findNode("left_id", true);
    auto right = actions.findNode("right_id", true);
    left.setSourceRelations(BitSet().set(0));
    right.setSourceRelations(BitSet().set(1));

    auto forward = JoinActionRef::transform({left, right}, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
    auto reverse = JoinActionRef::transform({right, left}, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
    const auto forward_binding = bindJoinOrderPredicate(forward, *catalog);
    const auto reverse_binding = bindJoinOrderPredicate(reverse, *catalog);

    const auto * forward_equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&forward_binding);
    const auto * reverse_equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&reverse_binding);
    ASSERT_NE(forward_equality, nullptr);
    ASSERT_NE(reverse_equality, nullptr);
    EXPECT_EQ(forward_equality->lhs, reverse_equality->rhs);
    EXPECT_EQ(forward_equality->rhs, reverse_equality->lhs);
}

TEST(JoinOrderDataProperties, ResolvableCrossDomainEqualityBindingIsResidual)
{
    tryRegisterFunctions();
    auto uuid_type = std::make_shared<DataTypeUUID>();
    auto fixed_string_type = std::make_shared<DataTypeFixedString>(36);
    Block left_header;
    left_header.insert(ColumnWithTypeAndName(uuid_type->createColumn(), uuid_type, "left_id"));
    Block right_header;
    right_header.insert(ColumnWithTypeAndName(fixed_string_type->createColumn(), fixed_string_type, "right_id"));
    auto catalog = makeCatalog({{makeProperties(left_header), left_header}, {makeProperties(right_header), right_header}});

    JoinExpressionActions actions(left_header, right_header);
    auto left = actions.findNode("left_id", true);
    auto right = actions.findNode("right_id", true);
    left.setSourceRelations(BitSet().set(0));
    right.setSourceRelations(BitSet().set(1));
    const auto predicate = JoinActionRef::transform({left, right}, JoinActionRef::AddFunction(JoinConditionOperator::Equals));

    EXPECT_TRUE(std::holds_alternative<JoinOrderResidualPredicateBinding>(bindJoinOrderPredicate(predicate, *catalog)));
}

TEST(JoinOrderDataProperties, TypeMismatchIsClassifiedAsUnsupported)
{
    tryRegisterFunctions();
    auto catalog_left_header = makeHeader({"left_id"});
    auto catalog_right_header = makeHeader({"right_id"});
    auto catalog = makeCatalog(
        {{makeProperties(catalog_left_header, {0}), catalog_left_header},
         {makeProperties(catalog_right_header, {0}), catalog_right_header}});

    auto action_left_header = makeTypedHeader<DataTypeUInt32>({"left_id"});
    JoinExpressionActions actions(action_left_header, catalog_right_header);
    auto left = actions.findNode("left_id", true);
    auto right = actions.findNode("right_id", true);
    left.setSourceRelations(BitSet().set(0));
    right.setSourceRelations(BitSet().set(1));
    const auto predicate = JoinActionRef::transform({left, right}, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
    const auto binding = bindJoinOrderPredicate(predicate, *catalog);

    const auto * reason = getUnsupportedReason(binding);
    ASSERT_NE(reason, nullptr);
    EXPECT_EQ(*reason, JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType);
}

TEST(JoinOrderDataProperties, AllAlgorithmsConsumeCanonicalCaps)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{
        JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE, JoinOrderAlgorithm::DPSUB, JoinOrderAlgorithm::DPHYP};
    for (const auto algorithm : algorithms)
    {
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        JoinOrderOptimizationDebugInfo debug_info;
        const auto [uncapped_rows, uncapped_cost]
            = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, 100, 20, {}, &debug_info);
        EXPECT_EQ(uncapped_rows, 2000u) << toString(algorithm);
        EXPECT_DOUBLE_EQ(uncapped_cost, 2000.0) << toString(algorithm);
        expectCanonicalDebugInfoEmpty(debug_info, toString(algorithm));

        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto [capped_rows, capped_cost] = optimizeTwoRelationJoin(settings);
        EXPECT_EQ(capped_rows, 100u) << toString(algorithm);
        EXPECT_DOUBLE_EQ(capped_cost, 100.0) << toString(algorithm);
    }
}

TEST(JoinOrderDataProperties, CanonicalCapOutcomeCountersDistinguishAssessments)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    JoinOrderOptimizationDebugInfo debug_info;
    const auto disabled = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, 100, 20, {}, &debug_info);
    EXPECT_EQ(disabled.first, 2000u);
    EXPECT_DOUBLE_EQ(disabled.second, 2000.0);
    expectCanonicalDebugInfoEmpty(debug_info, "disabled");

    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto missing = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, std::nullopt, 20, {}, &debug_info);
    EXPECT_FALSE(missing.first);
    EXPECT_GE(missing.second, 0.0);
    expectCanonicalCapAssessments(debug_info, 0, 1, 0, 0, "missing input rows");

    const auto not_proven = optimizeTwoRelationJoin(settings, false, UniqueKeyEvidence::AggregationGrouping, 100, 20, {}, &debug_info);
    EXPECT_EQ(not_proven.first, 2000u);
    EXPECT_DOUBLE_EQ(not_proven.second, 2000.0);
    expectCanonicalCapAssessments(debug_info, 0, 0, 1, 0, "not proven");

    const auto unsupported = optimizeTwoRelationJoin(
        settings,
        true,
        UniqueKeyEvidence::AggregationGrouping,
        100,
        20,
        JoinOrderPropertyUnsupportedReason::CrossOrCommaRegion,
        &debug_info);
    EXPECT_EQ(unsupported.first, 2000u);
    EXPECT_DOUBLE_EQ(unsupported.second, 2000.0);
    expectCanonicalCapAssessments(debug_info, 0, 0, 0, 1, "unsupported");

    const auto proven = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, 100, 20, {}, &debug_info);
    EXPECT_EQ(proven.first, 100u);
    EXPECT_DOUBLE_EQ(proven.second, 100.0);
    expectCanonicalCapAssessments(debug_info, 1, 0, 0, 0, "proven");
}

TEST(JoinOrderDataProperties, MissingRowsBypassCanonicalLookupsForAllAlgorithms)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;

    constexpr std::array algorithms{
        JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE, JoinOrderAlgorithm::DPSUB, JoinOrderAlgorithm::DPHYP};
    for (const auto algorithm : algorithms)
    {
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        JoinOrderOptimizationDebugInfo debug_info;
        const auto [missing_left_rows, missing_left_cost]
            = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, std::nullopt, 20, {}, &debug_info);
        EXPECT_FALSE(missing_left_rows) << toString(algorithm);
        EXPECT_GE(missing_left_cost, 0.0) << toString(algorithm);
        expectCanonicalInferenceMetricsEmpty(debug_info, toString(algorithm));
        expectCanonicalCapAssessments(debug_info, 0, 1, 0, 0, toString(algorithm));

        const auto [missing_right_rows, missing_right_cost]
            = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::AggregationGrouping, 100, std::nullopt, {}, &debug_info);
        EXPECT_FALSE(missing_right_rows) << toString(algorithm);
        EXPECT_GE(missing_right_cost, 0.0) << toString(algorithm);
        expectCanonicalInferenceMetricsEmpty(debug_info, toString(algorithm));
        expectCanonicalCapAssessments(debug_info, 0, 1, 0, 0, toString(algorithm));
    }
}

TEST(JoinOrderDataProperties, AllAlgorithmsConsumeCanonicalTriangleCaps)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{
        JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE, JoinOrderAlgorithm::DPSUB, JoinOrderAlgorithm::DPHYP};
    for (const auto algorithm : algorithms)
    {
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        JoinOrderOptimizationDebugInfo debug_info;
        EXPECT_EQ(optimizeTriangleJoin(settings, &debug_info)->estimated_rows, 10000u) << toString(algorithm);
        expectCanonicalDebugInfoEmpty(debug_info, toString(algorithm));

        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        EXPECT_EQ(optimizeTriangleJoin(settings)->estimated_rows, 5u) << toString(algorithm);
    }
}

TEST(JoinOrderDataProperties, PhysicalJoinMaterializesEveryCanonicalEqualityClass)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;

    const auto optimized = optimizeCompositeTransitiveCut(settings);
    const auto & root = optimized.plan;
    const auto ac = findEntry(root, BitSet().set(0).set(2));
    ASSERT_TRUE(ac) << "test statistics must select the A-C continuation: root=" << root->dump() << ", left=" << root->left->dump()
                    << " rows=" << root->left->estimated_rows.value_or(0) << " cost=" << root->left->cost
                    << ", right=" << root->right->dump();

    bool enforces_transitive_x = false;
    bool retains_residual = false;
    for (const auto & expression : ac->join_operator.expression)
    {
        const auto [op, lhs, rhs] = expression.asBinaryPredicate();
        const std::array names{lhs.getColumnName(), rhs.getColumnName()};
        enforces_transitive_x |= op == JoinConditionOperator::Equals
            && ((names[0] == "a_x" && names[1] == "c_x") || (names[0] == "c_x" && names[1] == "a_x"));
        retains_residual |= op == JoinConditionOperator::Less;
    }
    EXPECT_TRUE(enforces_transitive_x) << ac->join_operator.dump();
    EXPECT_TRUE(retains_residual) << ac->join_operator.dump();
}

TEST(JoinOrderDataProperties, PhysicalJoinMaterializesEveryMemberUsedByCanonicalCut)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;

    constexpr std::array algorithms{
        JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE, JoinOrderAlgorithm::DPSUB, JoinOrderAlgorithm::DPHYP};
    for (const auto algorithm : algorithms)
    {
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        const auto optimized = optimizeMultiMemberEqualityCut(settings);
        const auto ac = findEntry(optimized.plan, BitSet().set(0).set(2));
        ASSERT_TRUE(ac) << toString(algorithm) << ": " << optimized.plan->dump();
        EXPECT_EQ(ac->estimated_rows, 1u) << toString(algorithm);

        bool enforces_a_x = false;
        bool enforces_a_y = false;
        for (const auto & expression : ac->join_operator.expression)
        {
            const auto [op, lhs, rhs] = expression.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;
            const std::array names{lhs.getColumnName(), rhs.getColumnName()};
            enforces_a_x |= (names[0] == "a_x" && names[1] == "c_z") || (names[0] == "c_z" && names[1] == "a_x");
            enforces_a_y |= (names[0] == "a_y" && names[1] == "c_z") || (names[0] == "c_z" && names[1] == "a_y");
        }
        EXPECT_TRUE(enforces_a_x) << toString(algorithm) << ": " << ac->join_operator.dump();
        EXPECT_TRUE(enforces_a_y) << toString(algorithm) << ": " << ac->join_operator.dump();
    }
}

TEST(JoinOrderDataProperties, StorageDeclarationCannotAffectCosting)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;

    const auto [rows, cost] = optimizeTwoRelationJoin(settings, true, UniqueKeyEvidence::StorageDeclaration);
    EXPECT_EQ(rows, 2000u);
    EXPECT_DOUBLE_EQ(cost, 2000.0);
}

TEST(JoinOrderDataProperties, IneligibleDisconnectedRegionMatchesFeatureOff)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSUB, JoinOrderAlgorithm::GREEDY};

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    auto feature_off = optimizeDisconnectedTwoRelationJoin(settings);
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    auto canonical_on = optimizeDisconnectedTwoRelationJoin(settings);

    EXPECT_EQ(feature_off->join_operator.kind, JoinKind::Cross);
    EXPECT_EQ(canonical_on->join_operator.kind, JoinKind::Cross);
    EXPECT_EQ(feature_off->estimated_rows, canonical_on->estimated_rows);
    EXPECT_DOUBLE_EQ(feature_off->cost, canonical_on->cost);
}

TEST(JoinOrderDataProperties, GreedyProoflessTransitiveChainPreservesDisconnectedCrossFallback)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven, true);
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven, true);

    expectSamePlan(feature_off.plan, canonical_on.plan);
    ASSERT_TRUE(feature_off.plan);
    EXPECT_EQ(feature_off.plan->join_operator.kind, JoinKind::Cross);
    EXPECT_EQ(canonical_on.plan->join_operator.kind, JoinKind::Cross);
}

TEST(JoinOrderDataProperties, ProoflessTransitiveCandidatesRestoreGreedyAndDPsizeFeatureOffBehavior)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE};
    constexpr std::array outcomes{
        TransitiveChainCanonicalOutcome::NotProven,
        TransitiveChainCanonicalOutcome::MissingInputRows,
        TransitiveChainCanonicalOutcome::Unsupported};
    for (const auto algorithm : algorithms)
    {
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        for (const auto outcome : outcomes)
        {
            SCOPED_TRACE(toString(algorithm));
            SCOPED_TRACE(static_cast<int>(outcome));
            settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
            const auto feature_off = optimizeTransitiveChain(settings, outcome);
            settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
            const auto canonical_on = optimizeTransitiveChain(settings, outcome);
            expectSamePlan(feature_off.plan, canonical_on.plan);
        }
    }
}

TEST(JoinOrderDataProperties, ApplicableUnaryPredicatePreservesLegacyConnectivityAndSelectivity)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE};
    for (const auto algorithm : algorithms)
    {
        SCOPED_TRACE(toString(algorithm));
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeUnaryPredicateJoin(settings);
        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto canonical_on = optimizeUnaryPredicateJoin(settings);

        expectSamePlan(feature_off.plan, canonical_on.plan);
        ASSERT_TRUE(canonical_on.plan);
        EXPECT_EQ(canonical_on.plan->join_operator.kind, JoinKind::Inner);
        EXPECT_EQ(canonical_on.plan->estimated_rows, 200u);
        EXPECT_DOUBLE_EQ(canonical_on.plan->cost, 200.0);
        ASSERT_EQ(canonical_on.plan->join_operator.expression.size(), 1u);
        const auto & predicate_sources = canonical_on.plan->join_operator.expression.front().getSourceRelations();
        EXPECT_EQ(predicate_sources, BitSet().set(0));
    }
}

TEST(JoinOrderDataProperties, LosingProvenCandidateDoesNotMaterializeSelectedProoflessPlan)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE};
    for (const auto algorithm : algorithms)
    {
        SCOPED_TRACE(toString(algorithm));
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeLosingProvenCandidate(settings);
        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        JoinOrderOptimizationDebugInfo debug_info;
        const auto canonical_on = optimizeLosingProvenCandidate(settings, false, &debug_info);

        EXPECT_GT(debug_info.cap_assessments.proven, 0);
        expectSamePlan(feature_off.plan, canonical_on.plan);
        EXPECT_FALSE(planContainsEquality(canonical_on.plan, "a_u", "c_u"));
    }

    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSIZE, JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off_fallback = optimizeLosingProvenCandidate(settings, true);
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    JoinOrderOptimizationDebugInfo debug_info;
    const auto canonical_on_fallback = optimizeLosingProvenCandidate(settings, true, &debug_info);

    EXPECT_GT(debug_info.cap_assessments.proven, 0);
    expectSamePlan(feature_off_fallback.plan, canonical_on_fallback.plan);
    ASSERT_TRUE(canonical_on_fallback.plan);
    EXPECT_EQ(canonical_on_fallback.plan->join_operator.kind, JoinKind::Cross);
    EXPECT_FALSE(planContainsEquality(canonical_on_fallback.plan, "a_u", "c_u"));
}

TEST(JoinOrderDataProperties, IndependentTransitivePredicateSettingRemainsIndependentOfCanonicalProof)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE};
    for (const auto algorithm : algorithms)
    {
        SCOPED_TRACE(toString(algorithm));
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven);
        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven);

        expectSamePlan(feature_off.plan, canonical_on.plan);
        const auto ac = findEntry(canonical_on.plan, BitSet().set(0).set(2));
        ASSERT_TRUE(ac);
        EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
        EXPECT_EQ(ac->estimated_rows, 10u);
    }
}

TEST(JoinOrderDataProperties, ProvenCanonicalAssessmentAdmitsTransitiveCandidate)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array algorithms{JoinOrderAlgorithm::GREEDY, JoinOrderAlgorithm::DPSIZE};
    for (const auto algorithm : algorithms)
    {
        SCOPED_TRACE(toString(algorithm));
        settings.query_plan_optimize_join_order_algorithm = {algorithm};
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
        EXPECT_FALSE(findEntry(feature_off.plan, BitSet().set(0).set(2)));

        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
        const auto ac = findEntry(canonical_on.plan, BitSet().set(0).set(2));
        ASSERT_TRUE(ac);
        EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
        EXPECT_EQ(ac->estimated_rows, 10u);
        EXPECT_DOUBLE_EQ(ac->cost, 10.0);
        EXPECT_FALSE(ac->join_operator.expression.empty());
    }
}

TEST(JoinOrderDataProperties, DPhypProoflessTransitiveChainPreservesFeatureOffPlan)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array outcomes{
        TransitiveChainCanonicalOutcome::NotProven,
        TransitiveChainCanonicalOutcome::MissingInputRows,
        TransitiveChainCanonicalOutcome::Unsupported};
    for (const auto outcome : outcomes)
    {
        SCOPED_TRACE(static_cast<int>(outcome));
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeTransitiveChain(settings, outcome);
        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto canonical_on = optimizeTransitiveChain(settings, outcome);

        expectSamePlan(feature_off.plan, canonical_on.plan);
        EXPECT_FALSE(findEntry(canonical_on.plan, BitSet().set(0).set(2)));
        expectInnerJoinsHaveExpressions(canonical_on.plan);
    }
}

TEST(JoinOrderDataProperties, DPhypProvenCapDoesNotCreateSyntheticTopology)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
    EXPECT_FALSE(findEntry(feature_off.plan, BitSet().set(0).set(2)));

    /// Policy A: proofs are candidate-specific and cannot authorize static hypergraph topology,
    /// so even a Proven canonical cut must not let DPhyp discover the transitive pair.
    /// Greedy and DPsize admit this candidate through their per-pair assessment instead
    /// (see `ProvenCanonicalAssessmentAdmitsTransitiveCandidate`).
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
    const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
    EXPECT_FALSE(findEntry(canonical_on.plan, BitSet().set(0).set(2)));
    expectInnerJoinsHaveExpressions(canonical_on.plan);
}

TEST(JoinOrderDataProperties, DPhypProvenSingletonEdgeDiscoversTransitiveJoin)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
    auto counters_before = getDPhypCounterSnapshot();
    const auto policy_a = optimizeDPhypMixedTransitiveChain(settings);
    const auto policy_a_counters = getDPhypCounterSnapshot() - counters_before;
    EXPECT_FALSE(findEntry(policy_a.plan, BitSet().set(0).set(2)));
    EXPECT_EQ(policy_a_counters.edge_candidates_assessed, 0u);
    EXPECT_EQ(policy_a_counters.proven_synthetic_edges, 0u);
    EXPECT_EQ(policy_a_counters.searched_plans, 24u);

    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    counters_before = getDPhypCounterSnapshot();
    const auto optimized = optimizeDPhypMixedTransitiveChain(settings);
    const auto counters = getDPhypCounterSnapshot() - counters_before;

    const auto ac = findEntry(optimized.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac) << optimized.plan->dump();
    EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
    EXPECT_EQ(ac->estimated_rows, 10u);
    EXPECT_TRUE(ac->used_canonical_cap);
    /// An exact singleton proof has no equality links internal to either singleton,
    /// so its obligation ledger is present but empty; the synthesized A-C cut below
    /// is still audited by selected-plan finalization.
    EXPECT_EQ(ac->canonical_cap_obligations, 0u);
    EXPECT_TRUE(planContainsEquality(optimized.plan, "a_x", "c_x"));
    EXPECT_FALSE(ac->join_operator.expression.empty());
    expectInnerJoinsHaveExpressions(optimized.plan);

    EXPECT_EQ(counters.explicit_hyperedges, 4u);
    EXPECT_EQ(counters.edge_candidates_assessed, 3u);
    EXPECT_EQ(counters.edge_candidates_rejected, 2u);
    EXPECT_EQ(counters.proven_synthetic_edges, 1u);
    EXPECT_EQ(counters.candidates_admitted, 14u);
    EXPECT_EQ(counters.candidates_rejected, 1u);
    EXPECT_EQ(counters.searched_plans, 33u);
    EXPECT_EQ(counters.searched_plans - policy_a_counters.searched_plans, 9u);
    EXPECT_EQ(counters.fallbacks, 0u);
}

TEST(JoinOrderDataProperties, DPhypProvenTopologyCarriesAndDischargesCompositeObligations)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    const auto counters_before = getDPhypCounterSnapshot();
    const auto optimized = optimizeExternalBridgeCap(settings);
    const auto counters = getDPhypCounterSnapshot() - counters_before;

    const auto ab = findEntry(optimized.plan, BitSet().set(0).set(1));
    ASSERT_TRUE(ab) << optimized.plan->dump();
    EXPECT_TRUE(ab->used_canonical_cap);
    EXPECT_TRUE(planContainsEquality(optimized.plan, "a_x", "b_x"));

    const auto abc = findEntry(optimized.plan, BitSet().set(0).set(1).set(2));
    ASSERT_TRUE(abc) << optimized.plan->dump();
    EXPECT_TRUE(abc->used_canonical_cap);
    EXPECT_NE(abc->canonical_cap_obligations, 0u);
    EXPECT_EQ(counters.edge_candidates_assessed, 1u);
    EXPECT_EQ(counters.edge_candidates_rejected, 0u);
    EXPECT_EQ(counters.proven_synthetic_edges, 1u);
    expectInnerJoinsHaveExpressions(optimized.plan);
}

TEST(JoinOrderDataProperties, DPhypProvenEdgeSearchBudgetTriggersConfiguredFallback)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP, JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_max_searched_plans = 24;
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
    auto counters_before = getDPhypCounterSnapshot();
    const auto policy_a = optimizeDPhypMixedTransitiveChain(settings);
    const auto policy_a_counters = getDPhypCounterSnapshot() - counters_before;
    EXPECT_FALSE(findEntry(policy_a.plan, BitSet().set(0).set(2)));
    EXPECT_EQ(policy_a_counters.searched_plans, 24u);
    EXPECT_EQ(policy_a_counters.fallbacks, 0u);

    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    counters_before = getDPhypCounterSnapshot();
    const auto fallback = optimizeDPhypMixedTransitiveChain(settings);
    const auto fallback_counters = getDPhypCounterSnapshot() - counters_before;
    EXPECT_EQ(fallback_counters.searched_plans, 25u); /// exhaustion is deterministically max + 1
    EXPECT_EQ(fallback_counters.fallbacks, 1u);
    EXPECT_EQ(fallback_counters.proven_synthetic_edges, 1u);
    const auto ac = findEntry(fallback.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac) << fallback.plan->dump();
    EXPECT_TRUE(ac->used_canonical_cap);
    EXPECT_TRUE(planContainsEquality(fallback.plan, "a_x", "c_x"));
    expectInnerJoinsHaveExpressions(fallback.plan);

    /// A failed last DPhyp attempt is an error, not a fallback to another algorithm.
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_max_searched_plans = 1;
    counters_before = getDPhypCounterSnapshot();
    EXPECT_THROW(optimizeDPhypMixedTransitiveChain(settings), Exception);
    const auto no_successor_counters = getDPhypCounterSnapshot() - counters_before;
    EXPECT_EQ(no_successor_counters.searched_plans, 2u);
    EXPECT_EQ(no_successor_counters.fallbacks, 0u);
}

TEST(JoinOrderDataProperties, DPhypProvenEdgesFailClosedWithoutInstallableSingletonProof)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array outcomes{
        TransitiveChainCanonicalOutcome::NotProven,
        TransitiveChainCanonicalOutcome::MissingInputRows,
        TransitiveChainCanonicalOutcome::Unsupported};
    for (const auto outcome : outcomes)
    {
        SCOPED_TRACE(static_cast<int>(outcome));
        settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
        auto counters_before = getDPhypCounterSnapshot();
        const auto policy_a = optimizeTransitiveChain(settings, outcome);
        const auto policy_a_counters = getDPhypCounterSnapshot() - counters_before;

        settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
        counters_before = getDPhypCounterSnapshot();
        JoinOrderOptimizationDebugInfo debug_info;
        const auto proof_gated = optimizeTransitiveChain(settings, outcome, false, &debug_info);
        const auto proof_gated_counters = getDPhypCounterSnapshot() - counters_before;

        expectSamePlan(policy_a.plan, proof_gated.plan);
        EXPECT_FALSE(findEntry(proof_gated.plan, BitSet().set(0).set(2)));
        expectInnerJoinsHaveExpressions(proof_gated.plan);
        EXPECT_EQ(policy_a_counters.explicit_hyperedges, 2u);
        EXPECT_EQ(proof_gated_counters.explicit_hyperedges, 2u);
        EXPECT_EQ(policy_a_counters.edge_candidates_assessed, 0u);
        EXPECT_EQ(proof_gated_counters.edge_candidates_assessed, 1u);
        EXPECT_EQ(proof_gated_counters.edge_candidates_rejected, 1u);
        EXPECT_EQ(proof_gated_counters.proven_synthetic_edges, 0u);
        EXPECT_EQ(proof_gated_counters.searched_plans, policy_a_counters.searched_plans);
        EXPECT_EQ(proof_gated_counters.fallbacks, policy_a_counters.fallbacks);
        if (outcome == TransitiveChainCanonicalOutcome::NotProven)
            EXPECT_GT(debug_info.cap_assessments.not_proven, 0);
        else if (outcome == TransitiveChainCanonicalOutcome::MissingInputRows)
            EXPECT_GT(debug_info.cap_assessments.missing_input_rows, 0);
        else if (outcome == TransitiveChainCanonicalOutcome::Unsupported)
            EXPECT_GT(debug_info.cap_assessments.unsupported, 0);
    }
}

TEST(JoinOrderDataProperties, DPhypCompositeOnlyProofDoesNotCreateSingletonTopology)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
    auto counters_before = getDPhypCounterSnapshot();
    const auto policy_a = optimizeCompositeOnlyTransitiveTopology(settings);
    const auto policy_a_counters = getDPhypCounterSnapshot() - counters_before;

    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    counters_before = getDPhypCounterSnapshot();
    const auto proof_gated = optimizeCompositeOnlyTransitiveTopology(settings);
    const auto proof_gated_counters = getDPhypCounterSnapshot() - counters_before;

    const BitSet acd = BitSet().set(0).set(2).set(3);
    expectSamePlan(policy_a.plan, proof_gated.plan);
    EXPECT_FALSE(findEntry(proof_gated.plan, acd));
    EXPECT_EQ(proof_gated_counters.edge_candidates_assessed, 2u);
    EXPECT_EQ(proof_gated_counters.edge_candidates_rejected, 2u);
    EXPECT_EQ(proof_gated_counters.proven_synthetic_edges, 0u);
    EXPECT_EQ(proof_gated_counters.searched_plans, policy_a_counters.searched_plans);
    EXPECT_EQ(proof_gated_counters.fallbacks, 0u);

    /// DPsize assesses the composite A-{C,D} split dynamically and proves the fixture's
    /// intended cap; DPhyp's singleton-only topology intentionally cannot discover it.
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSIZE};
    const auto dpsize = optimizeCompositeOnlyTransitiveTopology(settings);
    const auto dpsize_acd = findEntry(dpsize.plan, acd);
    ASSERT_TRUE(dpsize_acd) << dpsize.plan->dump();
    EXPECT_TRUE(dpsize_acd->used_canonical_cap);
    EXPECT_EQ(dpsize_acd->canonical_cap_obligations, 0u);
    EXPECT_TRUE(planContainsEquality(dpsize.plan, "a_x", "c_x"));
    EXPECT_TRUE(planContainsEquality(dpsize.plan, "a_y", "d_y"));
    expectInnerJoinsHaveExpressions(dpsize.plan);
}

TEST(JoinOrderDataProperties, DPhypCrossDomainEqualitiesDoNotCreateTransitiveTopology)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_dphyp_proven_edges = false;
    const auto policy_a = optimizeCrossDomainEqualityChain(settings, /*trusted_keys=*/true);
    settings.query_plan_optimize_join_order_dphyp_proven_edges = true;
    const auto counters_before = getDPhypCounterSnapshot();
    const auto proof_gated = optimizeCrossDomainEqualityChain(settings, /*trusted_keys=*/true);
    const auto counters = getDPhypCounterSnapshot() - counters_before;

    expectSamePlan(policy_a.plan, proof_gated.plan);
    EXPECT_FALSE(findEntry(proof_gated.plan, BitSet().set(0).set(2)));
    EXPECT_EQ(counters.edge_candidates_assessed, 0u);
    EXPECT_EQ(counters.edge_candidates_rejected, 0u);
    EXPECT_EQ(counters.proven_synthetic_edges, 0u);
    EXPECT_EQ(counters.candidates_rejected, 0u);
    EXPECT_EQ(counters.fallbacks, 0u);
    expectInnerJoinsHaveExpressions(proof_gated.plan);
}

TEST(JoinOrderDataProperties, DPhypIndependentTransitiveSettingStillDiscoversSyntheticTopology)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = true;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPHYP};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven);
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::NotProven);

    expectSamePlan(feature_off.plan, canonical_on.plan);
    const auto ac = findEntry(canonical_on.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac);
    EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
    EXPECT_EQ(ac->estimated_rows, 10u);
    EXPECT_FALSE(ac->join_operator.expression.empty());
    expectInnerJoinsHaveExpressions(canonical_on.plan);
}

TEST(JoinOrderDataProperties, DPsubProoflessTransitiveChainPreservesFeatureOffBehavior)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSUB};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    constexpr std::array outcomes{
        TransitiveChainCanonicalOutcome::NotProven,
        TransitiveChainCanonicalOutcome::MissingInputRows,
        TransitiveChainCanonicalOutcome::Unsupported};
    for (const auto outcome : outcomes)
    {
        SCOPED_TRACE(static_cast<int>(outcome));
        settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
        const auto feature_off = optimizeTransitiveChain(settings, outcome);
        settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
        const auto canonical_on = optimizeTransitiveChain(settings, outcome);

        expectSamePlan(feature_off.plan, canonical_on.plan);
        EXPECT_FALSE(findEntry(canonical_on.plan, BitSet().set(0).set(2)));
        expectInnerJoinsHaveExpressions(canonical_on.plan);
    }
}

TEST(JoinOrderDataProperties, DPsubProvenCanonicalAssessmentAdmitsTransitiveCandidate)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSUB};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
    EXPECT_FALSE(findEntry(feature_off.plan, BitSet().set(0).set(2)));

    /// DPsub assesses transitive pairs per candidate during enumeration, so unlike DPhyp
    /// (static topology, see `DPhypProvenCapDoesNotCreateSyntheticTopology`) a `Proven`
    /// canonical assessment admits the pair, matching greedy and DPsize.
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto canonical_on = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
    const auto ac = findEntry(canonical_on.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac);
    EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
    EXPECT_EQ(ac->estimated_rows, 10u);
    EXPECT_FALSE(ac->join_operator.expression.empty());
    expectInnerJoinsHaveExpressions(canonical_on.plan);
}

TEST(JoinOrderDataProperties, DPsubIndependentTransitiveAdmissionStillUsesProvenCanonicalCap)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = true;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::DPSUB};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    /// Independent transitive admission must not make the token's default/disabled cap look
    /// pre-assessed. DPsub still has to perform the canonical lookup and apply the proven cap.
    const auto optimized = optimizeTransitiveChain(settings, TransitiveChainCanonicalOutcome::Proven);
    const auto ac = findEntry(optimized.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac);
    EXPECT_EQ(ac->join_operator.kind, JoinKind::Inner);
    EXPECT_EQ(ac->estimated_rows, 10u);
    EXPECT_TRUE(ac->used_canonical_cap);
    EXPECT_FALSE(ac->join_operator.expression.empty());
    expectInnerJoinsHaveExpressions(optimized.plan);
}

TEST(JoinOrderDataProperties, SynthesizedTransitivePredicateOrderIsDeterministic)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = true;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    /// Two equality classes synthesize predicates at the same transitive `A-C` join. The
    /// classes live in a pointer-hashed map, so without deterministic ordering the ON-clause
    /// order would vary run to run, destabilizing `EXPLAIN` output and join fingerprints.
    const auto optimized = optimizeTwoClassTransitiveChain(settings);
    const auto ac = findEntry(optimized.plan, BitSet().set(0).set(2));
    ASSERT_TRUE(ac) << optimized.plan->dump();

    std::vector<std::pair<String, String>> synthesized;
    for (const auto & expression : ac->join_operator.expression)
    {
        const auto [op, lhs, rhs] = expression.asBinaryPredicate();
        ASSERT_EQ(op, JoinConditionOperator::Equals);
        synthesized.emplace_back(lhs.getColumnName(), rhs.getColumnName());
    }
    const std::vector<std::pair<String, String>> expected{{"a_x", "c_x"}, {"a_y", "c_y"}};
    EXPECT_EQ(synthesized, expected);
}

TEST(JoinOrderDataProperties, ResolvableCrossDomainEqualitiesAreNotComposedTransitively)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.enable_join_transitive_predicates = false;
    const auto transitive_off = optimizeCrossDomainEqualityChain(settings);
    settings.enable_join_transitive_predicates = true;
    const auto transitive_on = optimizeCrossDomainEqualityChain(settings);

    expectSamePlan(transitive_off.plan, transitive_on.plan);
    EXPECT_FALSE(findEntry(transitive_on.plan, BitSet().set(0).set(2)));
    EXPECT_TRUE(planContainsEquality(transitive_on.plan, "a_u", "b_s"));
    EXPECT_TRUE(planContainsEquality(transitive_on.plan, "b_s", "c_e"));
    expectInnerJoinsHaveExpressions(transitive_on.plan);
}

TEST(JoinOrderDataProperties, ProvenCapEnforcesEqualityAtGreedyCrossFallback)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    /// Feature off: the greedy fallback keeps the cross pair, and no cleanup runs.
    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off = optimizeCrossFallbackUnderCap(settings);
    const auto cross_entry = findEntry(feature_off.plan, BitSet().set(0).set(1));
    ASSERT_TRUE(cross_entry) << feature_off.plan->dump();
    EXPECT_EQ(cross_entry->join_operator.kind, JoinKind::Cross);
    EXPECT_TRUE(cross_entry->join_operator.expression.empty());

    /// With a proven cap in the selected plan, the cap's equality class spans the cross
    /// fallback pair; the cut must be enforced there, so the entry gains the synthesized
    /// predicate and becomes an inner join.
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto canonical_on = optimizeCrossFallbackUnderCap(settings);
    const auto converted_entry = findEntry(canonical_on.plan, BitSet().set(0).set(1));
    ASSERT_TRUE(converted_entry) << canonical_on.plan->dump();
    EXPECT_EQ(converted_entry->join_operator.kind, JoinKind::Inner);
    EXPECT_TRUE(planContainsEquality(canonical_on.plan, "a_x", "b_x"));
    expectInnerJoinsHaveExpressions(canonical_on.plan);
}

TEST(JoinOrderDataProperties, CrossDomainEqualitiesCannotSeedCanonicalProof)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    settings.query_plan_optimize_join_order_use_proven_uniqueness = false;
    const auto feature_off = optimizeCrossDomainEqualityChain(settings, /*trusted_keys=*/true);
    EXPECT_FALSE(findEntry(feature_off.plan, BitSet().set(0).set(2)));

    /// Both sides expose proven keys, but neither original equality has a valid
    /// transitive comparison domain, so no canonical class or cap may use the chain.
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    const auto canonical_on = optimizeCrossDomainEqualityChain(settings, /*trusted_keys=*/true);
    expectSamePlan(feature_off.plan, canonical_on.plan);
    EXPECT_FALSE(findEntry(canonical_on.plan, BitSet().set(0).set(2)));
    expectInnerJoinsHaveExpressions(canonical_on.plan);
}

TEST(JoinOrderDataProperties, ExternalBridgeObligationsReachSelectedEntriesAndAreDischarged)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.enable_join_transitive_predicates = false;
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;
    settings.query_plan_optimize_join_order_data_property_diagnostics = false;

    const auto optimized = optimizeExternalBridgeCap(settings);
    ASSERT_TRUE(optimized.plan);

    /// The `({a,b}, c)` join used a cap whose proof fired the class bridged through `e`
    /// (provider class index 1), so the entry must record that obligation.
    const auto abc = findEntry(optimized.plan, BitSet().set(0).set(1).set(2));
    ASSERT_TRUE(abc) << optimized.plan->dump();
    EXPECT_TRUE(abc->used_canonical_cap);
    EXPECT_EQ(abc->canonical_cap_obligations, UInt64{1} << 1);

    /// Cleanup discharged the obligation: the `{a,b}` subtree enforces `a_x = b_x`.
    /// `verifySelectedPlanCapRequirements` ran inside `optimizeJoinOrder`; a violation
    /// aborts debug builds via `chassert` and logs an error in release (the plan itself
    /// stays correct, only its cap-based estimate would be optimistic).
    const auto ab = findEntry(optimized.plan, BitSet().set(0).set(1));
    ASSERT_TRUE(ab) << optimized.plan->dump();
    EXPECT_EQ(ab->join_operator.kind, JoinKind::Inner);
    EXPECT_TRUE(planContainsEquality(optimized.plan, "a_x", "b_x"));
    expectInnerJoinsHaveExpressions(optimized.plan);
}

TEST(JoinOrderDataProperties, DiagnosticsDoNotChangeCanonicalPlan)
{
    Settings source_settings;
    QueryPlanOptimizationSettings settings(source_settings, 0, {}, ExpressionActionsSettings{}, {}, false);
    settings.query_plan_optimize_join_order_algorithm = {JoinOrderAlgorithm::GREEDY};
    settings.query_plan_optimize_join_order_use_proven_uniqueness = true;

    settings.query_plan_optimize_join_order_data_property_diagnostics = false;
    const auto diagnostics_off = optimizeTwoRelationJoin(settings);
    settings.query_plan_optimize_join_order_data_property_diagnostics = true;
    const auto diagnostics_on = optimizeTwoRelationJoin(settings);

    EXPECT_EQ(diagnostics_off, diagnostics_on);
}
