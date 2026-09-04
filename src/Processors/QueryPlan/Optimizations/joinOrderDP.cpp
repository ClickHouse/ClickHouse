#include <Processors/QueryPlan/Optimizations/joinOrderDP.h>

#include <Interpreters/JoinOperator.h>
#include <IO/Operators.h>

#include <ranges>

namespace DB
{

/// Checks if predicate has sources from both left and right sets
bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right)
{
    const auto & participating = predicate->getSourceRelations();
    return areIntersecting(participating, left) && areIntersecting(participating, right);
}

DPJoinEntryPtr evaluateJoin(
    const QueryGraph & query_graph,
    PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    std::vector<JoinActionRef *> & predicates,
    LoggerPtr log)
{
    auto selectivity = computeSelectivity(query_graph, dp_table, expression_selectivity, predicates, left->relations, right->relations);
    auto new_cost = computeJoinCost(left, right, selectivity);

    const BitSet combined_rels = left->relations | right->relations;
    auto current_best = dp_table.find(combined_rels);
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
        return nullptr;

    /// Transitively connected pairs are inner joins; their predicate is synthesized later.
    bool connected = !predicates.empty()
        || query_graph.areTransitivelyConnected(left->relations, right->relations);
    auto effective_kind = (connected && join_kind == JoinKind::Cross) ? JoinKind::Inner : join_kind;
    auto cardinality = estimateJoinCardinality(left, right, selectivity, effective_kind);
    JoinOperator join_operator(
        effective_kind, JoinStrictness::All, JoinLocality::Unspecified,
        std::ranges::to<std::vector>(predicates | std::views::transform([](const auto * p) { return *p; })));
    auto new_entry = std::make_shared<DPJoinEntry>(left, right, new_cost, cardinality, std::move(join_operator));

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

}
