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

bool hasEquiConnection(const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right)
{
    for (const auto * edge : edges)
    {
        auto [op, lhs, rhs] = edge->asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        const auto & sources = edge->getSourceRelations();
        if ((sources & left) && (sources & right))
            return true;
    }
    return false;
}

/// Whether a join of `left_relations` with the rest splits the relations of `entry` into other sets than
/// `entry` does; a join with the same two sets swapped is the same split.
static bool splitsDifferently(const DPJoinEntry & entry, const BitSet & left_relations)
{
    if (entry.isLeaf())
        return true;
    return !(left_relations == entry.left->relations) && !(left_relations == entry.right->relations);
}

static DPJoinEntryPtr makeJoinEntry(
    const QueryGraph & query_graph,
    const JoinKeyEstimate & keys,
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    const std::vector<JoinActionRef *> & predicates,
    double cost)
{
    /// Transitively connected pairs are inner joins; their predicate is synthesized later.
    bool connected = !predicates.empty()
        || query_graph.areTransitivelyConnected(left->relations, right->relations);
    auto effective_kind = (connected && join_kind == JoinKind::Cross) ? JoinKind::Inner : join_kind;
    auto cardinality = estimateJoinCardinality(left, right, keys, effective_kind, query_graph.join_strictness);
    JoinOperator join_operator(
        effective_kind, query_graph.join_strictness, JoinLocality::Unspecified,
        std::ranges::to<std::vector>(predicates | std::views::transform([](const auto * p) { return *p; })));
    auto entry = std::make_shared<DPJoinEntry>(left, right, cost, keys.selectivity, cardinality, std::move(join_operator));
    entry->estimate_from_defaults |= assumesIndependentKeys(keys, left->estimated_rows, right->estimated_rows);
    return entry;
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
    auto keys = computeSelectivity(query_graph, dp_table, expression_selectivity, predicates, left->relations, right->relations);
    auto new_cost = computeJoinCost(query_graph, left, right, keys, join_kind, query_graph.join_strictness);

    const BitSet combined_rels = left->relations | right->relations;
    auto current_best = dp_table.find(combined_rels);
    const bool track_runner_up = query_graph.keep_runner_up && combined_rels.count() == query_graph.relation_stats.size();
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
    {
        if (track_runner_up && splitsDifferently(*current_best->second, left->relations)
            && (!query_graph.full_set_runner_up || new_cost < query_graph.full_set_runner_up->cost))
            query_graph.full_set_runner_up = makeJoinEntry(query_graph, keys, left, right, join_kind, predicates, new_cost);
        return nullptr;
    }

    auto new_entry = makeJoinEntry(query_graph, keys, left, right, join_kind, predicates, new_cost);

    if (track_runner_up)
    {
        /// The runner-up must split the relations differently from the best plan, and the replaced best
        /// plan becomes the runner-up when it does and is cheaper than the runner-up so far.
        auto & runner_up = query_graph.full_set_runner_up;
        if (runner_up && !splitsDifferently(*new_entry, runner_up->left->relations))
            runner_up = nullptr;
        if (current_best != dp_table.end())
        {
            const auto & replaced = current_best->second;
            if (splitsDifferently(*new_entry, replaced->left->relations) && (!runner_up || replaced->cost < runner_up->cost))
                runner_up = replaced;
        }
    }

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

}
