#include <Processors/QueryPlan/Optimizations/joinOrderDP.h>

#include <Interpreters/JoinOperator.h>
#include <IO/Operators.h>

#include <functional>
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
    entry->estimate_from_defaults |= joinRowsRestOnAssumption(keys, left->estimated_rows, right->estimated_rows);
    return entry;
}

/// Keeps in `QueryGraph::full_set_alternatives` the cheapest plans over all relations, one per way to split
/// them at the top join, at most one more than `max_join_order_alternatives` (the best plan is among them).
/// `make_entry` builds the plan only when it is kept.
static void keepFullSetAlternative(
    const QueryGraph & query_graph, const BitSet & left_relations, double cost, const std::function<DPJoinEntryPtr()> & make_entry)
{
    auto & kept = query_graph.full_set_alternatives;
    auto same_split = std::ranges::find_if(kept, [&](const auto & entry) { return !splitsDifferently(*entry, left_relations); });
    if (same_split != kept.end())
    {
        if (cost >= (*same_split)->cost)
            return;
        kept.erase(same_split);
    }
    else if (kept.size() > query_graph.max_join_order_alternatives && cost >= kept.back()->cost)
        return;

    auto position = std::ranges::upper_bound(kept, cost, {}, [](const auto & entry) { return entry->cost; });
    kept.insert(position, make_entry());
    if (kept.size() > query_graph.max_join_order_alternatives + 1)
        kept.pop_back();
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
    const bool track_alternatives = query_graph.max_join_order_alternatives > 0
        && combined_rels.count() == query_graph.relation_stats.size();
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
    {
        if (track_alternatives)
            keepFullSetAlternative(query_graph, left->relations, new_cost,
                [&] { return makeJoinEntry(query_graph, keys, left, right, join_kind, predicates, new_cost); });
        return nullptr;
    }

    auto new_entry = makeJoinEntry(query_graph, keys, left, right, join_kind, predicates, new_cost);
    if (track_alternatives)
        keepFullSetAlternative(query_graph, left->relations, new_cost, [&] { return new_entry; });

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

}
