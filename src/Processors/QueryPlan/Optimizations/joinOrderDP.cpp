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
    const JoinOrderPropertyContext & properties,
    PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    std::vector<JoinActionRef *> & predicates,
    const JoinOrderPropertyContext::JoinCandidateAssessment & assessment,
    LoggerPtr log)
{
    /// Equivalence-derived selectivity requires the independent transitive setting or a proven
    /// canonical cut; a proofless candidate must receive the exact feature-off selectivity.
    auto selectivity = assessment.equivalence_selectivity_allowed
        ? computeSelectivity(query_graph, dp_table, expression_selectivity, predicates, left->relations, right->relations)
        : computeSelectivity(query_graph, dp_table, expression_selectivity, predicates);

    /// Transitively connected pairs are inner joins; their predicate is synthesized later.
    auto effective_kind = (assessment.connected() && join_kind == JoinKind::Cross) ? JoinKind::Inner : join_kind;
    auto estimate = properties.estimateCardinality(
        left->estimated_rows, right->estimated_rows, selectivity, effective_kind, JoinStrictness::All, assessment.canonical_cap);
    auto new_cost = computeJoinCost(left, right, selectivity, estimate.upper_bound);

    const BitSet combined_rels = left->relations | right->relations;
    auto current_best = dp_table.find(combined_rels);
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
        return nullptr;

    JoinOperator join_operator(
        effective_kind, JoinStrictness::All, JoinLocality::Unspecified,
        std::ranges::to<std::vector>(predicates | std::views::transform([](const auto * p) { return *p; })));
    auto new_entry = std::make_shared<DPJoinEntry>(left, right, new_cost, selectivity, estimate.rows, std::move(join_operator));
    new_entry->used_canonical_cap = estimate.upper_bound.has_value();
    const auto * proven_cap = getProvenCap(assessment.canonical_cap);
    new_entry->canonical_cap_obligations = proven_cap ? proven_cap->obligation_classes : 0;

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

}
