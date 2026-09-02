#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>

#include <Common/Exception.h>
#include <IO/Operators.h>
#include <Interpreters/JoinOperator.h>

#include <algorithm>
#include <deque>
#include <ranges>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

class GreedyJoinOrderOptimizer
{
public:
    explicit GreedyJoinOrderOptimizer(QueryGraph & query_graph_)
        : query_graph(query_graph_)
    {
    }

    DPJoinEntryPtr solve();

private:
    QueryGraph & query_graph;
    SelectivityCache expression_selectivity;
    PlanMemo dp_table;
    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
};

DPJoinEntryPtr GreedyJoinOrderOptimizer::solve()
{
    /// Discard any partial state left by an earlier algorithm in the fallback chain
    /// (e.g. `dphyp,greedy`) so cost-model lookups via `getColumnStats` only see
    /// entries built by this run. `expression_selectivity` is cleared along with
    /// `dp_table` because multi-relation predicates resolve NDV through it.
    dp_table.clear();
    expression_selectivity.clear();

    std::deque<std::shared_ptr<DPJoinEntry>> components;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        components.push_back(std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats));
    }

    std::vector<JoinActionRef *> applied_edges;
    /// Iteratively join components until we have a single plan
    while (components.size() > 1)
    {
        std::shared_ptr<DPJoinEntry> best_plan = nullptr;
        size_t best_i = 0;
        size_t best_j = 0;

        /// Try all pairs of components
        for (size_t i = 0; i < components.size(); i++)
        {
            for (size_t j = i + 1; j < components.size(); j++)
            {
                auto left = components[i];
                auto right = components[j];

                auto join_kind = isValidJoinOrder(query_graph, left->relations, right->relations);
                if (!join_kind)
                    continue;

                auto edges = getApplicableExpressions(query_graph, left->relations, right->relations);
                bool connected = !edges.empty()
                    || query_graph.areTransitivelyConnected(left->relations, right->relations);
                if (!connected && best_plan)
                    continue;

                auto selectivity = computeSelectivity(query_graph, dp_table, expression_selectivity, edges, left->relations, right->relations);
                auto current_cost = computeJoinCost(left, right, selectivity);
                if (!best_plan || current_cost < best_plan->cost)
                {
                    if (join_kind == JoinKind::Inner && !connected)
                        join_kind = JoinKind::Cross;
                    auto cardinality = estimateJoinCardinality(left, right, selectivity, join_kind.value());
                    JoinOperator join_operator(join_kind.value(), JoinStrictness::All, JoinLocality::Unspecified);
                    bool is_inner_step = isInner(join_kind.value()) || isCrossOrComma(join_kind.value());
                    for (const auto * e : edges)
                    {
                        /// A filter predicate applied at an outer join step must not go to the
                        /// ON clause, where it would affect matching instead of filtering and
                        /// let non-matching rows of the preserved side survive NULL-extended.
                        /// Apply it after the join instead.
                        if (is_inner_step || query_graph.outer_join_conditions.contains(*e))
                            join_operator.expression.push_back(*e);
                        else
                            join_operator.residual_filter.push_back(*e);
                    }
                    applied_edges = std::move(edges);
                    best_plan = std::make_shared<DPJoinEntry>(left, right, current_cost, cardinality, std::move(join_operator));
                    best_i = i;
                    best_j = j;
                }
            }
        }

        /// The loop above accepts any pair passing isValidJoinOrder, even an unconnected
        /// one (which becomes a cross product), as long as no best plan exists yet. So
        /// reaching this point means no pair of components can be joined at all: the
        /// outer join restrictions are stuck. This cannot happen for a query graph built
        /// from a well-formed join tree: required partner sets follow the original tree's
        /// scoping, so the original join order always remains valid.
        if (!best_plan)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No valid join pair found among components [{}], the outer join restrictions cannot be satisfied",
                fmt::join(components | std::views::transform([](const auto & c) { return c->dump(); }), ", "));

        LOG_TEST(log, "Best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, join operator: {}",
            best_plan->dump(), best_plan->left->dump(), best_plan->right->dump(),
            best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown",
            best_plan->join_operator.dump());

        /// replace the two components with the best plan
        components.erase(components.begin() + std::max(best_i, best_j));
        components.erase(components.begin() + std::min(best_i, best_j));
        components.push_front(best_plan);
        dp_table[best_plan->relations] = best_plan;

        for (auto * edge : applied_edges)
            *edge = nullptr;
    }

    for (auto * edge : applied_edges)
        *edge = nullptr;

    auto non_applied_edges = std::views::filter(query_graph.edges, [](auto & edge) { return bool(edge); });
    if (!non_applied_edges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some expressions was not applied: [{}]",
            fmt::join(non_applied_edges | std::views::take(5) | std::views::transform(&JoinActionRef::dump), ", "));

    return components.at(0);
}

}

DPJoinEntryPtr solveGreedyJoinOrder(QueryGraph & query_graph)
{
    return GreedyJoinOrderOptimizer(query_graph).solve();
}

}
