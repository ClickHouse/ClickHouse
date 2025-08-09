#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <deque>
#include <functional>
#include <limits>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <base/defines.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <ranges>
#include <stack>
#include <unordered_map>
#include <vector>


namespace ProfileEvents
{
    extern const Event JoinReorderMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DPJoinEntry::DPJoinEntry(size_t id, std::optional<UInt64> rows)
    : relations()
    , cost(0.0)
    , estimated_rows(rows)
    , relation_id(id)
{
    relations.set(id);
}

DPJoinEntry::DPJoinEntry(DPJoinEntryPtr lhs,
        DPJoinEntryPtr rhs,
        double cost_,
        std::optional<UInt64> cardinality_,
        JoinOperator join_operator_,
        JoinMethod join_method_)
    : relations(lhs->relations | rhs->relations)
    , left(std::move(lhs))
    , right(std::move(rhs))
    , cost(cost_)
    , estimated_rows(cardinality_)
    , join_operator(std::move(join_operator_))
    , join_method(join_method_)
{
}

bool DPJoinEntry::isLeaf() const { return !left && !right; }

String DPJoinEntry::dump() const
{
    if (isLeaf())
        return fmt::format("Leaf({})", relation_id);
    return fmt::format("Join({})", toString(relations));
}

class JoinOrderOptimizer
{
public:
    explicit JoinOrderOptimizer(QueryGraph query_graph_)
        : query_graph(std::move(query_graph_))
    {
    }

    std::shared_ptr<DPJoinEntry> solve();
private:
    void buildQueryGraph();

    std::shared_ptr<DPJoinEntry> solveDP();
    std::shared_ptr<DPJoinEntry> solveGreedy();

    std::optional<JoinKind> isValidJoinOrder(const BitSet & lhs, const BitSet & rhs) const;
    std::vector<JoinActionRef *> getApplicableExpressions(const BitSet & left, const BitSet & right);

    double computeSelectivity(const JoinActionRef & edge);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges);
    size_t getColumnStats(BitSet rels, const String & column_name);

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, bool> applied;
    std::unordered_map<JoinActionRef, double> expression_selectivity;
    std::unordered_map<BitSet, DPJoinEntryPtr> dp_table;

    LoggerPtr log = getLogger("JoinOrderOptimizer");
};


size_t JoinOrderOptimizer::getColumnStats(BitSet rels, const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    if (rels.count() != 1)
    {
        /// Assume all keys are distinct
        if (auto it = dp_table.find(rels); it != dp_table.end())
            return it->second->estimated_rows.value_or(0);
        return 0;
    }

    auto rel_id = rels.findFirstSet();
    if (rel_id < 0 || relation_stats.size() <= static_cast<size_t>(rel_id))
        return 0;

    const auto & relation_stat = relation_stats.at(rel_id);
    const auto & column_stats = relation_stat.column_stats;
    if (auto it = column_stats.find(column_name); it != column_stats.end())
        return it->second.num_distinct_values;
    return relation_stat.estimated_rows.value_or(0);
}

double JoinOrderOptimizer::computeSelectivity(const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge, 1.0);
    auto & selectivity = it->second;
    if (!inserted)
        return selectivity;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return 0.0;

    UInt64 lhs_ndv = getColumnStats(lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / max_ndv);
    return selectivity;
}

double JoinOrderOptimizer::computeSelectivity(const std::vector<JoinActionRef *> & edges)
{
    double selectivity = 1.0;
    for (const auto & edge : edges)
        selectivity = std::min(selectivity, computeSelectivity(*edge));
    return selectivity;
}


static std::optional<UInt64> estimateJoinCardinality(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    if (!left->estimated_rows || !right->estimated_rows)
        return {};

    double lhs = left->estimated_rows.value();
    double rhs = right->estimated_rows.value();

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, lhs + rhs);

    if (joined_rows > std::numeric_limits<UInt64>::max())
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

static double computeJoinCost(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    UNUSED(join_kind);
    UNUSED(selectivity);
    // return left->cost + right->cost + std::min(left->estimated_rows, right->estimated_rows);
    return left->cost + right->cost + selectivity * left->estimated_rows.value_or(1) * right->estimated_rows.value_or(1);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinReorderMicroseconds);

    std::shared_ptr<DPJoinEntry> best_plan;
    // if (query_graph.relation_stats.size() <= APPLY_DP_THRESHOLD)
    // {
    //     LOG_TRACE(log, "Solving join order using dynamic programming");
    //     best_plan = solveDP();
    //     if (!best_plan)
    //         LOG_TRACE(log, "Dynamic programming failed to find a valid join order");
    // }

    if (!best_plan)
    {
        LOG_TRACE(log, "Solving join order using greedy algorithm");
        best_plan = solveGreedy();
    }

    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    LOG_TRACE(log, "Optimized join order in {:.2f} ms", watch.elapsed() / 1000.0);
    return best_plan;
}


std::vector<JoinActionRef *> JoinOrderOptimizer::getApplicableExpressions(const BitSet & left, const BitSet & right)
{
    std::vector<JoinActionRef *> applicable;

    BitSet joined_rels = left | right;
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;
        const auto & edge_sources = edge.getSourceRelations();
        if (!isSubsetOf(edge_sources, joined_rels))
            continue;

        auto pinned = query_graph.pinned[edge];
        if (!isSubsetOf(pinned, left | right))
            continue;

        applicable.push_back(&edge);
    }
    return applicable;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
    std::deque<std::shared_ptr<DPJoinEntry>> components;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        components.push_back(std::make_shared<DPJoinEntry>(i, rel.estimated_rows));
    }

    std::vector<JoinActionRef *> applied_edge;
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

                auto join_kind = isValidJoinOrder(left->relations, right->relations);
                if (!join_kind)
                    continue;

                auto edge = getApplicableExpressions(left->relations, right->relations);
                if (edge.empty() && (best_plan || join_kind.value() == JoinKind::Inner))
                    continue;

                auto selectivity = computeSelectivity(edge);
                auto current_cost = computeJoinCost(left, right, selectivity, join_kind.value());
                if (!best_plan || current_cost < best_plan->cost)
                {
                    auto cardinality = estimateJoinCardinality(left, right, selectivity, join_kind.value());
                    JoinOperator join_operator(
                        join_kind.value(), JoinStrictness::All, JoinLocality::Local,
                        std::ranges::to<std::vector>(edge | std::views::transform([](const auto * e) { return *e; })));
                    applied_edge = std::move(edge);
                    best_plan = std::make_shared<DPJoinEntry>(left, right, current_cost, cardinality, std::move(join_operator));
                    best_i = i;
                    best_j = j;
                }
            }
        }

        /// If no valid join was found, add a cross product between smallest relations
        if (!best_plan)
        {
            /// Find two smallest components
            UInt64 first_best = std::numeric_limits<UInt64>::max();
            best_i = 0;
            UInt64 second_best = std::numeric_limits<UInt64>::max();
            best_j = 0;

            for (size_t idx = 0; idx < components.size(); idx++)
            {
                auto & component = components[idx];
                UInt64 estimated_rows = component->estimated_rows.value_or(std::numeric_limits<UInt64>::max() - 1);
                if (estimated_rows < first_best)
                {
                    std::tie(second_best, best_j) = std::tie(first_best, best_i);
                    std::tie(first_best, best_i) = std::tie(estimated_rows, idx);
                }
                else if (estimated_rows < second_best)
                {
                    std::tie(second_best, best_j) = std::tie(estimated_rows, idx);
                }
            }

            if (best_i == best_j)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find two smallest components");
            if (!isValidJoinOrder(components[best_i]->relations, components[best_j]->relations))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find two smallest components");

            auto cost = computeJoinCost(components[best_i], components[best_j], 1.0);
            auto cardinality = estimateJoinCardinality(components[best_i], components[best_j], 1.0);
            JoinOperator join_operator(JoinKind::Cross, JoinStrictness::All, JoinLocality::Local);
            best_plan = std::make_shared<DPJoinEntry>(components[best_i], components[best_j], cost, cardinality, join_operator);
            applied_edge.clear();
        }

        if (best_i == best_j)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find two smallest components");

        /// replace the two components with the best plan
        components.erase(components.begin() + std::max(best_i, best_j));
        components.erase(components.begin() + std::min(best_i, best_j));
        components.push_front(best_plan);
        dp_table[best_plan->relations] = best_plan;

        for (auto * edge : applied_edge)
            *edge = nullptr;
    }

    for (auto * edge : applied_edge)
        *edge = nullptr;

    auto non_applied_edges = std::views::filter(query_graph.edges, [](auto & edge) { return bool(edge); });
    if (!non_applied_edges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some expressions was not applied: [{}]",
            fmt::join(non_applied_edges | std::views::transform([](const auto & e) { return e.dump(); }), ", "));

    return components.at(0);
}


std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDP()
{
    return nullptr;
}

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrder(const BitSet & lhs, const BitSet & rhs) const
{
    JoinKind join_type = JoinKind::Inner;

    for (const auto & [first, second, join_kind_bound] : query_graph.dependencies)
    {
        if (lhs == first)
            join_type = reverseJoinKind(join_kind_bound);
        if (rhs == first)
            join_type = join_kind_bound;

        auto check = [&](const auto & a, const auto & b)
        {
            /// false positive clang-tidy warning
            return ((a & first) == BitSet() /// NOLINT
                || (a == first && (b & second))
                || (a & second)
                || isSubsetOf(b, first));
        };

        if (!check(lhs, rhs) || !check(rhs, lhs))
            return {};
    }
    return join_type;
}

DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph)
{
    if (query_graph.relation_stats.size() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderOptimizer: number of relations must be greater than 1");

    JoinOrderOptimizer reorderer(std::move(query_graph));
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_plan;
}

}
