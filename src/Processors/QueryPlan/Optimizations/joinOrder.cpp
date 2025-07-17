#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
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

DPJoinEntry::DPJoinEntry(size_t id, size_t rows)
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
        size_t cardinality_,
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

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, bool> applied;
    std::unordered_map<JoinActionRef, double> expression_selectivity;

    LoggerPtr log = getLogger("JoinOrderOptimizer");
};


const ColumnStats * getColumnStats(BitSet rels, const String & column_name, const std::vector<RelationStats> & relation_stats)
{
    if (rels.count() != 1)
        return nullptr;

    auto rel_id = rels.findFirstSet();
    if (rel_id < 0 || relation_stats.size() <= static_cast<size_t>(rel_id))
        return nullptr;

    const auto & column_stats = relation_stats.at(rel_id).column_stats;
    auto it = column_stats.find(column_name);
    if (it == column_stats.end())
        return nullptr;

    return &it->second;
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

    const auto * lhs_stats = getColumnStats(lhs.getSourceRelations(), lhs.getColumnName(), query_graph.relation_stats);
    const auto * rhs_stats = getColumnStats(rhs.getSourceRelations(), rhs.getColumnName(), query_graph.relation_stats);
    UInt64 max_ndv = std::max(lhs_stats ? lhs_stats->num_distinct_values : 0,
                              rhs_stats ? rhs_stats->num_distinct_values : 0);
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


static size_t estimateJoinCardinality(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    double joined_rows = std::max(left->estimated_rows * right->estimated_rows * selectivity, 1.0);

    switch (join_kind)
    {
        case JoinKind::Inner:
            [[fallthrough]];
        case JoinKind::Comma:
            [[fallthrough]];
        case JoinKind::Cross:
            return static_cast<size_t>(joined_rows);
        case JoinKind::Left:
            return static_cast<size_t>(std::max<double>(joined_rows, left->estimated_rows));
        case JoinKind::Right:
            return static_cast<size_t>(std::max<double>(joined_rows, right->estimated_rows));
        case JoinKind::Full:
            return static_cast<size_t>(std::max<double>(joined_rows, left->estimated_rows + right->estimated_rows));
        default:
            return static_cast<size_t>(joined_rows);
    }
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
    return left->cost + right->cost + selectivity * left->estimated_rows * right->estimated_rows;
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
        if (!isSubsetOf(pinned, left) && !isSubsetOf(pinned, right))
            continue;

        applicable.push_back(&edge);
    }
    return applicable;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
    std::vector<std::shared_ptr<DPJoinEntry>> components;
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
            size_t first_best = std::numeric_limits<size_t>::max();
            best_i = 0;
            size_t second_best = std::numeric_limits<size_t>::max();
            best_j = 0;

            for (size_t idx = 0; idx < components.size(); idx++)
            {
                auto & component = components[idx];
                if (component->estimated_rows < first_best)
                {
                    std::tie(second_best, best_j) = std::tie(first_best, best_i);
                    std::tie(first_best, best_i) = std::tie(component->estimated_rows, idx);
                }
                else if (component->estimated_rows < second_best)
                {
                    std::tie(second_best, best_j) = std::tie(component->estimated_rows, idx);
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
        components.push_back(best_plan);
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
            return ((a & first) == BitSet()
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
    auto best_dp = reorderer.solve();
    if (!best_dp)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_dp;
}

}
