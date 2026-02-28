#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <deque>
#include <functional>
#include <limits>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/safe_cast.h>
#include <base/defines.h>
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
    extern const int EXPERIMENTAL_FEATURE_ERROR;
}

DPJoinEntry::DPJoinEntry(size_t id, std::optional<UInt64> rows)
    : relations()
    , cost(0.0)
    , estimated_rows(rows)
    , relation_id(static_cast<int>(id))
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
    return fmt::format("Join({})", fmt::join(relations, ","));
}

class JoinOrderOptimizer
{
public:
    JoinOrderOptimizer(QueryGraph query_graph_, const std::vector<JoinOrderAlgorithm> & enabled_algorithms_)
        : query_graph(std::move(query_graph_))
        , enabled_algorithms(enabled_algorithms_)
    {
        auto context = CurrentThread::getQueryContext();
        if (context)
        {
            query_status = context->getProcessListElementSafe();
            interactive_cancel_callback = context->getInteractiveCancelCallback();
        }
    }

    std::shared_ptr<DPJoinEntry> solve();
private:
    void buildQueryGraph();

    std::shared_ptr<DPJoinEntry> solveDPsize();
    std::shared_ptr<DPJoinEntry> solveGreedy();

    std::optional<JoinKind> isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const;
    std::vector<JoinActionRef *> getApplicableExpressions(const BitSet & left, const BitSet & right);

    double computeSelectivity(const JoinActionRef & edge);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges);
    size_t getColumnStats(BitSet rels, const String & column_name);

    /// Peridically called from potentially long running optimization to check time limits and send progress
    void checkLimits();

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, bool> applied;
    std::unordered_map<JoinActionRef, double> expression_selectivity;
    std::unordered_map<BitSet, DPJoinEntryPtr> dp_table;

    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    LoggerPtr log = getLogger("JoinOrderOptimizer");

    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

void JoinOrderOptimizer::checkLimits()
{
    if (query_status)
        query_status->checkTimeLimit();
    if (interactive_cancel_callback)
        interactive_cancel_callback();
}

size_t JoinOrderOptimizer::getColumnStats(BitSet rels, const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        /// Assume all keys are distinct
        if (auto it = dp_table.find(rels); it != dp_table.end())
            return it->second->estimated_rows.value_or(0);
        return 0;
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
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
        return 1.0;

    UInt64 lhs_ndv = getColumnStats(lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
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

    double lhs = static_cast<double>(left->estimated_rows.value());
    double rhs = static_cast<double>(right->estimated_rows.value());

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, lhs + rhs);

    /// Use >= to avoid undefined behavior when joined_rows is very close to max UInt64
    /// Due to floating point precision, a value slightly less than max when compared
    /// as double could still overflow when cast to UInt64
    if (joined_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

static double computeJoinCost(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity)
{
    return left->cost + right->cost + selectivity * static_cast<double>(left->estimated_rows.value_or(1)) * static_cast<double>(right->estimated_rows.value_or(1));
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinReorderMicroseconds);

    std::shared_ptr<DPJoinEntry> best_plan;

    for (const auto & algorithm : enabled_algorithms)
    {
        LOG_TRACE(log, "Solving join order using {} algorithm", toString(algorithm));
        switch (algorithm)
        {
            case JoinOrderAlgorithm::DPSIZE:
                best_plan = solveDPsize();
                break;
            case JoinOrderAlgorithm::GREEDY:
                best_plan = solveGreedy();
                if (!best_plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order with greedy algorithm");
                break;
        }

        if (best_plan)
            break;
    }

    if (!best_plan)
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Failed to find a valid join order, try adding 'greedy' algorithm as fallback to query_plan_optimize_join_order_algorithm setting.");

    LOG_TRACE(log, "Optimized join order in {:.2f} ms, best plan cost: {}, estimated cardinality: {}",
        static_cast<double>(watch.elapsed()) / 1000.0, best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown");
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

        auto pin_it = query_graph.pinned.find(edge);
        if (pin_it != query_graph.pinned.end())
        {
            /** We pin the expression in two cases:
              * 1. The expression is part of an OUTER JOIN ON clause.
              * 2. The expression depends on a relation that was previously part of an OUTER JOIN.
              * In the first case, the OUTER JOINed relation can only appear as a singleton in the `left` or `right` set.
              * In the second case, the relation can be part of a bushy tree,
              * so the expression is not strictly applied the first time its pinned relation is joined.
              * Here, we just check if the expression is pinned to another join,
              * which can be different from the expression's source relations.
              */
            if (!joined_rels.test(pin_it->second))
                continue;
        }

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
                if (edge.empty() && best_plan)
                    continue;

                auto selectivity = computeSelectivity(edge);
                auto current_cost = computeJoinCost(left, right, selectivity);
                if (!best_plan || current_cost < best_plan->cost)
                {
                    if (!edge.empty() && join_kind == JoinKind::Cross)
                        join_kind = JoinKind::Inner;
                    auto cardinality = estimateJoinCardinality(left, right, selectivity, join_kind.value());
                    JoinOperator join_operator(
                        join_kind.value(), JoinStrictness::All, JoinLocality::Unspecified,
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join restriction violated");

            auto cost = computeJoinCost(components[best_i], components[best_j], 1.0);
            auto cardinality = estimateJoinCardinality(components[best_i], components[best_j], 1.0);
            JoinOperator join_operator(JoinKind::Cross, JoinStrictness::All, JoinLocality::Unspecified);
            /// Use left: min idx, right: max idx to keep original order order of joins
            /// We will swap tables later if needed
            best_plan = std::make_shared<DPJoinEntry>(components[std::min(best_i, best_j)], components[std::max(best_i, best_j)], cost, cardinality, join_operator);
            applied_edge.clear();
        }

        if (best_i == best_j)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find components to join");

        LOG_TEST(log, "Best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, join operator: {}",
            best_plan->dump(), best_plan->left->dump(), best_plan->right->dump(),
            best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown",
            best_plan->join_operator.dump());

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
            fmt::join(non_applied_edges | std::views::take(5) | std::views::transform(&JoinActionRef::dump), ", "));

    return components.at(0);
}


/// Checks if predicate has sources from both left and right sets
static bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right)
{
    const auto & participating = predicate->getSourceRelations();
    return (participating & left) && (participating & right);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPsize()
{
    const size_t total_relations_count = query_graph.relation_stats.size();

    /// Components by size (index 0 is not used that why the size is N+1)
    std::vector<std::unordered_map<BitSet, DPJoinEntryPtr>> components(total_relations_count + 1);

    /// Populate DP table for components of size=1
    for (size_t i = 0; i < total_relations_count; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows);
        components[1][entry->relations] = entry;
        dp_table[entry->relations] = entry;
    }

    /// Iteratively build components of size from 2 to N
    for (size_t component_size = 2; component_size <= total_relations_count; ++component_size)
    {
        for (size_t smaller_component_size = 1; smaller_component_size <= component_size / 2; ++smaller_component_size)
        {
            const size_t bigger_component_size = component_size - smaller_component_size;

            for (const auto & [_, right] : components[smaller_component_size])
            {
                checkLimits();

                for (const auto & [_, left] : components[bigger_component_size])
                {
                    /// Do components overlap?
                    if (left->relations & right->relations)
                        continue;

                    /// If both components are of the same size then check each pair just once, not twice
                    if (smaller_component_size == bigger_component_size && *left->relations.begin() > *right->relations.begin())
                        continue;

                    const auto combined_relations = left->relations | right->relations;

                    auto join_kind = isValidJoinOrder(left->relations, right->relations);
                    if (!join_kind)
                        continue;

                    /// FIXME: Restrict to Inner joins for now because isValidJoinOrder seems to not handle non-Inner joins with swapped inputs correctly
                    if (*join_kind != JoinKind::Inner)
                        continue;

                    auto applicable_edge = getApplicableExpressions(left->relations, right->relations);
                    /// Only leave the edges that connect left and right
                    std::vector<JoinActionRef *> edge;
                    for (auto & edge_it : applicable_edge)
                    {
                        if (connects(edge_it, left->relations, right->relations))
                        {
                            LOG_TEST(log, "Adding predicate connecting {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else if ((edge_it->fromLeft() || edge_it->fromRight() || edge_it->fromNone()) && component_size == 2)
                        {
                            /// If a predicate does not connect tables we add it at the earliest stage - when joining just 2 tables
                            LOG_TEST(log, "Adding early non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else
                        {
                            LOG_TEST(log, "Skipping non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                        }
                    }

                    LOG_TEST(log, "Considering join between {} and {}, predicates count: {}", left->dump(), right->dump(), edge.size());

                    if (edge.empty())
                        continue;

                    auto selectivity = computeSelectivity(edge);
                    auto new_cost = computeJoinCost(left, right, selectivity);

                    auto current_best = dp_table.find(combined_relations);
                    if (current_best == dp_table.end() || new_cost < current_best->second->cost)
                    {
                        if (!edge.empty() && join_kind == JoinKind::Cross)
                            join_kind = JoinKind::Inner;
                        auto cardinality = estimateJoinCardinality(left, right, selectivity, join_kind.value());
                        JoinOperator join_operator(
                            join_kind.value(), JoinStrictness::All, JoinLocality::Unspecified,
                            std::ranges::to<std::vector>(edge | std::views::transform([](const auto * e) { return *e; })));
                        auto new_best_plan = std::make_shared<DPJoinEntry>(left, right, new_cost, cardinality, std::move(join_operator));

                        LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
                            new_best_plan->dump(), new_best_plan->left->dump(), new_best_plan->right->dump(),
                            new_best_plan->cost, new_best_plan->estimated_rows ? toString(*new_best_plan->estimated_rows) : "unknown",
                            new_best_plan->join_operator.dump());

                        dp_table[combined_relations] = new_best_plan;
                        components[component_size][combined_relations] = new_best_plan;
                    }
                }
            }
        }
    }

    auto best_full_plan = dp_table.find(BitSet::allSet(total_relations_count));
    if (best_full_plan != dp_table.end())
        return best_full_plan->second;

    LOG_TRACE(log, "Failed to find best plan using DPsize algorithm");
    return nullptr;
}

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const
{
    auto check = [&](const auto & lhs, const auto & rhs) -> std::optional<JoinKind>
    {
        auto rel_id = lhs.getSingleBit();
        if (rel_id.has_value())
        {
            auto it = query_graph.join_kinds.find(rel_id.value());
            if (it != query_graph.join_kinds.end())
            {
                if (isSubsetOf(it->second.first, rhs))
                    return it->second.second;
                return {};
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
        left_join_type = res.value();
    else
        return {};

    if (auto res = check(right_mask, left_mask))
        right_join_type = res.value();
    else
        return {};

    if (left_join_type == JoinKind::Inner)
        return right_join_type;
    if (right_join_type == JoinKind::Inner)
        return left_join_type;
    /// Allow FULL join as it's restricted to table swapping and no reordering
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflict, join is not possible:
    /// FROM t1 LEFT JOIN t2 LEFT JOIN t3
    /// t1 -> Inner, t2 -> Left, t3 -> Left
    /// Cannot do (t2 x t3)
    return {};
}

DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (query_graph.relation_stats.size() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderOptimizer: number of relations must be greater than 1");

    JoinOrderOptimizer reorderer(std::move(query_graph), optimization_settings.query_plan_optimize_join_order_algorithm);
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_plan;
}

}
