#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <limits>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/joinCost.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// Represents a join edge in the query graph
struct JoinEdge
{
    BaseRelsSet left_rels;
    BaseRelsSet right_rels;
    JoinOperator * join_operator;

    double selectivity = 1.0;
};


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
            JoinOperator * join_operator_,
            JoinMethod join_method_)
    : relations(lhs->relations | rhs->relations)
    , left(std::move(lhs))
    , right(std::move(rhs))
    , cost(cost_)
    , estimated_rows(cardinality_)
    , join_operator(join_operator_)
    , join_method(join_method_)
{
}

bool DPJoinEntry::isLeaf() const { return !left && !right; }

class JoinOrderOptimizer
{
public:
    explicit JoinOrderOptimizer(JoinStepLogical & join_step);

    std::shared_ptr<DPJoinEntry> solve();

private:
    void buildQueryGraph();

    std::shared_ptr<DPJoinEntry> solveDP();
    std::shared_ptr<DPJoinEntry> solveGreedy();

    bool isValidJoinOrder(const BaseRelsSet & lhs, const BaseRelsSet & rhs) const;

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    JoinStepLogical & original_join_step;
    std::vector<RelationStats> relation_stats;
    std::vector<JoinEdge> join_edges;

    // Dependencies that must be respected (e.g., LEFT JOIN constraints)
    std::vector<std::pair<BaseRelsSet, BaseRelsSet>> dependencies;

    std::unordered_map<BaseRelsSet, std::shared_ptr<DPJoinEntry>> memo;

    LoggerPtr log = getLogger("JoinOrderOptimizer");
};

template <typename F>
void forEachJoinAction(const JoinCondition & join_condition, F && f)
{
    for (auto & pred : join_condition.predicates)
    {
        f(pred.left_node);
        f(pred.right_node);
    }

    for (auto & node : join_condition.restrict_conditions)
    {
        f(node);
    }
}

template <typename F>
void forEachJoinAction(const JoinExpression & join_expression, F && f)
{
    forEachJoinAction(join_expression.condition, f);
    for (auto & join_condition : join_expression.disjunctive_conditions)
        forEachJoinAction(join_condition, f);
}


std::pair<BaseRelsSet, BaseRelsSet> extractRelations(const JoinOperator & join_op, BaseRelsSet lsuper, BaseRelsSet rsuper)
{
    BaseRelsSet left_rels;
    BaseRelsSet right_rels;

    forEachJoinAction(join_op.expression, [&](const auto & action)
    {
        auto rels = action.getSourceRels();
        if (isSubsetOf(rels, lsuper))
        {
            left_rels |= rels;
        }
        else if (isSubsetOf(rels, rsuper))
        {
            right_rels |= rels;
        }
        else if (isSubsetOf(rels, lsuper | rsuper))
        {
            left_rels |= (rels & lsuper);
            right_rels |= (rels & rsuper);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine relations for JoinActionRef {}", action.getColumnName());
        }
    });

    return {left_rels, right_rels};
}

JoinOrderOptimizer::JoinOrderOptimizer(JoinStepLogical& join_step)
    : original_join_step(join_step)
{
    // Initialize relation statistics
    size_t num_tables = original_join_step.getNumberOfTables();
    relation_stats.reserve(num_tables);

    for (size_t i = 0; i < num_tables; ++i)
    {
        RelationStats stats;
        stats.estimated_rows = 1000; // FIXME Default estimation, should be improved
        relation_stats.push_back(std::move(stats));
    }

    buildQueryGraph();
}

void JoinOrderOptimizer::buildQueryGraph()
{
    auto & join_operators = original_join_step.getJoinOperators();

    for (size_t i = 0; i < join_operators.size(); ++i) {
        auto & join_op = join_operators[i];

        JoinEdge edge;

        /// In the original order, each join connects tables [0..i] with table [i+1]
        BaseRelsSet left_rels;
        for (size_t j = 0; j <= i; ++j)
            left_rels.set(j);

        BaseRelsSet right_rels;
        right_rels.set(i + 1);

        auto [extracted_left, extracted_right] = extractRelations(join_op, left_rels, right_rels);

        edge.join_operator = &join_op;
        edge.selectivity = estimateJoinSelectivity(join_op, relation_stats);
        edge.left_rels = extracted_left;
        edge.right_rels = extracted_right;

        join_edges.push_back(std::move(edge));

        /// Non-reorderable joins
        if (isLeftOrFull(join_op.kind))
            dependencies.emplace_back(edge.right_rels, edge.left_rels);
        if (isRightOrFull(join_op.kind))
            dependencies.emplace_back(edge.left_rels, edge.right_rels);
    }
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    if (relation_stats.size() <= 1)
        return nullptr;

    std::shared_ptr<DPJoinEntry> best_plan;
    if (relation_stats.size() <= APPLY_DP_THRESHOLD)
        best_plan = solveDP();

    if (!best_plan)
        best_plan = solveGreedy();

    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_plan;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDP()
{
    LOG_TRACE(log, "Solving join order using dynamic programming");
    // This is a placeholder that will be properly implemented later
    return nullptr;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
    LOG_TRACE(log, "Solving join order using greedy algorithm");

    std::vector<std::shared_ptr<DPJoinEntry>> components;
    for (size_t i = 0; i < relation_stats.size(); ++i)
    {
        const auto & rel = relation_stats[i];
        components.push_back(std::make_shared<DPJoinEntry>(i, rel.estimated_rows));
    }

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
                const auto & left = components[i];
                const auto & right = components[j];

                if (!isValidJoinOrder(left->relations, right->relations))
                    continue;

                for (auto edge : join_edges)
                {
                    bool connects_left_to_right = (left->relations & edge.left_rels).any() && (right->relations & edge.right_rels).any();
                    bool connects_right_to_left = (left->relations & edge.right_rels).any() && (right->relations & edge.left_rels).any();

                    if (!connects_left_to_right && !connects_right_to_left)
                        continue;

                    auto current_cost = computeJoinCost(left, right, edge.selectivity);
                    if (!best_plan || current_cost < best_plan->cost)
                    {
                        auto cardinality = estimateJoinCardinality(left, right, edge.selectivity, edge.join_operator->kind);
                        best_plan = std::make_shared<DPJoinEntry>(left, right, current_cost, cardinality, edge.join_operator);
                        best_i = i;
                        best_j = j;
                    }
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

            auto cost = computeJoinCost(components[best_i], components[best_j], 1.0);
            auto cardinality = estimateJoinCardinality(components[best_i], components[best_j], 1.0);
            best_plan = std::make_shared<DPJoinEntry>(components[best_i], components[best_j], cost, cardinality, nullptr);
        }

        if (best_i == best_j)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find two smallest components");

        /// replace the two components with the best plan
        components.erase(components.begin() + std::max(best_i, best_j));
        components.erase(components.begin() + std::min(best_i, best_j));
        components.push_back(best_plan);
    }

    return components.at(0);
}

bool JoinOrderOptimizer::isValidJoinOrder(const BaseRelsSet & lhs, const BaseRelsSet & rhs) const
{
    for (const auto & [dep_left, dep_right] : dependencies)
    {
        // If the dependent left relations are split between lhs and rhs,
        // and the dependent right relations are in rhs, we can't reorder
        if ((dep_left & lhs).any() && (dep_left & rhs).any() && (dep_right & rhs).any())
            return false;

        // If the dependent right relations are in lhs, but not all
        // dependent left relations are in lhs, we can't reorder
        if ((dep_right & lhs).any() && !(isSubsetOf(dep_left, lhs)))
            return false;
    }

    return true;
}

DPJoinEntryPtr optimizeJoinOrder(JoinStepLogical & join_step)
{
    if (join_step.getNumberOfTables() <= 1)
        return {};

    JoinOrderOptimizer reorderer(join_step);
    auto best_dp = reorderer.solve();
    if (!best_dp)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_dp;
}

}
