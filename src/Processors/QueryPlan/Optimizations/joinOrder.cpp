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
    const JoinOperator * join_operator;

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
            const JoinOperator * join_operator_,
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
    explicit JoinOrderOptimizer(const JoinStepLogical & join_step);

    std::shared_ptr<DPJoinEntry> solve();

private:
    void buildQueryGraph();

    std::shared_ptr<DPJoinEntry> solveDP();
    std::shared_ptr<DPJoinEntry> solveGreedy();

    std::optional<JoinKind> isValidJoinOrder(const BaseRelsSet & lhs, const BaseRelsSet & rhs) const;

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    const JoinStepLogical & original_join_step;
    std::vector<RelationStats> relation_stats;
    std::vector<JoinEdge> join_edges;

    /// OUTER JOIN constraints
    std::vector<std::tuple<BaseRelsSet, BaseRelsSet, JoinKind>> dependencies;

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

[[ maybe_unused ]]
static auto bitsetToPositions(BaseRelsSet s)
{
    return std::views::iota(0u, s.size()) | std::views::filter([=](size_t i) { return s.test(i); });
}

static JoinKind flipJoinKind(JoinKind kind)
{
    switch (kind)
    {
        case JoinKind::Left:
            return JoinKind::Right;
        case JoinKind::Right:
            return JoinKind::Left;
        case JoinKind::Inner: [[ fallthrough ]];
        case JoinKind::Full:
            return kind;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported join kind: {}", toString(kind));
    }
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

JoinOrderOptimizer::JoinOrderOptimizer(const JoinStepLogical & join_step)
    : original_join_step(join_step)
{
    // Initialize relation statistics
    size_t num_tables = original_join_step.getNumberOfTables();
    relation_stats = original_join_step.getRelationStats();
    if (relation_stats.size() < num_tables)
        relation_stats.resize(num_tables);

    buildQueryGraph();
}

void JoinOrderOptimizer::buildQueryGraph()
{
    auto & join_operators = original_join_step.getJoinOperators();

    for (size_t i = 0; i < join_operators.size(); ++i)
    {
        const auto & join_op = join_operators[i];

        JoinEdge edge;

        /// In the original order, each join connects tables [0..i] with table [i+1]
        BaseRelsSet left_side;
        for (size_t j = 0; j <= i; ++j)
            left_side.set(j);

        BaseRelsSet right_side;
        right_side.set(i + 1);

        /// Non-reorderable joins
        if (isLeftOrFull(join_op.kind))
            dependencies.emplace_back(right_side, left_side, join_op.kind);
        if (isRightOrFull(join_op.kind))
            dependencies.emplace_back(left_side, right_side, flipJoinKind(join_op.kind));

        auto [extracted_left, extracted_right] = extractRelations(join_op, left_side, right_side);

        edge.join_operator = &join_op;
        edge.selectivity = estimateJoinSelectivity(join_op, relation_stats);
        edge.left_rels = extracted_left;
        edge.right_rels = extracted_right;

        join_edges.push_back(std::move(edge));
    }
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    if (relation_stats.size() <= 1)
        return nullptr;

    std::shared_ptr<DPJoinEntry> best_plan;
    if (relation_stats.size() <= APPLY_DP_THRESHOLD)
    {
        LOG_TRACE(log, "Solving join order using dynamic programming");
        best_plan = solveDP();
        if (!best_plan)
            LOG_TRACE(log, "Dynamic programming failed to find a valid join order");
    }

    if (!best_plan)
    {
        LOG_TRACE(log, "Solving join order using greedy algorithm");
        best_plan = solveGreedy();
    }

    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_plan;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDP()
{
    // This is a placeholder that will be properly implemented later
    return nullptr;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
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
                auto left = components[i];
                auto right = components[j];

                auto join_kind_restrction = isValidJoinOrder(left->relations, right->relations);
                if (!join_kind_restrction)
                    continue;

                for (auto edge : join_edges)
                {
                    bool connects_left_to_right = (left->relations & edge.left_rels).any() && (right->relations & edge.right_rels).any();
                    bool connects_right_to_left = (left->relations & edge.right_rels).any() && (right->relations & edge.left_rels).any();

                    if (!connects_left_to_right && !connects_right_to_left)
                        continue;

                    if ((connects_left_to_right && join_kind_restrction != edge.join_operator->kind)
                     || (connects_right_to_left && join_kind_restrction != flipJoinKind(edge.join_operator->kind)))
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Join order is not valid, expected {} but got {}", toString(join_kind_restrction.value()), toString(edge.join_operator->kind));

                    auto current_cost = computeJoinCost(left, right, edge.selectivity);
                    if (!best_plan || current_cost < best_plan->cost)
                    {
                        if (connects_right_to_left)
                            std::swap(left, right);
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
            if (!isValidJoinOrder(components[best_i]->relations, components[best_j]->relations))
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

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrder(const BaseRelsSet & lhs, const BaseRelsSet & rhs) const
{
    JoinKind join_type = JoinKind::Inner;

    for (const auto & [first, second, join_type_bound] : dependencies)
    {
        if (lhs == first)
            join_type = flipJoinKind(join_type_bound);
        if (rhs == first)
            join_type = join_type_bound;

        auto check = [&](auto a, auto b)
        {
            return ((a & first).none() || (a == first && (b & second).any()) || (a & second).any() || isSubsetOf(b, first));
        };

        if (!check(lhs, rhs) || !check(rhs, lhs))
            return {};
    }
    return join_type;
}

DPJoinEntryPtr optimizeJoinOrder(const JoinStepLogical & join_step)
{
    if (join_step.getNumberOfTables() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have at least 2 tables");

    JoinOrderOptimizer reorderer(join_step);
    auto best_dp = reorderer.solve();
    if (!best_dp)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    return best_dp;
}

DPJoinEntryPtr constructLeftDeepJoinOrder(const JoinStepLogical & join_step)
{
    size_t num_tables = join_step.getNumberOfTables();
    if (num_tables < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have at least 2 tables");

    const auto & join_operators = join_step.getJoinOperators();

    auto lhs = std::make_shared<DPJoinEntry>(0, 0);
    for (size_t i = 1; i < num_tables; ++i)
    {
        auto rhs = std::make_shared<DPJoinEntry>(i, 0);
        lhs = std::make_shared<DPJoinEntry>(lhs, rhs, 0.0, 0, &join_operators.at(i - 1));
    }
    return lhs;
}

}
