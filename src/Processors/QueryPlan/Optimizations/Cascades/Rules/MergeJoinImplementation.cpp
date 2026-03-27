#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Core/SortDescription.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Merge join: both inputs sorted by join keys, linear scan, no hash table.
/// Competes with `HashJoinImplementation` — wins when sorting is free
/// (via `SortedRead` on PK-aligned join keys).
class MergeJoinImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "MergeJoin"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool MergeJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep());
    if (!join_step || expression->strategy != nullptr)
        return false;

    /// ASOF joins have inequality predicates not supported by merge join.
    if (join_step->getJoinOperator().strictness == JoinStrictness::Asof)
        return false;

    /// Must have at least one equi-join predicate.
    for (const auto & predicate : join_step->getJoinOperator().expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op == JoinConditionOperator::Equals)
        {
            if ((left_node.fromLeft() && right_node.fromRight())
                || (left_node.fromRight() && right_node.fromLeft()))
                return true;
        }
    }
    return false;
}

std::vector<GroupExpressionPtr> MergeJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep());
    chassert(join_step);
    chassert(expression->inputs.size() == 2);

    /// Extract equi-join key pairs (same pattern as HashJoinImplementation).
    struct JoinKeyPair { String left; String right; };
    std::vector<JoinKeyPair> equi_keys;
    for (const auto & predicate : join_step->getJoinOperator().expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        else if (!left_node.fromLeft() || !right_node.fromRight())
            continue;
        equi_keys.push_back({left_node.getColumnName(), right_node.getColumnName()});
    }

    if (equi_keys.empty())
        return {};

    /// Build sort descriptions for both sides.
    SortDescription left_sort;
    SortDescription right_sort;
    for (const auto & [left_col, right_col] : equi_keys)
    {
        left_sort.push_back(SortColumnDescription{left_col, 1, 0});
        right_sort.push_back(SortColumnDescription{right_col, 1, 0});
    }

    const auto candidate_node_counts = getCandidateNodeCounts(memo.getClusterNodeCount());
    std::vector<GroupExpressionPtr> result;

    auto create_merge_join = [&](
        const DistributionDescription & left_dist,
        const DistributionDescription & right_dist,
        const DistributionDescription & output_dist,
        std::shared_ptr<const IJoinStrategy> strategy)
    {
        auto new_step = join_step->clone();
        auto * typed = typeid_cast<JoinStepLogical *>(new_step.get());
        typed->getJoinSettings().join_algorithms = {JoinAlgorithm::FULL_SORTING_MERGE};

        GroupExpressionPtr merge_join = std::make_shared<GroupExpression>(*expression);
        merge_join->plan_step = std::move(new_step);
        merge_join->strategy = std::move(strategy);

        merge_join->inputs[0].required_properties.distribution = left_dist;
        merge_join->inputs[0].required_properties.sorting = left_sort;
        merge_join->inputs[1].required_properties.distribution = right_dist;
        merge_join->inputs[1].required_properties.sorting = right_sort;
        merge_join->properties.distribution = output_dist;

        merge_join->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(merge_join);
        result.push_back(merge_join);
    };

    /// Local merge join: both inputs at {1 node}, sorted.
    create_merge_join({}, {}, {}, std::make_shared<LocalMergeJoinStrategy>());

    /// Shuffle merge join at each candidate node count.
    for (size_t node_count : candidate_node_counts)
    {
        DistributionDescription left_dist;
        left_dist.node_count = node_count;

        DistributionDescription right_dist;
        right_dist.node_count = node_count;

        DistributionDescription output_dist;
        output_dist.node_count = node_count;

        for (const auto & [left_col, right_col] : equi_keys)
        {
            left_dist.columns.push_back({left_col});
            right_dist.columns.push_back({right_col});
            output_dist.columns.push_back({left_col, right_col});
        }

        create_merge_join(left_dist, right_dist, output_dist, std::make_shared<ShuffleMergeJoinStrategy>());
    }

    return result;
}

OptimizationRulePtr createMergeJoinImplementation() { return std::make_shared<MergeJoinImplementation>(); }

}
