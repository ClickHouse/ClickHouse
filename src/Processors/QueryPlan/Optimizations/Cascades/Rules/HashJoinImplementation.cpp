#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Produces all applicable hash join implementations in a single rule:
///   - Local join: both inputs gathered to one node (always applicable)
///   - Broadcast join: left partitioned any way, right replicated (only when node_count > 1)
///   - Partitioned (shuffle) join: both inputs shuffled by join keys (only when node_count > 1
///     and the join has equi-join predicates)
///
/// When node_count == 1 all three strategies produce the same plan, so only the local join
/// is emitted to avoid redundant identical alternatives in the memo.
class HashJoinImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "HashJoin"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool HashJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr &&
        expression->strategy == nullptr;
}

std::vector<GroupExpressionPtr> HashJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());
    chassert(join_step);
    chassert(expression->inputs.size() == 2);

    const size_t cluster_node_count = 4;    /// TODO: get actual cluster topology

    /// Determine the node count for distributed strategies.
    /// When the parent explicitly requires multi-node distribution, match that count so the
    /// join output can directly satisfy the requirement without an extra exchange.
    /// Otherwise fall back to the cluster size.
    const size_t distributed_node_count = required_properties.distribution.node_count > 1
        ? required_properties.distribution.node_count
        : cluster_node_count;

    std::vector<GroupExpressionPtr> result;

    /// Strategy 1: Local join — both inputs gathered to a single node.
    /// Always applicable; when distributed_node_count == 1 it is also the only strategy
    /// because all three strategies produce the same plan on a single-node cluster.
    {
        auto new_join_step = join_step->clone();
        new_join_step->setStepDescription(fmt::format("Local HashJoin {}", join_step->getStepDescription()), 200);

        GroupExpressionPtr local_join = std::make_shared<GroupExpression>(*expression);
        local_join->plan_step = std::move(new_join_step);
        local_join->strategy = std::make_shared<LocalJoinStrategy>();

        DistributionDescription single_node;     /// node_count=1, not replicated (default)
        local_join->inputs[0].required_properties.distribution = single_node;
        local_join->inputs[1].required_properties.distribution = single_node;
        local_join->properties.distribution = single_node;

        local_join->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(local_join);
        result.push_back(local_join);
    }

    /// For a single-node cluster all distributed strategies are identical to local join — skip them.
    if (distributed_node_count == 1)
        return result;

    /// Strategy 2: Broadcast join — left input partitioned any way across N nodes,
    /// right input replicated to all N nodes.
    {
        auto new_join_step = join_step->clone();
        new_join_step->setStepDescription(fmt::format("Broadcast HashJoin {}", join_step->getStepDescription()), 200);

        /// Left input: partitioned across N nodes (any column set is acceptable)
        DistributionDescription left_dist;
        left_dist.node_count = distributed_node_count;

        /// Right input: replicated to all N nodes
        DistributionDescription right_dist;
        right_dist.node_count = distributed_node_count;
        right_dist.is_replicated = true;

        GroupExpressionPtr broadcast_join = std::make_shared<GroupExpression>(*expression);
        broadcast_join->plan_step = std::move(new_join_step);
        broadcast_join->strategy = std::make_shared<BroadcastJoinStrategy>();
        broadcast_join->inputs[0].required_properties.distribution = left_dist;
        broadcast_join->inputs[1].required_properties.distribution = right_dist;
        /// Output inherits the left input's partitioning (any N-node partitioned distribution)
        broadcast_join->properties.distribution = left_dist;

        broadcast_join->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(broadcast_join);
        result.push_back(broadcast_join);
    }

    /// Strategy 3: Partitioned (shuffle) join — both inputs shuffled by join key columns.
    /// Only applicable when the join has equi-join predicates.
    if (!join_step->getJoinOperator().expression.empty())
    {
        /// Collect all equi-join key pairs, normalizing so that left_node
        /// comes from the join's left input and right_node from the right input.
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

        if (!equi_keys.empty())
        {
            DistributionDescription left_dist;
            left_dist.node_count = distributed_node_count;

            DistributionDescription right_dist;
            right_dist.node_count = distributed_node_count;

            DistributionDescription output_dist;
            output_dist.node_count = distributed_node_count;

            /// If the parent requires specific distribution columns, try to match them to join
            /// keys so the join output directly satisfies the parent's distribution requirement.
            /// Fall back to all equi-join keys if not all required columns can be matched.
            if (!required_properties.distribution.columns.empty())
            {
                bool all_matched = true;
                for (const auto & required_col_set : required_properties.distribution.columns)
                {
                    bool found = false;
                    for (const auto & [left_col, right_col] : equi_keys)
                    {
                        if (required_col_set.contains(left_col) || required_col_set.contains(right_col))
                        {
                            left_dist.columns.push_back({left_col});
                            right_dist.columns.push_back({right_col});
                            output_dist.columns.push_back({left_col, right_col});
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        all_matched = false;
                        break;
                    }
                }

                if (!all_matched)
                {
                    /// Required columns cannot all be matched to join keys — use all equi-join keys.
                    left_dist.columns.clear();
                    right_dist.columns.clear();
                    output_dist.columns.clear();
                    for (const auto & [left_col, right_col] : equi_keys)
                    {
                        left_dist.columns.push_back({left_col});
                        right_dist.columns.push_back({right_col});
                        output_dist.columns.push_back({left_col, right_col});
                    }
                }
            }
            else
            {
                for (const auto & [left_col, right_col] : equi_keys)
                {
                    left_dist.columns.push_back({left_col});
                    right_dist.columns.push_back({right_col});
                    output_dist.columns.push_back({left_col, right_col});
                }
            }

            auto new_join_step = join_step->clone();
            new_join_step->setStepDescription(fmt::format("Shuffle HashJoin {}", join_step->getStepDescription()), 200);

            GroupExpressionPtr partitioned_join = std::make_shared<GroupExpression>(*expression);
            partitioned_join->plan_step = std::move(new_join_step);
            partitioned_join->strategy = std::make_shared<ShuffleJoinStrategy>();
            partitioned_join->inputs[0].required_properties.distribution = left_dist;
            partitioned_join->inputs[1].required_properties.distribution = right_dist;
            partitioned_join->properties.distribution = output_dist;

            partitioned_join->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(partitioned_join);
            result.push_back(partitioned_join);
        }
    }

    return result;
}

OptimizationRulePtr createHashJoinImplementation() { return std::make_shared<HashJoinImplementation>(); }

}
