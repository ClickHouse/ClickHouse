#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Pushes partial aggregation below a join when GROUP BY keys include the
/// join key.  Reduces rows before the join, shrinking intermediate results.
///
/// Pattern: Aggregating(final) → JoinLogical(left, right)
/// Result:  MergingAggregated → JoinLogical(left, Aggregating(partial, right))
///
/// The partial aggregation groups by the join key on the pushed-down side.
/// After the join, MergingAggregated merges partial states to produce final results.
class EagerAggregation : public IOptimizationRule
{
public:
    String getName() const override { return "EagerAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool EagerAggregation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        return false;
    if (expression->strategy != nullptr)
        return false;
    if (!agg_step->getFinal())
        return false;
    if (agg_step->getParams().keys.empty())
        return false;
    if (agg_step->isGroupingSets() || agg_step->getParams().overflow_row)
        return false;

    /// Find a JoinLogical below the aggregation.  It may be the direct input
    /// or separated by passthrough steps (Expression, Filter).
    chassert(expression->inputs.size() == 1);

    auto find_join = [&](GroupId group_id, auto & self, int depth) -> bool
    {
        if (depth > 3)
            return false;
        auto group = memo.getGroup(group_id);
        for (const auto & child_expr : group->logical_expressions)
        {
            if (typeid_cast<const JoinStepLogical *>(child_expr->getQueryPlanStep()))
            {
                /// Check that the join has at least one equi-join predicate.
                const auto * join_step = typeid_cast<const JoinStepLogical *>(child_expr->getQueryPlanStep());
                for (const auto & predicate : join_step->getJoinOperator().expression)
                {
                    auto [op, left_node, right_node] = predicate.asBinaryPredicate();
                    if (op == JoinConditionOperator::Equals
                        && ((left_node.fromLeft() && right_node.fromRight())
                            || (left_node.fromRight() && right_node.fromLeft())))
                        return true;
                }
            }
            /// Follow through passthrough steps (Expression, Filter).
            if (child_expr->inputs.size() == 1
                && (typeid_cast<const ExpressionStep *>(child_expr->getQueryPlanStep())
                    || typeid_cast<const FilterStep *>(child_expr->getQueryPlanStep())))
            {
                if (self(child_expr->inputs[0].group_id, self, depth + 1))
                    return true;
            }
        }
        return false;
    };

    return find_join(expression->inputs[0].group_id, find_join, 0);
}

std::vector<GroupExpressionPtr> EagerAggregation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    chassert(agg_step && expression->inputs.size() == 1);

    const auto & agg_keys = agg_step->getParams().keys;
    std::vector<GroupExpressionPtr> result;

    /// Find join expressions, possibly through passthrough steps.
    struct JoinInfo
    {
        GroupExpressionPtr join_expr;
        /// Chain of passthrough group IDs between agg and join (exclusive of both).
        /// The new join group must feed into the first passthrough group to preserve
        /// the Expression(Before GROUP BY) step between join and aggregation.
    };

    std::vector<JoinInfo> join_candidates;

    auto collect_joins = [&](GroupId group_id, auto & self, int depth) -> void
    {
        if (depth > 3)
            return;
        auto group = memo.getGroup(group_id);
        for (const auto & child_expr : group->logical_expressions)
        {
            if (typeid_cast<const JoinStepLogical *>(child_expr->getQueryPlanStep())
                && child_expr->inputs.size() == 2)
            {
                join_candidates.push_back({child_expr});
            }
            if (child_expr->inputs.size() == 1
                && (typeid_cast<const ExpressionStep *>(child_expr->getQueryPlanStep())
                    || typeid_cast<const FilterStep *>(child_expr->getQueryPlanStep())))
            {
                self(child_expr->inputs[0].group_id, self, depth + 1);
            }
        }
    };
    collect_joins(expression->inputs[0].group_id, collect_joins, 0);

    for (const auto & [join_expr_ptr] : join_candidates)
    {
        const auto * join_step = typeid_cast<const JoinStepLogical *>(join_expr_ptr->getQueryPlanStep());
        chassert(join_step);

        /// Extract equi-join key pairs.
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
            continue;

        /// Try pushing partial aggregation to the right side of the join.
        /// Collect GROUP BY keys that match right-side join keys.
        Names partial_keys;
        for (const auto & [left_col, right_col] : equi_keys)
            for (const auto & key : agg_keys)
                if (key == right_col)
                    partial_keys.push_back(right_col);

        if (partial_keys.empty())
            continue;

        /// Create the partial aggregation on the right input.
        auto partial_step_ptr = agg_step->clone();
        auto * partial_step = dynamic_cast<AggregatingStep *>(partial_step_ptr.get());
        if (!partial_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "EagerAggregation: clone of AggregatingStep returned unexpected type");
        partial_step->setFinal(false);
        partial_step->setStepDescription(fmt::format("EagerPartial: {}", agg_step->getStepDescription()), 200);

        const auto agg_output_header = partial_step->getOutputHeader();

        GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(std::move(partial_step_ptr));
        partial_expr->inputs = {join_expr_ptr->inputs[1]};
        GroupId partial_group_id = memo.addGroup(partial_expr);

        /// Build a new JoinStepLogical with the aggregated right header.
        /// The right side now produces [join_key, agg_state_cols...].
        /// We construct a fresh JoinExpressionActions from the new headers,
        /// re-add the equi-join predicate, and set the output columns.
        /// The partial agg replaces source 0 (first input header).
        /// Source 1 (second input header) stays as the other join side.
        auto left_header = std::make_shared<Block>(*agg_output_header);
        const auto & right_header = join_step->getInputHeaders().front();

        JoinExpressionActions new_expr_actions(*left_header, *right_header);

        /// Re-create equi-join predicates as equals() function nodes.
        auto new_join_operator = join_step->getJoinOperator();
        new_join_operator.expression.clear();
        new_join_operator.residual_filter.clear();
        for (const auto & [left_col, right_col] : equi_keys)
        {
            auto left_ref = new_expr_actions.findNode(left_col, /*is_input=*/true);
            auto right_ref = new_expr_actions.findNode(right_col, /*is_input=*/true);

            /// Add equals(left_key, right_key) to the ActionsDAG.
            auto equals_resolver = FunctionFactory::instance().get("equals", nullptr);
            const auto * equals_node = &new_expr_actions.getActionsDAG()->addFunction(
                equals_resolver,
                {left_ref.getNode(), right_ref.getNode()},
                fmt::format("equals({}, {})", left_col, right_col));

            new_join_operator.expression.push_back(JoinActionRef(equals_node, new_expr_actions));
        }

        /// Output: all INPUT nodes from both sides (passthrough all columns).
        std::vector<const ActionsDAG::Node *> new_actions_after_join;
        for (const auto * node : new_expr_actions.getActionsDAG()->getInputs())
            new_actions_after_join.push_back(node);

        /// Set the DAG outputs so updateOutputHeader can build the output header.
        new_expr_actions.getActionsDAG()->getOutputs() = {new_actions_after_join.begin(), new_actions_after_join.end()};

        auto new_join_step_ptr = std::make_unique<JoinStepLogical>(
            left_header,
            right_header,
            std::move(new_join_operator),
            std::move(new_expr_actions),
            std::move(new_actions_after_join),
            join_step->getJoinSettings(),
            join_step->getSortingSettings());
        new_join_step_ptr->setStepDescription(fmt::format("EagerJoin: {}", join_step->getStepDescription()), 200);

        GroupExpressionPtr new_join_expr = std::make_shared<GroupExpression>(std::move(new_join_step_ptr));
        new_join_expr->inputs = {join_expr_ptr->inputs[0], {partial_group_id, {}}};
        GroupId new_join_group_id = memo.addGroup(new_join_expr);

        /// Merge aggregation on top of the new join.
        auto merge_params = agg_step->getParams();
        merge_params.only_merge = true;
        auto merge_step_ptr = std::make_unique<MergingAggregatedStep>(
            agg_output_header,
            std::move(merge_params),
            agg_step->getGroupingSetsParamsList(),
            /*final_=*/true,
            /*memory_efficient_aggregation_=*/false,
            /*memory_efficient_merge_threads_=*/0,
            agg_step->shouldProduceResultsInBucketOrder(),
            agg_step->getMaxBlockSize(),
            agg_step->getMaxBlockSizeForAggregationInOrder(),
            agg_step->usingMemoryBoundMerging());
        merge_step_ptr->setStepDescription(fmt::format("EagerMerge: {}", agg_step->getStepDescription()), 200);

        GroupExpressionPtr merge_expr = std::make_shared<GroupExpression>(std::move(merge_step_ptr));
        merge_expr->inputs = {{new_join_group_id, {}}};
        merge_expr->setApplied(*this, {});
        memo.getGroup(expression->group_id)->addLogicalExpression(merge_expr);
        result.push_back(merge_expr);
    }

    return result;
}

OptimizationRulePtr createEagerAggregation() { return std::make_shared<EagerAggregation>(); }

}
