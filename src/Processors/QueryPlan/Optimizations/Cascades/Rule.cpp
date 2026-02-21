#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, required_properties, memo);
    /// Note: Statistics are now stored in Group, not in GroupExpression, so no need to copy them
    expression->setApplied(*this, required_properties);
    return new_expressions;
}

#if 0
bool JoinAssociativity::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    if (expression->getName() != "Join")
        return false;
    if (expression->inputs.size() != 2)
        return false;
    auto left_child_group = memo.getGroup(expression->inputs[0]);
    return left_child_group->expressions.front()->getName() == "Join";

    /// TODO: check that result will not have a cross product?
}

static NameSet namesToSet(const Names & names)
{
    return NameSet(names.begin(), names.end());
}

std::vector<GroupExpressionPtr> JoinAssociativity::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo)) const
{
    auto log = getLogger("JoinAssociativity");

    auto group_abc = memo.getGroup(expression->group_id);   /// Current group which is the result of "(A JOIN B) JOIN C"

    chassert(expression->inputs.size() == 2);
    const GroupId group_id_ab = expression->inputs[0];
    const GroupId group_id_c = expression->inputs[1];
    auto expression_ab = memo.getGroup(group_id_ab)->expressions.front();

    chassert(expression_ab->getName() == "Join");
    chassert(expression_ab->inputs.size() == 2);
    const GroupId group_id_a = expression_ab->inputs[0];
    const GroupId group_id_b = expression_ab->inputs[1];

    /// TODO: check that both joins are Inner?


    /// Extract predicates from both JOINs
    auto * join_ab_c = typeid_cast<JoinStepLogical *>(expression->original_node->step.get());
    auto * join_ab = typeid_cast<JoinStepLogical *>(expression_ab->original_node->step.get());
    auto new_join = join_ab_c->clone();
    LOG_TRACE(log, "A join B condition:\n{}", toString(join_ab->getJoinInfo().expression.condition));
    LOG_TRACE(log, "AB join C condition:\n{}", toString(join_ab_c->getJoinInfo().expression.condition));

    /// Check that B JOIN C is not cross product, i.e. join_ab_c has a predicate between column coming from B and column from C
    NameSet columns_a = namesToSet(expression_ab->original_node->step->getInputHeaders().at(0)->getNames());
    NameSet columns_b = namesToSet(expression_ab->original_node->step->getInputHeaders().at(1)->getNames());
    NameSet columns_c = namesToSet(expression->original_node->step->getInputHeaders().at(1)->getNames());

    JoinCondition join_a_bc = join_ab->getJoinInfo().expression.condition;
    JoinCondition join_a_c;
    JoinCondition join_b_c;
    for (const auto & predicate_ab_c : join_ab_c->getJoinInfo().expression.condition.predicates)
    {
        if (predicate_ab_c.op == PredicateOperator::Equals &&
            columns_c.contains(predicate_ab_c.right_node.getColumnName()))
        {
            if (columns_a.contains(predicate_ab_c.left_node.getColumnName()))
                join_a_c.predicates.push_back(predicate_ab_c);
            if (columns_b.contains(predicate_ab_c.left_node.getColumnName()))
                join_b_c.predicates.push_back(predicate_ab_c);
        }
    }

    LOG_TRACE(log, "B join C condition:\n{}", toString(join_b_c));
    LOG_TRACE(log, "A join C condition:\n{}", toString(join_a_c));

    std::vector<GroupExpressionPtr> new_expressions;
    if (!join_b_c.predicates.empty())
    {
        /// New expression for group for "B JOIN C"
    //    auto join_b_c_step = std::make_shared<JoinStepLogical>(    );
        auto * join_bc_node = expression->original_node;    /// FIXME: properly create join node
        auto new_expression_bc = std::make_shared<GroupExpression>(join_bc_node);
        new_expression_bc->inputs = {group_id_b, group_id_c};
        const GroupId group_id_bc = memo.addGroup(new_expression_bc);

        /// Create expression for "A JOIN (B JOIN C)" and add it to the current group
        auto * join_a_bc_node = expression->original_node;    /// FIXME: properly create join node
        auto new_expression_a_bc = std::make_shared<GroupExpression>(join_a_bc_node);
        new_expression_a_bc->inputs = {group_id_a, group_id_bc};
        group_abc->addExpression(new_expression_a_bc);

        new_expressions.push_back(new_expression_bc);
        new_expressions.push_back(new_expression_a_bc);
    }

    if (!join_a_c.predicates.empty())
    {
        /// New expression for group for "A JOIN C"
    //    auto join_b_c_step = std::make_shared<JoinStepLogical>(    );
        auto * join_ac_node = expression->original_node;    /// FIXME: properly create join node
        auto new_expression_ac = std::make_shared<GroupExpression>(join_ac_node);
        new_expression_ac->inputs = {group_id_a, group_id_c};
        const GroupId group_id_ac = memo.addGroup(new_expression_ac);

        /// Create expression for "B JOIN (A JOIN C)" and add it to the current group
        auto * join_b_ac_node = expression->original_node;    /// FIXME: properly create join node
        auto new_expression_b_ac = std::make_shared<GroupExpression>(join_b_ac_node);
        new_expression_b_ac->inputs = {group_id_b, group_id_ac};
        group_abc->addExpression(new_expression_b_ac);

        new_expressions.push_back(new_expression_ac);
        new_expressions.push_back(new_expression_b_ac);
    }

    return new_expressions;
}
#endif

bool JoinCommutativity::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * join_step = typeid_cast<JoinStepLogical*>(expression->getQueryPlanStep());
    if (!join_step)
        return false;

    const auto & join = join_step->getJoinOperator();

    return
        join.kind == JoinKind::Inner ||
        join.kind == JoinKind::Cross ||
        join.strictness == JoinStrictness::Semi ||
        join.strictness == JoinStrictness::Any ||
        join.strictness == JoinStrictness::Anti;
}

/// Make the same JOIN but with left and right inputs swapped
std::unique_ptr<JoinStepLogical> cloneSwapped(const JoinStepLogical & join_step)
{
    /// Swap inputs
    auto left_input_header = join_step.getInputHeaders()[1];
    auto right_input_header = join_step.getInputHeaders()[0];
#if 0
    JoinExpressionActions join_expression_actions(
        left_input_header->getColumnsWithTypeAndName(),
        right_input_header->getColumnsWithTypeAndName(),
        join_step.getActionsDAG().clone());

    auto output_columns = join_step.getOutputHeader()->getNames();

    auto swapped_join_step = std::make_unique<JoinStepLogical>(
        left_input_header,
        right_input_header,
        join_step.getJoinOperator(), /// FIXME: need to remap all expression-s to cloned ActionsDAG!
        std::move(join_expression_actions),
        NameSet(output_columns.begin(), output_columns.end()),
        /*changed_types*/ std::unordered_map<String, const ActionsDAG::Node *>{}, /// TODO:
        /*use_nulls*/ false, /// TODO:
        join_step.getJoinSettings(),
        join_step.getSortingSettings());
#else
    auto swapped_join_step = std::unique_ptr<JoinStepLogical>(dynamic_cast<JoinStepLogical*>(join_step.clone().release()));
    swapped_join_step->swapInputs();
#endif

    return swapped_join_step;
}

std::vector<GroupExpressionPtr> JoinCommutativity::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    chassert(expression->inputs.size() == 2);
    const auto * join_step = typeid_cast<JoinStepLogical*>(expression->getQueryPlanStep());
    chassert(join_step);

    auto swapped_join_step = cloneSwapped(*join_step);
    swapped_join_step->setStepDescription(fmt::format("{} swapped", join_step->getStepDescription()), 200);

    GroupExpressionPtr expression_with_swapped_inputs = std::make_shared<GroupExpression>(nullptr);
    expression_with_swapped_inputs->plan_step = std::move(swapped_join_step);
    expression_with_swapped_inputs->inputs = {expression->inputs[1], expression->inputs[0]};
    expression_with_swapped_inputs->setApplied(*this, {});  /// Don't apply commutativity rule to the new expression
    memo.getGroup(expression->group_id)->addLogicalExpression(expression_with_swapped_inputs);

    return {expression_with_swapped_inputs};
}


bool HashJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
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
        new_join_step->setStepDescription(fmt::format("Local HashJoin IMPL: {}", join_step->getStepDescription()), 200);

        GroupExpressionPtr local_join = std::make_shared<GroupExpression>(*expression);
        local_join->plan_step = std::move(new_join_step);

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
        new_join_step->setStepDescription(fmt::format("Broadcast HashJoin IMPL: {}", join_step->getStepDescription()), 200);

        /// Left input: partitioned across N nodes (any column set is acceptable)
        DistributionDescription left_dist;
        left_dist.node_count = distributed_node_count;

        /// Right input: replicated to all N nodes
        DistributionDescription right_dist;
        right_dist.node_count = distributed_node_count;
        right_dist.is_replicated = true;

        GroupExpressionPtr broadcast_join = std::make_shared<GroupExpression>(*expression);
        broadcast_join->plan_step = std::move(new_join_step);
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
        /// Collect all equi-join key pairs.
        struct JoinKeyPair { String left; String right; };
        std::vector<JoinKeyPair> equi_keys;
        for (const auto & predicate : join_step->getJoinOperator().expression)
        {
            auto [op, left_node, right_node] = predicate.asBinaryPredicate();
            if (op == JoinConditionOperator::Equals)
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
            new_join_step->setStepDescription(fmt::format("Shuffle HashJoin IMPL: {}", join_step->getStepDescription()), 200);

            GroupExpressionPtr partitioned_join = std::make_shared<GroupExpression>(*expression);
            partitioned_join->plan_step = std::move(new_join_step);
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

bool LocalAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<AggregatingStep *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
}

std::vector<GroupExpressionPtr> LocalAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    /// TODO: create hash join step from JoinStepLogical
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();

    new_aggregating_step->setStepDescription(fmt::format("Local IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);
    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}


bool ShuffleAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    return aggregating_step != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        aggregating_step->getParams().keys_size != 0;
}

std::vector<GroupExpressionPtr> ShuffleAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    /// TODO: create hash join step from JoinStepLogical
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();

    new_aggregating_step->setStepDescription(fmt::format("Shuffle IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);
    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}


bool PartialDistributedAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<AggregatingStep *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
}

std::vector<GroupExpressionPtr> PartialDistributedAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();

    new_aggregating_step->setStepDescription(fmt::format("PartialDistributed IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);

    /// For the input we require partitioned distribution in any way
    size_t node_count = 4;    /// TODO: get actual cluster topology and calculate number of nodes
    DistributionDescription partitioned_distribution;
    partitioned_distribution.node_count = node_count;
    partitioned_distribution.is_replicated = false;

    aggregation_expression->inputs[0].required_properties.distribution = partitioned_distribution;

    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}

bool ParallelReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        required_properties.distribution.node_count > 1 &&
        !required_properties.distribution.is_replicated;
}

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * read_step = typeid_cast<ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto new_read_step = read_step->clone();

    new_read_step->setStepDescription(fmt::format("ParallelRead IMPL: {}", read_step->getStepDescription()), 200);
    GroupExpressionPtr read_expression = std::make_shared<GroupExpression>(*expression);
    read_expression->plan_step = std::move(new_read_step);
    read_expression->properties = required_properties;

    memo.getGroup(expression->group_id)->addPhysicalExpression(read_expression);
    read_expression->setApplied(*this, required_properties);
    return {read_expression};
}


bool DefaultImplementation::checkPattern(GroupExpressionPtr /*expression*/, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return true;
//    return typeid_cast<JoinStepLogical *>(expression->plan_step.get()) == nullptr &&
//        expression->original_node &&
//        !expression->plan_step;
}

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
////    implementation_expression->original_node = expression->original_node;
//    implementation_expression->inputs = expression->inputs;
//    implementation_expression->applied_rules = expression->applied_rules;
    implementation_expression->plan_step->setStepDescription(fmt::format("IMPL: {}", expression->plan_step->getStepDescription()), 200);
    implementation_expression->setApplied(*this, required_properties);

    /// FIXME: pass required properties only for steps that do not affect them, e.g. Filter and Expression steps
    if (implementation_expression->inputs.size() == 1)
        implementation_expression->inputs[0].required_properties = required_properties;

    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

bool DistributionEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isDistributionSatisfiedBy(required_properties.distribution, expression->properties.distribution);
}

std::vector<GroupExpressionPtr> DistributionEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);

    if (required_properties.distribution.columns.empty())
    {
        if (required_properties.distribution.is_replicated)
        {
            auto broadcast_exchange_step = std::make_unique<BroadcastExchangeStep>(
                expression->getQueryPlanStep()->getOutputHeader(),
                required_properties.distribution.node_count);
            implementation_expression->property_enforcer_steps.push_back(std::move(broadcast_exchange_step));
        }
        else if (required_properties.distribution.node_count == 1 && expression->properties.distribution.node_count > 1)
        {
            auto gather_exchange_step = std::make_unique<GatherExchangeStep>(
                expression->getQueryPlanStep()->getOutputHeader(),
                expression->properties.distribution.node_count);
            implementation_expression->property_enforcer_steps.push_back(std::move(gather_exchange_step));
        }
    }
    else
    {
        if (required_properties.distribution.is_replicated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot enforce replicated distribution with specific columns");

        Names shuffle_columns;
        for (const auto & distribution_column : required_properties.distribution.columns)
        {
            /// TODO: take the column that is present in the expression and is equivalent to required distribution column
            shuffle_columns.push_back(*distribution_column.begin());
        }
        auto shuffle_exchange_step = std::make_unique<ShuffleExchangeStep>(
            expression->getQueryPlanStep()->getOutputHeader(),
            std::move(shuffle_columns),
            required_properties.distribution.node_count,
            expression->properties.distribution.node_count);
        implementation_expression->property_enforcer_steps.push_back(std::move(shuffle_exchange_step));
    }
    implementation_expression->properties.distribution = required_properties.distribution;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}


bool SortingEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isSortingSatisfiedBy(required_properties.sorting, expression->properties.sorting);
}

std::vector<GroupExpressionPtr> SortingEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    auto sorting_step = std::make_unique<SortingStep>(
        expression->getQueryPlanStep()->getOutputHeader(),
        required_properties.sorting,
        0,
        SortingStep::Settings(65000)    /// TODO: construct from settings
    );
    implementation_expression->property_enforcer_steps.push_back(std::move(sorting_step));
    implementation_expression->properties.sorting = required_properties.sorting;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

}
