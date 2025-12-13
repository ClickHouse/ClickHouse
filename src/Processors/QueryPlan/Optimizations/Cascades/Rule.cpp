#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
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
    /// TODO: create hash join step from JoinStepLogical
    auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());

    auto new_join_step = join_step->clone();

    new_join_step->setStepDescription(fmt::format("HashJoin IMPL: {}", join_step->getStepDescription()), 200);

    GroupExpressionPtr hash_join_expression = std::make_shared<GroupExpression>(*expression);
    hash_join_expression->plan_step = std::move(new_join_step);
    chassert(hash_join_expression->inputs.size() == 2);
    memo.getGroup(expression->group_id)->addPhysicalExpression(hash_join_expression);
    hash_join_expression->setApplied(*this, required_properties);
    return {hash_join_expression};
}

bool ShuffleHashJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());
    return join_step != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        !join_step->getJoinOperator().expression.empty();
}

std::vector<GroupExpressionPtr> ShuffleHashJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    /// TODO: create hash join step from JoinStepLogical
    auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());

    auto new_join_step = join_step->clone();

    new_join_step->setStepDescription(fmt::format("ShuffleHashJoin IMPL: {}", join_step->getStepDescription()), 200);

    GroupExpressionPtr hash_join_expression = std::make_shared<GroupExpression>(*expression);

    DistributionColumns distribution_columns;
    for (const auto & predicate : join_step->getJoinOperator().expression)
    {
        auto equality_predicate = predicate.asBinaryPredicate();
        if (get<0>(equality_predicate) != JoinConditionOperator::Equals)
            continue;

        distribution_columns.push_back({
            get<1>(equality_predicate).getColumnName(),
            get<2>(equality_predicate).getColumnName()});
    }

    hash_join_expression->plan_step = std::move(new_join_step);

    chassert(hash_join_expression->inputs.size() == 2);
    /// Set required distribution by join keys to both inputs
    /// TODO: handle swapped inputs
    hash_join_expression->inputs[0].required_properties.distribution_columns = distribution_columns;
    hash_join_expression->inputs[1].required_properties.distribution_columns = distribution_columns;

    /// Output is also distributed by join keys
    /// TODO: handle equivalent columns
    hash_join_expression->properties.distribution_columns = distribution_columns;

    memo.getGroup(expression->group_id)->addPhysicalExpression(hash_join_expression);
    hash_join_expression->setApplied(*this, required_properties);
    return {hash_join_expression};
}

bool BroadcastJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
}

std::vector<GroupExpressionPtr> BroadcastJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    /// TODO: create hash join step from JoinStepLogical
    auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());

    auto new_join_step = join_step->clone();

    new_join_step->setStepDescription(fmt::format("BroadcastJoin IMPL: {}", join_step->getStepDescription()), 200);

    GroupExpressionPtr hash_join_expression = std::make_shared<GroupExpression>(*expression);
    hash_join_expression->plan_step = std::move(new_join_step);
    chassert(hash_join_expression->inputs.size() == 2);
    memo.getGroup(expression->group_id)->addPhysicalExpression(hash_join_expression);
    hash_join_expression->setApplied(*this, required_properties);
    return {hash_join_expression};
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
    /// TODO: create hash join step from JoinStepLogical
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();

    new_aggregating_step->setStepDescription(fmt::format("PartialDistributed IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);
    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
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
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

bool DistributionEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return expression->properties.distribution_columns != required_properties.distribution_columns;
}

std::vector<GroupExpressionPtr> DistributionEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    /// TODO: add actual Shuffle step
    SortDescription sort_description;
//    for (const auto & column : required_properties.distribution_columns)
//        sort_description.push_back(SortColumnDescription(*column.begin()));
    auto sorting_step = std::make_unique<SortingStep>(
        expression->getQueryPlanStep()->getOutputHeader(),
        sort_description,
        0,
        SortingStep::Settings(65000)    /// TODO: construct from settings
    );
//    implementation_expression->property_enforcer_steps.push_back(std::move(sorting_step));
    implementation_expression->properties.distribution_columns = required_properties.distribution_columns;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}


bool SortingEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return expression->properties.sorting != required_properties.sorting;
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
