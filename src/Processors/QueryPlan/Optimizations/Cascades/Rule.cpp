#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
//#include <Interpreters/JoinInfo.h>
#include <Core/Names.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, memo);
    expression->setApplied(*this);
    return new_expressions;
}

#if 0
bool JoinAssociativity::checkPattern(GroupExpressionPtr expression, const Memo & memo) const
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

std::vector<GroupExpressionPtr> JoinAssociativity::applyImpl(GroupExpressionPtr expression, Memo & memo) const
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

bool JoinCommutativity::checkPattern(GroupExpressionPtr expression, const Memo & /*memo*/) const
{
    return expression->getName() == "Join";
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

std::vector<GroupExpressionPtr> JoinCommutativity::applyImpl(GroupExpressionPtr expression, Memo & memo) const
{
    chassert(expression->inputs.size() == 2);
    const auto * join_step = typeid_cast<JoinStepLogical*>(expression->getQueryPlanStep());
    chassert(join_step);

    auto swapped_join_step = cloneSwapped(*join_step);
    swapped_join_step->setStepDescription(join_step->getStepDescription() + " swapped");

    GroupExpressionPtr expression_with_swapped_inputs = std::make_shared<GroupExpression>(nullptr);
    expression_with_swapped_inputs->plan_step = std::move(swapped_join_step);
    expression_with_swapped_inputs->inputs = {expression->inputs[1], expression->inputs[0]};
    expression_with_swapped_inputs->setApplied(*this);  /// Don't apply commutativity rule to the new expression
    memo.getGroup(expression->group_id)->addExpression(expression_with_swapped_inputs);

    return {expression_with_swapped_inputs};
}


bool HashJoinImplementation::checkPattern(GroupExpressionPtr expression, const Memo & /*memo*/) const
{
    return typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr;
}

std::vector<GroupExpressionPtr> HashJoinImplementation::applyImpl(GroupExpressionPtr expression, Memo & memo) const
{
    /// TODO: create hash join step from JoinStepLogical
    auto * join_step = typeid_cast<JoinStepLogical *>(expression->getQueryPlanStep());

//    QueryPlanOptimizationSettings optimization_settings(CurrentThread::get().getQueryContext());
//
//        JoinActionRef post_filter(nullptr);
//    auto join_ptr = join_step->convertToPhysical(
//        post_filter,
//        /*keep_logical*/ false,
//        optimization_settings.max_threads,
//        optimization_settings.max_entries_for_hash_table_stats,
//        optimization_settings.initial_query_id,
//        optimization_settings.lock_acquire_timeout,
//        optimization_settings.actions_settings,
//        {});
//
//    chassert(!join_ptr->isFilled());
//
//    SharedHeader output_header = join_step->getOutputHeader();
//
//    const auto & join_expression_actions = join_step->getExpressionActions();
//
//    const auto & settings = join_step->getSettings();
//
//    auto required_output_from_join = join_expression_actions.post_join_actions->getRequiredColumnsNames();
//    auto new_join_step = std::make_unique<JoinStep>(
//        join_step->getInputHeaders()[0],
//        join_step->getInputHeaders()[1],
//        join_ptr,
//        settings.max_block_size,
//        settings.min_joined_block_size_rows,
//        settings.min_joined_block_size_bytes,
//        optimization_settings.max_threads,
//        NameSet(required_output_from_join.begin(), required_output_from_join.end()),
//        false /*optimize_read_in_order*/,
//        true /*use_new_analyzer*/);


    auto new_join_step = join_step->clone();

    new_join_step->setStepDescription("IMPL: " + join_step->getStepDescription());

    GroupExpressionPtr hash_join_expression = std::make_shared<GroupExpression>(*expression);
    hash_join_expression->plan_step = std::move(new_join_step);
    chassert(hash_join_expression->inputs.size() == 2);
    memo.getGroup(expression->group_id)->addExpression(hash_join_expression);
    hash_join_expression->setApplied(*this);
    return {hash_join_expression};
}

bool DefaultImplementation::checkPattern(GroupExpressionPtr expression, const Memo & /*memo*/) const
{
    return typeid_cast<JoinStepLogical *>(expression->plan_step.get()) == nullptr &&
        expression->original_node &&
        !expression->plan_step;
}

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(nullptr);
    implementation_expression->original_node = expression->original_node;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->applied_rules = expression->applied_rules;
    implementation_expression->setApplied(*this);
    memo.getGroup(expression->group_id)->addExpression(implementation_expression);
    return {implementation_expression};
}

}
