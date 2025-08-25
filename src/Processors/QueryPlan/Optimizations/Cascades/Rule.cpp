#include <memory>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include "Core/Names.h"
#include "Interpreters/JoinInfo.h"
#include "Processors/QueryPlan/JoinStepLogical.h"
#include "Processors/QueryPlan/Optimizations/Cascades/Group.h"
#include "Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h"
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include "Common/logger_useful.h"
#include "Common/typeid_cast.h"

namespace DB
{

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, memo);
    expression->setApplied(*this);
    return new_expressions;
}


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
    auto * join_ab_c = typeid_cast<JoinStepLogical *>(expression->node.step.get());
    auto * join_ab = typeid_cast<JoinStepLogical *>(expression_ab->node.step.get());
    auto new_join = join_ab_c->clone();
    LOG_TRACE(log, "A join B condition:\n{}", toString(join_ab->getJoinInfo().expression.condition));
    LOG_TRACE(log, "AB join C condition:\n{}", toString(join_ab_c->getJoinInfo().expression.condition));

    /// Check that B JOIN C is not cross product, i.e. join_ab_c has a predicate between column coming from B and column from C
    NameSet columns_a = namesToSet(expression_ab->node.step->getInputHeaders().at(0)->getNames());
    NameSet columns_b = namesToSet(expression_ab->node.step->getInputHeaders().at(1)->getNames());
    NameSet columns_c = namesToSet(expression->node.step->getInputHeaders().at(1)->getNames());

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
        const auto & join_bc_node = expression->node;    /// FIXME: properly create join node
        auto new_expression_bc = std::make_shared<GroupExpression>(join_bc_node);
        new_expression_bc->inputs = {group_id_b, group_id_c};
        const GroupId group_id_bc = memo.addGroup(new_expression_bc);

        /// Create expression for "A JOIN (B JOIN C)" and add it to the current group
        const auto & join_a_bc_node = expression->node;    /// FIXME: properly create join node
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
        const auto & join_ac_node = expression->node;    /// FIXME: properly create join node
        auto new_expression_ac = std::make_shared<GroupExpression>(join_ac_node);
        new_expression_ac->inputs = {group_id_a, group_id_c};
        const GroupId group_id_ac = memo.addGroup(new_expression_ac);

        /// Create expression for "B JOIN (A JOIN C)" and add it to the current group
        const auto & join_b_ac_node = expression->node;    /// FIXME: properly create join node
        auto new_expression_b_ac = std::make_shared<GroupExpression>(join_b_ac_node);
        new_expression_b_ac->inputs = {group_id_b, group_id_ac};
        group_abc->addExpression(new_expression_b_ac);

        new_expressions.push_back(new_expression_ac);
        new_expressions.push_back(new_expression_b_ac);
    }

    return new_expressions;
}


bool JoinCommutativity::checkPattern(GroupExpressionPtr expression, const Memo & /*memo*/) const
{
    return expression->getName() == "Join";
}

std::vector<GroupExpressionPtr> JoinCommutativity::applyImpl(GroupExpressionPtr expression, Memo & memo) const
{
    GroupExpressionPtr expression_with_swapped_inputs = std::make_shared<GroupExpression>(*expression);
    chassert(expression_with_swapped_inputs->inputs.size() == 2);
    std::swap(expression_with_swapped_inputs->inputs[0], expression_with_swapped_inputs->inputs[1]);
    expression_with_swapped_inputs->setApplied(*this);  /// Don't want to apply commutativity rule to the new expression
    memo.getGroup(expression->group_id)->addExpression(expression_with_swapped_inputs);
    return {expression_with_swapped_inputs};
}

}
