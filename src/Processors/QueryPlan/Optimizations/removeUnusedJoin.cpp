#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Columns/ColumnConst.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Whether the join outputs every row of the side it preserves exactly once, whatever the other side holds.
bool outputsPreservedRowsOnce(const JoinStepLogical & join, const QueryPlan::Node & other_node, JoinTableSide other_side)
{
    const auto & join_operator = join.getJoinOperator();

    /// A filter applied after the join drops rows.
    if (!join_operator.residual_filter.empty())
        return false;

    switch (join_operator.strictness)
    {
        case JoinStrictness::Any:
            return true;
        /// The old `ANY` keeps one row of the right side per key, so it preserves the left rows only.
        case JoinStrictness::RightAny:
            return other_side == JoinTableSide::Right;
        case JoinStrictness::All:
            return matchesAtMostOneRow(join, other_node, other_side);
        /// `ASOF` takes one row as well, but its condition is validated only when the join is built.
        default:
            return false;
    }
}

/// A `CommonSubplanReferenceStep` reads the result of a `CommonSubplanStep` elsewhere in the plan, and that step
/// is replaced with one that can be executed only when its reference is found in the plan.
bool hasCommonSubplanReference(const QueryPlan::Node & root)
{
    VectorWithMemoryTracking<const QueryPlan::Node *> nodes_to_visit{&root};
    while (!nodes_to_visit.empty())
    {
        const auto * current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (typeid_cast<const CommonSubplanReferenceStep *>(current->step.get()))
            return true;

        nodes_to_visit.insert(nodes_to_visit.end(), current->children.begin(), current->children.end());
    }
    return false;
}

}

size_t tryRemoveUnusedJoin(QueryPlan::Node * node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get());
    if (!join || node->children.size() != 2)
        return 0;

    /// Decorrelation relies on the structure of the join, and `arrayJoin` in the condition is rejected when the join is built.
    if (join->hasCorrelatedExpressions() || join->getActionsDAG().hasArrayJoin())
        return 0;

    const auto kind = join->getJoinOperator().kind;
    if (!isLeftOrRight(kind))
        return 0;

    const auto preserved_side = isLeft(kind) ? JoinTableSide::Left : JoinTableSide::Right;
    const auto other_side = isLeft(kind) ? JoinTableSide::Right : JoinTableSide::Left;
    auto & preserved_node = *node->children[JoinStepLogical::getInputIndex(preserved_side)];
    auto & other_node = *node->children[JoinStepLogical::getInputIndex(other_side)];

    const auto output_actions = join->getOutputActions();
    for (const auto & output : output_actions)
        if (output.getSourceRelations().test(JoinStepLogical::getInputIndex(other_side)))
            return 0;

    if (!outputsPreservedRowsOnce(*join, other_node, other_side))
        return 0;

    /// The join builds its totals row from the totals of both inputs, and a prepared storage is validated
    /// against the kind and strictness of the join only when the join is built.
    if (canProduceTotals(other_node) || isPreparedJoinStorage(*node->children.back()))
        return 0;

    if (hasCommonSubplanReference(other_node))
        return 0;

    /// What the join outputs is computed from the preserved side alone, so an expression over it does the same.
    auto actions = JoinExpressionActions::getSubDAG(output_actions);
    actions.appendInputsForUnusedColumns(*preserved_node.step->getOutputHeader());

    /// The join materializes its constants when it outputs a dummy column.
    const auto & join_header = *join->getOutputHeader();
    auto & outputs = actions.getOutputs();
    for (size_t position = 0; position < outputs.size(); ++position)
        if (outputs[position]->column && isColumnConst(*outputs[position]->column)
            && !isColumnConst(*join_header.getByPosition(position).column))
            outputs[position] = &actions.materializeNode(*outputs[position]);

    auto expression_step = std::make_unique<ExpressionStep>(preserved_node.step->getOutputHeader(), std::move(actions));
    expression_step->setStepDescription("Output of removed JOIN");

    node->step = std::move(expression_step);
    node->children = {&preserved_node};
    return 1;
}

}
