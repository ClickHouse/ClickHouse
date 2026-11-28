#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// True when the ExpressionStep only forwards columns and can be spliced out.
/// `updateHeader` keeps passthrough columns and drops inputs that are not outputs,
/// so a projection is not an identity even if every node is INPUT.
bool isTrivialIdentityExpression(const ExpressionStep & expr)
{
    const auto & dag = expr.getExpression();
    const auto & inputs = dag.getInputs();
    const auto & outputs = dag.getOutputs();
    if (inputs.size() != outputs.size())
        return false;

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (inputs[i] != outputs[i])
            return false;
    }

    for (const auto & node : dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::INPUT)
            return false;
    }

    if (expr.getInputHeaders().empty() || !expr.hasOutputHeader())
        return false;

    return blocksHaveEqualStructure(*expr.getInputHeaders().front(), *expr.getOutputHeader());
}

}

size_t tryRemoveTrivialExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    if (parent_node->children.size() != 1)
        return 0;

    auto * expr = typeid_cast<ExpressionStep *>(parent_node->step.get());
    if (!expr)
        return 0;

    if (!isTrivialIdentityExpression(*expr))
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    parent_node->step = std::move(child_node->step);
    parent_node->children.swap(child_node->children);
    return 1;
}

}
