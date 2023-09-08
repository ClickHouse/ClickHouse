#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

size_t tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    if (parent_node->children.size() != 1)
        return false;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * parent_expr = typeid_cast<ExpressionStep *>(parent.get());
    auto * parent_filter = typeid_cast<FilterStep *>(parent.get());
    auto * child_expr = typeid_cast<ExpressionStep *>(child.get());

    if (parent_expr && child_expr)
    {
        const auto & child_actions = child_expr->getExpression();
        const auto & parent_actions = parent_expr->getExpression();

        /// We cannot combine actions with arrayJoin and stateful function because we not always can reorder them.
        /// Example: select rowNumberInBlock() from (select arrayJoin([1, 2]))
        /// Such a query will return two zeroes if we combine actions together.
        if (child_actions->hasArrayJoin() && parent_actions->hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(*child_actions), std::move(*parent_actions));

        auto expr = std::make_unique<ExpressionStep>(child_expr->getInputStreams().front(), merged);
        expr->setStepDescription("(" + parent_expr->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(expr);
        parent_node->children.swap(child_node->children);
        return 1;
    }
    else if (parent_filter && child_expr)
    {
        const auto & child_actions = child_expr->getExpression();
        const auto & parent_actions = parent_filter->getExpression();

        if (child_actions->hasArrayJoin() && parent_actions->hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(*child_actions), std::move(*parent_actions));

        auto filter = std::make_unique<FilterStep>(child_expr->getInputStreams().front(),
                                                   merged,
                                                   parent_filter->getFilterColumnName(),
                                                   parent_filter->removesFilterColumn());
        filter->setStepDescription("(" + parent_filter->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    if (child_expr)
    {
        const auto & child_actions = child_expr->getExpression();
        const auto & child_inputs = child_actions->getInputs();
        const auto & child_outputs = child_actions->getOutputs();


        // Check if all nodes in the DAG have ActionType INPUT
        bool all_input_nodes = std::all_of(
            child_actions->getNodes().begin(),
            child_actions->getNodes().end(),
            [](const auto & node) { return node.type == ActionsDAG::ActionType::INPUT; });

        // Check if the list of inputs and the list of outputs in DAG is identical
        bool inputs_outputs_identical = child_inputs == child_outputs;

        // Check if Inputs list is a prefix of step header and ActionsDAG::project_input == false
        // or Inputs list and step header are identical
        const auto & child_header = child_expr->getInputStreams().front().header;

        bool is_input_prefix_of_header = child_inputs.size() <= child_header.columns()
            && std::equal(child_inputs.begin(),
                          child_inputs.end(),
                          child_header.begin(),
                          child_header.end(),
                          [](const auto & input, const auto & column) { return input->result_name == column.name; });

        bool identical_inputs_and_header = child_inputs.size() == child_header.columns() && is_input_prefix_of_header;

        if (!child_actions->isProjectInput())
            identical_inputs_and_header |= is_input_prefix_of_header;

        if (all_input_nodes && inputs_outputs_identical && identical_inputs_and_header)
        {
            // Remove the redundant expression step
            parent_node->children.swap(child_node->children);
            return 1;
        }
    }

    return 0;
}

}
