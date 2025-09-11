#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

QueryPlan::Node * makeExpressionNodeOnTopOf(QueryPlan::Node * node, ActionsDAG actions_dag, const String & filter_column_name, QueryPlan::Nodes & nodes);

size_t trySplitJoin(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node->step.get());
    if (!join_step || node->children.size() != 2)
        return 0;

    size_t num_new_nodes = 0;

    String filter_coumn_name;

    if (auto fitler_dag = join_step->getFilterActions(JoinTableSide::Left, filter_coumn_name))
    {
        auto * new_node = makeExpressionNodeOnTopOf(node->children.at(0), std::move(*fitler_dag), filter_coumn_name, nodes);
        node->children.at(0) = new_node;
        new_node->step->setStepDescription("Join filter");
        num_new_nodes++;
    }

    if (auto fitler_dag = join_step->getFilterActions(JoinTableSide::Right, filter_coumn_name))
    {
        auto * new_node = makeExpressionNodeOnTopOf(node->children.at(1), std::move(*fitler_dag), filter_coumn_name, nodes);
        node->children.at(1) = new_node;
        new_node->step->setStepDescription("Join filter");
        num_new_nodes++;
    }
    return num_new_nodes;
}

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
size_t trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (size_t join_split = trySplitJoin(node, nodes))
        return join_split;

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step)
        return 0;

    const auto & expr = filter_step->getExpression();
    const std::string & filter_column_name = filter_step->getFilterColumnName();

    /// Do not split if there are function like runningDifference.
    if (expr.hasStatefulFunctions())
        return 0;

    bool filter_name_clashs_with_input = false;
    if (filter_step->removesFilterColumn())
    {
        for (const auto * input : expr.getInputs())
        {
            if (input->result_name == filter_column_name)
            {
                filter_name_clashs_with_input = true;
                break;
            }
        }
    }

    auto split = expr.splitActionsForFilter(filter_column_name);

    if (split.second.trivial())
        return 0;

    bool remove_filter = false;
    if (filter_step->removesFilterColumn())
        remove_filter = split.second.removeUnusedResult(filter_column_name);

    auto description = filter_step->getStepDescription();

    auto & filter_node = nodes.emplace_back();
    node->children.swap(filter_node.children);
    node->children.push_back(&filter_node);

    std::string split_filter_name = filter_column_name;
    if (filter_name_clashs_with_input)
    {
        split_filter_name = "__split_filter";

        for (auto & filter_output : split.first.getOutputs())
        {
            if (filter_output->result_name == filter_column_name)
            {
                filter_output = &split.first.addAlias(*filter_output, split_filter_name);
                break;
            }
        }
    }

    filter_node.step = std::make_unique<FilterStep>(
            filter_node.children.at(0)->step->getOutputHeader(),
            std::move(split.first),
            std::move(split_filter_name),
            remove_filter);

    node->step = std::make_unique<ExpressionStep>(filter_node.step->getOutputHeader(), std::move(split.second));

    filter_node.step->setStepDescription("(" + description + ")[split]");
    node->step->setStepDescription(description);

    return 2;
}

}
