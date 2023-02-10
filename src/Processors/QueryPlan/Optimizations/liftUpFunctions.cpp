#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

const DB::DataStream & getChildOutputStream(DB::QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node \"{}\" is expected to have only one child.", node.step->getName());
    return node.children.front()->step->getOutputStream();
}

}

namespace DB::QueryPlanOptimizations
{

size_t tryExecuteFunctionsAfterSorting(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent_step = parent_node->step;
    auto & child_step = child_node->step;
    auto * sorting_step = typeid_cast<SortingStep *>(parent_step.get());
    auto * expression_step = typeid_cast<ExpressionStep *>(child_step.get());

    if (!sorting_step || !expression_step)
        return 0;

    // Filling step position should be preserved
    if (!child_node->children.empty())
        if (typeid_cast<FillingStep *>(child_node->children.front()->step.get()))
            return 0;

    NameSet sort_columns;
    for (const auto & col : sorting_step->getSortDescription())
        sort_columns.insert(col.column_name);
    auto [needed_for_sorting, unneeded_for_sorting] = expression_step->getExpression()->splitActionsBySortingDescription(sort_columns);

    // No calculations can be postponed.
    if (unneeded_for_sorting->trivial())
        return 0;

    // Sorting (parent_node) -> Expression (child_node)
    auto & node_with_needed = nodes.emplace_back();
    std::swap(node_with_needed.children, child_node->children);
    child_node->children = {&node_with_needed};

    node_with_needed.step = std::make_unique<ExpressionStep>(getChildOutputStream(node_with_needed), std::move(needed_for_sorting));
    node_with_needed.step->setStepDescription(child_step->getStepDescription());
    // Sorting (parent_node) -> so far the origin Expression (child_node) -> NeededCalculations (node_with_needed)

    std::swap(parent_step, child_step);
    // so far the origin Expression (parent_node) -> Sorting (child_node) -> NeededCalculations (node_with_needed)

    sorting_step->updateInputStream(getChildOutputStream(*child_node));

    auto description = parent_step->getStepDescription();
    parent_step = std::make_unique<DB::ExpressionStep>(child_step->getOutputStream(), std::move(unneeded_for_sorting));
    parent_step->setStepDescription(description + " [lifted up part]");
    // UneededCalculations (parent_node) -> Sorting (child_node) -> NeededCalculations (node_with_needed)

    return 3;
}
}
