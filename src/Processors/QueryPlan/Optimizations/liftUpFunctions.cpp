#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <base/logger_useful.h>
#include <Poco/Logger.h>

namespace DB::QueryPlanOptimizations
{

void swapSortingAndUnnecessaryCalculation(QueryPlan::Node * parent_node, ActionsDAGPtr && unneeded_for_sorting)
{
    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent_step = parent_node->step;
    auto & child_step = child_node->step;
    auto * sorting_step = typeid_cast<SortingStep *>(parent_step.get());

    // Sorting -> Expression
    std::swap(parent_step, child_step);
    // Expression -> Sorting

    sorting_step->updateInputStream(child_node->children.at(0)->step->getOutputStream());
    LOG_TRACE(
        &Poco::Logger::get("Optimizer"), "New Sorting input header: {}", sorting_step->getInputStreams().at(0).header.dumpStructure());
    auto input_header = sorting_step->getInputStreams().at(0).header;
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "Old Sorting output header: {}", sorting_step->getOutputStream().header.dumpStructure());
    sorting_step->updateOutputStream(std::move(input_header));
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "New Sorting output header: {}", sorting_step->getOutputStream().header.dumpStructure());
    auto description = parent_node->step->getStepDescription();
    parent_step = std::make_unique<ExpressionStep>(child_step->getOutputStream(), std::move(unneeded_for_sorting));
    LOG_TRACE(
        &Poco::Logger::get("Optimizer"), "New Expression input header: {}", parent_step->getInputStreams().at(0).header.dumpStructure());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "New Expression output header: {}", parent_step->getOutputStream().header.dumpStructure());
    parent_step->setStepDescription(description + " [lifted up part]");
    // UnneededCalculations -> Sorting
}

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

    NameSet sort_columns;
    for (const auto & col : sorting_step->getSortDescription())
        sort_columns.insert(col.column_name);
    const auto & expression = expression_step->getExpression();
    auto [needed_for_sorting, unneeded_for_sorting] = expression->splitActionsBySortingDescription(sort_columns);
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "Original Expression: {}", expression->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "Needed for Sorting: {}", needed_for_sorting->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "Unneeded for Sorting: {}", unneeded_for_sorting->dumpDAG());

    auto description = child_step->getStepDescription();

    // No calculations can be postponed.
    if (unneeded_for_sorting->trivial())
        return 0;

    // Everything can be done after the sorting.
    /*if (needed_for_sorting->trivial())
    {
        swapSortingAndUnnecessaryCalculation(parent_node, std::move(unneeded_for_sorting));
        return 2;
    }*/

    // Sorting (parent_node) -> Expression (child_node)
    auto & node_with_needed = nodes.emplace_back();
    node_with_needed.children.swap(child_node->children);
    child_node->children.emplace_back(&node_with_needed);
    node_with_needed.step
        = std::make_unique<ExpressionStep>(node_with_needed.children.at(0)->step->getOutputStream(), std::move(needed_for_sorting));
    node_with_needed.step->setStepDescription(std::move(description));

    // Sorting (parent_node) -> so far the origin Expression (child_node) -> NeededCalculations (node_with_needed)
    swapSortingAndUnnecessaryCalculation(parent_node, std::move(unneeded_for_sorting));
    // UneededCalculations (child_node) -> Sorting (parent_node) -> NeededCalculations (node_with_needed)

    return 3;
}
}
