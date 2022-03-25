#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <base/logger_useful.h>
#include <Poco/Logger.h>

namespace DB::QueryPlanOptimizations
{

void swapSortingAndUnnecessaryCalculation(QueryPlan::Node * parent_node, ActionsDAGPtr && actions)
{
    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent_step = parent_node->step;
    auto & child_step = child_node->step;
    auto * sorting_step = typeid_cast<SortingStep *>(parent_step.get());

    // Sorting -> UnnecessaryCalculations
    std::swap(parent_step, child_step);
    // UnnecessaryCalculations -> Sorting

    sorting_step->updateInputStream(child_node->children.at(0)->step->getOutputStream());
    auto input_header = child_step->getInputStreams().at(0).header;
    sorting_step->updateOutputStream(input_header);
    parent_step = std::make_unique<ExpressionStep>(child_step->getOutputStream(), std::move(actions));
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

    for (auto sc : sort_columns)
        LOG_TRACE(&Poco::Logger::get("Optimizer"), "sort_columns: {}", fmt::join(sort_columns, ", "));

    auto split_actions = expression->splitActionsBySortingDescription(sort_columns);
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "source: {}", expression->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "first: {}", split_actions.first->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "second: {}", split_actions.second->dumpDAG());

    // No calculations can be postponed.
    if (split_actions.second->trivial())
        return 0;

    // Everything can be done after the sorting.
    if (split_actions.first->trivial())
    {
        swapSortingAndUnnecessaryCalculation(parent_node, std::move(split_actions.second));
        return 2;
    }

    // Sorting -> Expression
    auto & node = nodes.emplace_back();

    node.children.swap(child_node->children);
    child_node->children.emplace_back(&node);

    node.step = std::make_unique<ExpressionStep>(node.children.at(0)->step->getOutputStream(), std::move(split_actions.first));
    // Sorting (parent_node) -> UnnecessaryCalculations (child_node) -> NecessaryCalculations (node)
    swapSortingAndUnnecessaryCalculation(parent_node, std::move(split_actions.second));
    // UnnecessaryCalculations (child_node) -> Sorting (parent_node) -> NecessaryCalculations (node)

    return 3;
}
}
