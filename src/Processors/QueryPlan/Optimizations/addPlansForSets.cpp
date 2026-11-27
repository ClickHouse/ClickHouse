#include <memory>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/BlocksMarshallingStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

bool addPlansForSets(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & root_plan, QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(node.step.get());
    if (!delayed)
        return false;

    auto plans = DelayedCreatingSetsStep::makePlansForSets(std::move(*delayed), optimization_settings);
    node.children.reserve(1 + plans.size());

    SharedHeaders input_headers;
    input_headers.reserve(1 + plans.size());
    input_headers.push_back(node.children.front()->step->getOutputHeader());

    for (const auto & plan : plans)
    {
        input_headers.push_back(plan->getCurrentHeader());
        node.children.push_back(plan->getRootNode());
        auto [add_nodes, add_resources] = QueryPlan::detachNodesAndResources(std::move(*plan));
        nodes.splice(nodes.end(), std::move(add_nodes));
        root_plan.addResources(std::move(add_resources));
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_headers));
    creating_sets->setStepDescription("Create sets before main query execution");
    node.step = std::move(creating_sets);
    return true;
}

/// The sets of a `DelayedCreatingSetsStep` are used only below it, and no row reaches the steps above an
/// always-false filter or a `LIMIT` that takes no row, so none of its sets is needed.
void removeSetsOfAlwaysFalseScope(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(node.step.get());
    if (!delayed || delayed->getSets().empty() || node.children.size() != 1)
        return;

    QueryPlan::Node * cut = nullptr;
    for (auto * current = node.children.front(); current->children.size() == 1; current = current->children.front())
    {
        if (const auto * filter = typeid_cast<const FilterStep *>(current->step.get()))
        {
            const auto * filter_node = filter->getExpression().tryFindInOutputs(filter->getFilterColumnName());
            if (!filter_node || !filter_node->column || !isColumnConst(*filter_node->column)
                || !ConstantFilterDescription(*filter_node->column).always_false)
                continue;

            cut = current;
            break;
        }

        /// Unless it reads till end (for totals or `exact_rows_before_limit`), `LIMIT 0` reads no row of its input.
        if (const auto * limit = typeid_cast<const LimitStep *>(current->step.get()))
        {
            if (limit->getLimitWithOffset() == 0 && !limit->alwaysReadTillEnd())
                cut = current;
            break;
        }

        if (!typeid_cast<const ExpressionStep *>(current->step.get())
            && !typeid_cast<const DelayedCreatingSetsStep *>(current->step.get())
            && !typeid_cast<const ExtremesStep *>(current->step.get())
            && !typeid_cast<const BlocksMarshallingStep *>(current->step.get()))
            return;
    }

    if (!cut)
        return;

    auto *& input = cut->children.front();
    if (canProduceTotals(*input))
        return;

    if (!typeid_cast<const ReadNothingStep *>(input->step.get()))
    {
        auto & empty_node = nodes.emplace_back();
        empty_node.step = std::make_unique<ReadNothingStep>(input->step->getOutputHeader());
        input = &empty_node;
    }

    std::ignore = delayed->detachSets();
}

}
