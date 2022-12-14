#include <memory>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>
#include "Processors/QueryPlan/ExpressionStep.h"

namespace DB::QueryPlanOptimizations
{

static std::set<std::string_view> getDistinctColumns(const DistinctStep * distinct)
{
    /// find non-const columns in DISTINCT
    const ColumnsWithTypeAndName & distinct_columns = distinct->getOutputStream().header.getColumnsWithTypeAndName();
    std::set<std::string_view> non_const_columns;
    for (const auto & column : distinct_columns)
    {
        if (!isColumnConst(*column.column))
            non_const_columns.emplace(column.name);
    }
    return non_const_columns;
}

size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/)
{
    if (parent_node->children.empty())
        return 0;

    /// check if it is preliminary distinct node
    QueryPlan::Node * distinct_node = nullptr;
    DistinctStep * distinct_step = typeid_cast<DistinctStep *>(parent_node->children.front()->step.get());
    if (!distinct_step || !distinct_step->isPreliminary())
        return 0;

    distinct_node = parent_node->children.front();

    DistinctStep * inner_distinct_step = nullptr;
    QueryPlan::Node * node = distinct_node;
    while (!node->children.empty())
    {
        node = node->children.front();
        inner_distinct_step = typeid_cast<DistinctStep *>(node->step.get());
        if (inner_distinct_step)
            break;
    }
    if (!inner_distinct_step)
        return 0;

    if (getDistinctColumns(inner_distinct_step) != getDistinctColumns(inner_distinct_step))
        return 0;

    chassert(!distinct_node->children.empty());

    /// delete current distinct
    parent_node->children[0] = distinct_node->children.front();

    return 1;
}

}
