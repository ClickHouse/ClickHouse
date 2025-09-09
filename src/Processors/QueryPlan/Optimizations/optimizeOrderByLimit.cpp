#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

size_t tryPushDownOrderByLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return 0;

    if (node->children.size() != 1)
        return 0;
    node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!sorting_step)
        return 0;

    if (node->children.size() != 1)
        return 0;
    node = node->children.front();

    while (typeid_cast<ExpressionStep *>(node->step.get()) || typeid_cast<FilterStep *>(node->step.get()))
    {
        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return 0;

    /// Extract N
    size_t n = limit_step->getLimitForSorting();
    if (n > settings.max_limit_to_push_down_topn_predicate)
        return 0;

    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.front().column_name_in_storage.empty())
        return 0;

    const auto & sort_column = sorting_step->getInputHeaders().front()->getByName(sort_description.front().column_name);
    if (!sort_column.type->isValueRepresentedByNumber() || read_from_mergetree_step->getPrewhereInfo())
        return 0;

    const auto * sort_column_from_read
        = read_from_mergetree_step->getStorageMetadata()->getColumns().tryGet(sort_description.front().column_name_in_storage);
    if (!sort_column_from_read || !sort_column_from_read->type->equals(*sort_column.type))
        return 0;

    read_from_mergetree_step->setTopNFilterParams(
        {sort_description.front().column_name_in_storage, sort_column.type, n, sort_description.size() > 1});

    return 0;
}

}
