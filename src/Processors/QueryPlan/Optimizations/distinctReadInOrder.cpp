#include <memory>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Common/typeid_cast.h>
#include "Processors/QueryPlan/IQueryPlanStep.h"
#include "Storages/SelectQueryInfo.h"

namespace DB::QueryPlanOptimizations
{

///
size_t tryDistinctReadInOrder(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    /// walk through the plan
    /// (1) check if there is preliminary distinct node
    /// (2) check if nodes below preliminary distinct preserve sorting
    QueryPlan::Node * node = parent_node;
    DistinctStep * pre_distinct = nullptr;
    while (!node->children.empty())
    {
        if (pre_distinct)
        {
            /// check if nodes below DISTINCT preserve sorting
            auto * step = typeid_cast<ITransformingStep *>(node->step.get());
            if (step)
            {
                const ITransformingStep::DataStreamTraits & traits = step->getDataStreamTraits();
                if (!traits.preserves_sorting)
                    return 0;
            }
        }
        if (auto * tmp = typeid_cast<DistinctStep *>(node->step.get()); tmp)
        {
            if (tmp->isPreliminary())
                pre_distinct = tmp;
        }
        node = node->children.front();
    }
    if (!pre_distinct)
        return 0;

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_merge_tree)
        return 0;

    /// check if reading in order is already there
    const DataStream & output = read_from_merge_tree->getOutputStream();
    if (output.sort_mode != DataStream::SortMode::Chunk)
        return 0;

    const SortDescription & sort_desc = output.sort_description;
    const auto & distinct_columns = pre_distinct->getOutputStream().distinct_columns;
    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be benefitial sorting key is prefix of columns in DISTINCT
    if (sort_desc.size() < distinct_columns.size())
        return 0;

    /// check if DISTINCT has the same columns as sorting key
    SortDescription distinct_sort_desc;
    distinct_sort_desc.reserve(sort_desc.size());
    for (const auto & column_desc : sort_desc)
    {
        if (distinct_columns.end() == std::find(begin(distinct_columns), end(distinct_columns), column_desc.column_name))
            break;

        distinct_sort_desc.push_back(column_desc);
    }
    if (!distinct_sort_desc.empty())
        return 0;

    InputOrderInfoPtr order_info
        = std::make_shared<const InputOrderInfo>(distinct_sort_desc, distinct_sort_desc.size(), 1, pre_distinct->getLimitHint());
    read_from_merge_tree->setQueryInfoInputOrderInfo(order_info);

    /// update data stream's sorting properties
    pre_distinct->updateInputStream(read_from_merge_tree->getOutputStream());

    return 0;
}

}
