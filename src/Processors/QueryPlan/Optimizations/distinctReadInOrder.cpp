#include <memory>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Common/typeid_cast.h>

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
    QueryPlan::Node * pre_distinct_node = nullptr;
    while (!node->children.empty())
    {
        if (pre_distinct)
        {
            /// check if nodes below DISTINCT preserve sorting
            const auto * step = dynamic_cast<const ITransformingStep *>(node->step.get());
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
            {
                pre_distinct_node = node;
                pre_distinct = tmp;
            }
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
    if (output.sort_scope != DataStream::SortScope::Chunk)
        return 0;

    const SortDescription & sort_desc = output.sort_description;
    const auto & distinct_columns = pre_distinct->getOutputStream().header.getColumnsWithTypeAndName();
    std::vector<std::string_view> non_const_columns;
    non_const_columns.reserve(distinct_columns.size());
    for (const auto & column : distinct_columns)
    {
        if (!isColumnConst(*column.column))
            non_const_columns.emplace_back(column.name);
    }

    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be beneficial when sorting key is prefix of columns in DISTINCT
    if (sort_desc.size() < non_const_columns.size())
        return 0;

    /// check if DISTINCT has the same columns as sorting key
    SortDescription distinct_sort_desc;
    distinct_sort_desc.reserve(sort_desc.size());
    for (const auto & column_desc : sort_desc)
    {
        if (non_const_columns.end() == std::find(begin(non_const_columns), end(non_const_columns), column_desc.column_name))
            break;

        distinct_sort_desc.push_back(column_desc);
    }
    if (distinct_sort_desc.empty())
        return 0;

    const int direction = 1; /// default direction, ASC
    InputOrderInfoPtr order_info
        = std::make_shared<const InputOrderInfo>(distinct_sort_desc, distinct_sort_desc.size(), direction, pre_distinct->getLimitHint());
    read_from_merge_tree->setQueryInfoInputOrderInfo(order_info);

    /// update data stream's sorting properties
    std::vector<ITransformingStep *> steps2update;
    node = pre_distinct_node;
    while (node && node->step.get() != read_from_merge_tree)
    {
        auto * transform = dynamic_cast<ITransformingStep *>(node->step.get());
        if (transform)
            steps2update.push_back(transform);

        if (!node->children.empty())
            node = node->children.front();
        else
            node = nullptr;
    }

    const DataStream * input_stream = &read_from_merge_tree->getOutputStream();
    while (!steps2update.empty())
    {
        steps2update.back()->updateInputStream(*input_stream);
        input_stream = &steps2update.back()->getOutputStream();
        steps2update.pop_back();
    }

    return 0;
}

}
