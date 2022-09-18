#include <memory>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
size_t tryDistinctReadInOrder(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    /// check if it is preliminary distinct node
    DistinctStep * pre_distinct = nullptr;
    if (auto * distinct = typeid_cast<DistinctStep *>(parent_node->step.get()); distinct)
    {
        if (distinct->isPreliminary())
            pre_distinct = distinct;
    }
    if (!pre_distinct)
        return 0;

    /// walk through the plan
    /// (1) check if nodes below preliminary distinct preserve sorting
    /// (2) gather transforming steps to update their sorting properties later
    std::vector<ITransformingStep *> steps2update;
    QueryPlan::Node * node = parent_node;
    while (!node->children.empty())
    {
        auto * step = dynamic_cast<ITransformingStep *>(node->step.get());
        if (!step)
            return 0;

        const ITransformingStep::DataStreamTraits & traits = step->getDataStreamTraits();
        if (!traits.preserves_sorting)
            return 0;

        steps2update.push_back(step);

        node = node->children.front();
    }

    /// check if we read from MergeTree
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_merge_tree)
        return 0;

    /// if some `read in order` optimization is already applied - do not proceed
    /// todo: check later case with several read in order optimizations
    if (read_from_merge_tree->getOutputStream().sort_scope != DataStream::SortScope::Chunk)
        return 0;

    /// find non-const columns in DISTINCT
    const auto & distinct_columns = pre_distinct->getOutputStream().header.getColumnsWithTypeAndName();
    std::set<std::string_view> non_const_columns;
    for (const auto & column : distinct_columns)
    {
        if (!isColumnConst(*column.column))
            non_const_columns.emplace(column.name);
    }

    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be beneficial when sorting key is prefix of columns in DISTINCT
    const SortDescription & sort_desc = read_from_merge_tree->getOutputStream().sort_description;
    if (sort_desc.size() < non_const_columns.size())
        return 0;

    /// check if DISTINCT has the same columns as sorting key
    SortDescription distinct_sort_desc;
    distinct_sort_desc.reserve(sort_desc.size());
    for (const auto & column_desc : sort_desc)
    {
        if (non_const_columns.end() == non_const_columns.find(column_desc.column_name))
            break;

        distinct_sort_desc.push_back(column_desc);
    }
    if (distinct_sort_desc.size() != non_const_columns.size())
        return 0;

    /// update input order info in read_from_merge_tree step
    const int direction = 1; /// default direction, ASC
    InputOrderInfoPtr order_info
        = std::make_shared<const InputOrderInfo>(SortDescription{}, distinct_sort_desc.size(), direction, pre_distinct->getLimitHint());
    read_from_merge_tree->setQueryInfoInputOrderInfo(order_info);

    /// update data stream's sorting properties for found transforms
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
