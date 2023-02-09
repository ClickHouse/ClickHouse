#include <memory>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
size_t tryDistinctReadInOrder(QueryPlan::Node * parent_node)
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
    std::vector<ITransformingStep *> steps_to_update;
    QueryPlan::Node * node = parent_node;
    while (!node->children.empty())
    {
        auto * step = dynamic_cast<ITransformingStep *>(node->step.get());
        if (!step)
            return 0;

        const ITransformingStep::DataStreamTraits & traits = step->getDataStreamTraits();
        if (!traits.preserves_sorting)
            return 0;

        steps_to_update.push_back(step);

        node = node->children.front();
    }

    /// check if we read from MergeTree
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_merge_tree)
        return 0;

    /// if reading from merge tree doesn't provide any output order, we can do nothing
    /// it means that no ordering can provided or supported for a particular sorting key
    /// for example, tuple() or sipHash(string)
    if (read_from_merge_tree->getOutputStream().sort_description.empty())
        return 0;

    /// find non-const columns in DISTINCT
    const ColumnsWithTypeAndName & distinct_columns = pre_distinct->getOutputStream().header.getColumnsWithTypeAndName();
    std::set<std::string_view> non_const_columns;
    for (const auto & column : distinct_columns)
    {
        if (!isColumnConst(*column.column))
            non_const_columns.emplace(column.name);
    }

    const Names& sorting_key_columns = read_from_merge_tree->getStorageMetadata()->getSortingKeyColumns();
    /// check if DISTINCT has the same columns as sorting key
    size_t number_of_sorted_distinct_columns = 0;
    for (const auto & column_name : sorting_key_columns)
    {
        if (non_const_columns.end() == non_const_columns.find(column_name))
            break;

        ++number_of_sorted_distinct_columns;
    }
    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be beneficial when sorting key is prefix of columns in DISTINCT
    if (number_of_sorted_distinct_columns != non_const_columns.size())
        return 0;

    /// check if another read in order optimization is already applied
    /// apply optimization only if another read in order one uses less sorting columns
    /// example: SELECT DISTINCT a, b FROM t ORDER BY a; -- sorting key: a, b
    /// if read in order for ORDER BY is already applied, then output sort description will contain only column `a`
    /// but we need columns `a, b`, applying read in order for distinct will still benefit `order by`
    const DataStream & output_data_stream = read_from_merge_tree->getOutputStream();
    const SortDescription & output_sort_desc = output_data_stream.sort_description;
    if (output_data_stream.sort_scope != DataStream::SortScope::Chunk && number_of_sorted_distinct_columns <= output_sort_desc.size())
        return 0;

    /// update input order info in read_from_merge_tree step
    const int direction = 0; /// for DISTINCT direction doesn't matter, ReadFromMergeTree will choose proper one
    bool can_read = read_from_merge_tree->requestReadingInOrder(number_of_sorted_distinct_columns, direction, pre_distinct->getLimitHint());
    if (!can_read)
        return 0;

    /// update data stream's sorting properties for found transforms
    const DataStream * input_stream = &read_from_merge_tree->getOutputStream();
    while (!steps_to_update.empty())
    {
        steps_to_update.back()->updateInputStream(*input_stream);
        input_stream = &steps_to_update.back()->getOutputStream();
        steps_to_update.pop_back();
    }

    return 0;
}

}
