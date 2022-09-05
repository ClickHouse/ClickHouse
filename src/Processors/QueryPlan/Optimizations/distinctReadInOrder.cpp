#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

///
size_t tryDistinctReadInOrder(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    /// check if storage already read in order
    QueryPlan::Node * node = parent_node;
    DistinctStep const * pre_distinct = nullptr;
    while (!node->children.empty())
    {
        if (pre_distinct)
        {
            /// check if nodes below DISTINCT preserve sorting
            const auto * step = typeid_cast<const ITransformingStep *>(node->step.get());
            if (step)
            {
                const ITransformingStep::DataStreamTraits & traits = step->getDataStreamTraits();
                if (!traits.preserves_sorting)
                    return 0;
            }
        }
        if (auto const * tmp = typeid_cast<const DistinctStep *>(node->step.get()); tmp)
        {
            if (tmp->isPreliminary())
                pre_distinct = tmp;
        }
        node = node->children.front();
    }
    if (!pre_distinct)
        return 0;

    auto const * storage = typeid_cast<ReadFromMergeTree const *>(node->step.get());
    if (!storage)
        return 0;

    const DataStream & output = storage->getOutputStream();
    if (output.sort_mode != DataStream::SortMode::Chunk)
        return 0;

    /// check if DISTINCT has the same columns as sorting key
    const SortDescription & sort_desc = output.sort_description;
    const DataStream & distinct_output = pre_distinct->getOutputStream();
    const auto & distinct_columns = distinct_output.distinct_columns;
    auto it = sort_desc.begin();
    for (; it != sort_desc.end(); ++it)
    {
        if (distinct_columns.end() == std::find(begin(distinct_columns), end(distinct_columns), it->column_name))
            break;
    }
    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be benefitial sorting key is prefix of columns in DISTINCT
    if (it != sort_desc.end())
        return 0;

    /// if so, set storage to read in order


    return 0;
}

}
