#include <memory>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{
/// build actions DAG from stack of steps
static std::optional<ActionsDAG> buildActionsForPlanPath(std::vector<const ActionsDAG *> & dag_stack)
{
    if (dag_stack.empty())
        return {};

    ActionsDAG path_actions = dag_stack.back()->clone();
    dag_stack.pop_back();
    while (!dag_stack.empty())
    {
        ActionsDAG clone = dag_stack.back()->clone();
        dag_stack.pop_back();
        path_actions.mergeInplace(std::move(clone));
    }
    return path_actions;
}

static std::set<std::string>
getOriginalDistinctColumns(const ColumnsWithTypeAndName & distinct_columns, std::vector<const ActionsDAG *> & dag_stack)
{
    auto actions = buildActionsForPlanPath(dag_stack);
    FindOriginalNodeForOutputName original_node_finder(*actions);
    std::set<std::string> original_distinct_columns;
    for (const auto & column : distinct_columns)
    {
        /// const columns doesn't affect DISTINCT, so skip them
        if (isColumnConst(*column.column))
            continue;

        const auto * input_node = original_node_finder.find(column.name);
        if (!input_node)
            break;

        original_distinct_columns.insert(input_node->result_name);
    }
    return original_distinct_columns;
}

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
    /// (3) gather actions DAG to find original names for columns in distinct step later
    std::vector<ITransformingStep *> steps_to_update;
    QueryPlan::Node * node = parent_node;
    std::vector<const ActionsDAG *> dag_stack;
    while (!node->children.empty())
    {
        auto * step = dynamic_cast<ITransformingStep *>(node->step.get());
        if (!step)
            return 0;

        const ITransformingStep::DataStreamTraits & traits = step->getDataStreamTraits();
        if (!traits.preserves_sorting)
            return 0;

        steps_to_update.push_back(step);

        if (const auto * const expr = typeid_cast<const ExpressionStep *>(step); expr)
            dag_stack.push_back(&expr->getExpression());
        else if (const auto * const filter = typeid_cast<const FilterStep *>(step); filter)
            dag_stack.push_back(&filter->getExpression());

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

    /// get original names for DISTINCT columns
    const ColumnsWithTypeAndName & distinct_columns = pre_distinct->getOutputStream().header.getColumnsWithTypeAndName();
    auto original_distinct_columns = getOriginalDistinctColumns(distinct_columns, dag_stack);

    /// check if DISTINCT has the same columns as sorting key
    const Names & sorting_key_columns = read_from_merge_tree->getStorageMetadata()->getSortingKeyColumns();
    size_t number_of_sorted_distinct_columns = 0;
    for (const auto & column_name : sorting_key_columns)
    {
        if (!original_distinct_columns.contains(column_name))
            break;

        ++number_of_sorted_distinct_columns;
    }

    /// apply optimization only when distinct columns match or form prefix of sorting key
    /// todo: check if reading in order optimization would be beneficial when sorting key is prefix of columns in DISTINCT
    if (number_of_sorted_distinct_columns != original_distinct_columns.size())
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
