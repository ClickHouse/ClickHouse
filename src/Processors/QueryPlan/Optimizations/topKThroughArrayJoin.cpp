#include <Core/SortDescription.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// with parallel replicas each replica keeps only its own top-n, and with read in order the second pass of
/// `optimizeReadInOrder` reads only the rows the limit keeps anyway
bool shouldSkipTopKAboveMergeTreeInput(
    QueryPlan::Node & input_node,
    const SortingStep & sort_step,
    const SortDescription & description,
    size_t limit,
    const Optimization::ExtraSettings & settings)
{
    const auto * reading = findMergeTreeRead(&input_node);
    if (!reading)
        return false;

    if (reading->isParallelReadingFromReplicas())
        return true;

    if (!settings.read_in_order)
        return false;

    /// the second pass does not see every read, e.g. not one below a final `DISTINCT`
    if (!findReadingStepForReadInOrder(input_node, settings.read_in_order_through_join))
        return false;

    /// a reverse read may be impossible (`FINAL`)
    SortingStep probe_sort_step(input_node.step->getOutputHeader(), description, limit, sort_step.getSettings());
    const auto order_info = getInputOrderIfReadInOrderIsUseful(probe_sort_step, reading->getStorageMetadata()->getSortingKey(), input_node);
    return order_info && (order_info->direction == 1 || reading->canReadInReverseOrder());
}

/// an inner `ARRAY JOIN` drops a row only when all its arrays are empty, a constant condition needs no filter
void addArrayJoinEmptinessFilter(const ArrayJoinStep & array_join, QueryPlan::Node *& input_node, QueryPlan::Nodes & nodes)
{
    ActionsDAG dag(input_node->step->getOutputHeader()->getColumnsWithTypeAndName());
    auto not_empty_function = FunctionFactory::instance().get("notEmpty", nullptr);

    ActionsDAG::NodeRawConstPtrs not_empty;
    for (const auto & name : array_join.getColumns())
        not_empty.push_back(&dag.addFunction(not_empty_function, {&dag.findInOutputs(name)}, {}));

    const auto * condition = not_empty.front();
    if (not_empty.size() > 1)
        condition = &dag.addFunction(FunctionFactory::instance().get("or", nullptr), std::move(not_empty), {});

    if (condition->column)
        return;

    dag.getOutputs().push_back(condition);
    String filter_column_name = condition->result_name;

    auto & filter_node = nodes.emplace_back();
    filter_node.children.push_back(input_node);
    filter_node.step = std::make_unique<FilterStep>(
        input_node->step->getOutputHeader(),
        std::move(dag),
        std::move(filter_column_name),
        /*remove_filter_column_=*/ true);
    filter_node.step->setStepDescription("Non-empty arrays for ARRAY JOIN");
    input_node = &filter_node;
}

}

/// move the sort below `ARRAY JOIN` when no sort key is a joined column: an output row has the keys of its input row,
/// so the top-n input rows give the top-n output rows if each of them gives at least one row
size_t tryTopKThroughArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// `WITH TIES` needs all rows equal to the last one
    if (limit_step->withTies())
        return 0;

    /// `WITH TOTALS` and `exact_rows_before_limit` need all rows
    if (limit_step->alwaysReadTillEnd())
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * sort_node = parent_node->children.front();
    auto * sort_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sort_step)
        return 0;

    /// a sorted input already stops early, and a limit below a partitioned sort is not the top-n of a partition
    if (sort_step->getType() != SortingStep::Type::Full || sort_step->hasPartitions())
        return 0;

    if (sort_step->hasLimitByHint())
        return 0;

    if (sort_node->children.size() != 1)
        return 0;

    SortDescription description = sort_step->getSortDescription();
    QueryPlan::Node * first_node_below_sort = sort_node->children.front();
    QueryPlan::Node * array_join_node = first_node_below_sort;
    if (!peelPassThroughExpressions(array_join_node, description))
        return 0;

    auto * array_join_step = typeid_cast<ArrayJoinStep *>(array_join_node->step.get());
    /// a fused element filter can empty a non-empty array
    if (!array_join_step || array_join_step->hasElementFilter() || array_join_node->children.size() != 1)
        return 0;

    /// aligned arrays must throw `SIZES_OF_ARRAYS_DONT_MATCH` also for the rows the moved sort drops
    const auto & array_join_columns = array_join_step->getColumns();
    if (array_join_columns.size() > 1 && !array_join_step->isUnaligned())
        return 0;

    /// a joined column keeps its name, so the input header alone does not tell that a key is joined
    const auto & array_join_input_header = array_join_step->getInputHeaders().front();
    for (const auto & sort_column : description)
        if (std::ranges::contains(array_join_columns, sort_column.column_name) || !array_join_input_header->has(sort_column.column_name))
            return 0;

    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    QueryPlan::Node * array_join_input_node = array_join_node->children.front();
    if (shouldSkipTopKAboveMergeTreeInput(*array_join_input_node, *sort_step, description, n, settings))
        return 0;

    /// `LEFT ARRAY JOIN` gives a row for each input row
    if (!array_join_step->isLeft())
        addArrayJoinEmptinessFilter(*array_join_step, array_join_input_node, nodes);

    auto moved_sort_step = std::make_unique<SortingStep>(
        array_join_input_node->step->getOutputHeader(),
        description,
        n,
        sort_step->getSettings());
    moved_sort_step->setStepDescription(*sort_step);

    sort_node->step = std::move(moved_sort_step);
    sort_node->children[0] = array_join_input_node;
    array_join_node->children[0] = sort_node;
    array_join_step->updateInputHeader(sort_node->step->getOutputHeader());
    parent_node->children[0] = first_node_below_sort;

    /// depth to optimize again: `ArrayJoin -> Sorting -> [Filter]` and one `Expression` above
    return 4;
}

}
