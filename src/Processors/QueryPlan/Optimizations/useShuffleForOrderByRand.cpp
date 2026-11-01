#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <DataTypes/IDataType.h>


namespace DB::QueryPlanOptimizations
{

/// Replace the full sorting by a column calculated as `rand()` or `rand64()` with `ShuffleStep`.
///
/// `ShuffleStep` returns the rows in a uniformly random order in O(n), and fills the key column with
/// independent random values of the same type in sorted order. This has exactly the same distribution
/// as calculating `rand()` for every row and sorting by it. The output is sorted by the key column,
/// so the merging of sorted streams (e.g. from remote servers) is not affected.
///
/// It is applied at the end of the optimization, so that all optimizations that work with `SortingStep`
/// (lazy materialization, distributed sorting, etc.) have already been applied.
void useShuffleForOrderByRand(QueryPlan::Node & node)
{
    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting
        || sorting->getType() != SortingStep::Type::Full
        || sorting->hasPartitions()
        || sorting->isSortingForMergeJoin()
        || sorting->hasTopKThresholdTracker())
        return;

    const auto & description = sorting->getSortDescription();
    if (description.size() != 1 || description.front().with_fill || description.front().collator)
        return;

    const UInt64 limit = sorting->getLimit();
    const auto & sort_settings = sorting->getSettings();

    /// Without a limit, the shuffle keeps all rows in memory, and it does not spill to disk as the sorting does.
    if (!limit && (sort_settings.max_bytes_in_block_before_external_sort || sort_settings.max_bytes_in_query_before_external_sort))
        return;

    /// The limits for sorting are checked by the sorting transforms.
    if (sort_settings.size_limits.hasLimits())
        return;

    if (node.children.size() != 1)
        return;

    /// The sort column must be calculated by `rand()` right before the sorting, and nothing else may depend on its value,
    /// because the values are replaced. If it was calculated earlier (e.g. used in WHERE), it is an input of this step.
    const auto * expression = typeid_cast<const ExpressionStep *>(node.children.front()->step.get());
    if (!expression)
        return;

    const auto & dag = expression->getExpression();
    const auto & key_column_name = description.front().column_name;

    const ActionsDAG::Node * key_node = dag.tryFindInOutputs(key_column_name);
    if (!key_node)
        return;

    /// The same values must not be returned in another column.
    size_t num_outputs_with_key = 0;
    for (const auto * output : dag.getOutputs())
        num_outputs_with_key += (output == key_node || output->result_name == key_column_name);

    if (num_outputs_with_key != 1)
        return;

    if (key_node->type != ActionsDAG::ActionType::FUNCTION || !key_node->function_base)
        return;

    const auto & function_name = key_node->function_base->getName();
    if (function_name != "rand" && function_name != "rand64")
        return;

    WhichDataType which(key_node->result_type);
    if (!which.isUInt32() && !which.isUInt64())
        return;

    for (const auto & dag_node : dag.getNodes())
        for (const auto * child : dag_node.children)
            if (child == key_node)
                return;

    auto shuffle = std::make_unique<ShuffleStep>(
        sorting->getInputHeaders().front(),
        key_column_name,
        description.front().direction,
        limit,
        sort_settings.max_block_size);
    shuffle->setStepDescription("Shuffle for ORDER BY rand()");
    node.step = std::move(shuffle);
}

}
