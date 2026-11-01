#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <DataTypes/IDataType.h>

#include <unordered_map>


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

    /// With a large limit, every stream keeps and sorts `limit` rows, and the partial sorting is faster
    /// (with 100 million rows in 16 streams, the shuffle is faster up to `LIMIT 300000`, and slower with `LIMIT 1000000`).
    if (limit > 4 * sort_settings.max_block_size)
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

    const ActionsDAG::Node * key_output = dag.tryFindInOutputs(key_column_name);
    if (!key_output)
        return;

    std::unordered_map<const ActionsDAG::Node *, size_t> num_parents;
    for (const auto & dag_node : dag.getNodes())
        for (const auto * child : dag_node.children)
            ++num_parents[child];

    std::unordered_map<const ActionsDAG::Node *, size_t> num_outputs;
    for (const auto * output : dag.getOutputs())
        ++num_outputs[output];

    /// The values must not be returned in another column or used to calculate other columns.
    /// The key may be an alias of `rand()` (e.g. `SELECT rand() AS r ... ORDER BY r`).
    const ActionsDAG::Node * key_node = key_output;
    if (num_outputs[key_node] != 1 || num_parents[key_node] != 0)
        return;

    while (key_node->type == ActionsDAG::ActionType::ALIAS)
    {
        key_node = key_node->children.front();
        if (num_parents[key_node] != 1 || num_outputs.contains(key_node))
            return;
    }

    if (key_node->type != ActionsDAG::ActionType::FUNCTION || !key_node->function_base)
        return;

    const auto & function_name = key_node->function_base->getName();
    if (function_name != "rand" && function_name != "rand64")
        return;

    WhichDataType which(key_node->result_type);
    if (!which.isUInt32() && !which.isUInt64())
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
