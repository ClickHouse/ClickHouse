#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/topKDynamicFilter.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <optional>
#include <unordered_map>

namespace DB::QueryPlanOptimizations
{

/// The preliminary `DISTINCT` keeps up to `1.5 * LIMIT` rows per stream; the same cap as the `GROUP BY` top-K heap.
static constexpr size_t max_distinct_top_k = 100000;

/// Adds an expression below the preliminary `DISTINCT` that computes the sort columns the expression above it computes,
/// under new names, and points the sort description of the top-K to them. The expression above computes them again
/// from the rows the preliminary `DISTINCT` passes, which are few, so it is left as is: it only gets an unused input for each.
///
/// The values must be the same below and above, so the computation must be deterministic. It runs on all the input rows
/// instead of only the deduplicated ones, which the plain plan does too when the preliminary `DISTINCT` gives up on
/// mostly unique input (`allow_preliminary_distinct_abandoning`).
static bool computeSortColumnsBelow(
    const std::vector<const ActionsDAG::Node *> & computed_sort_nodes,
    SortDescription & pre_distinct_sort_description,
    ExpressionStep & computing_expression_step,
    QueryPlan::Node & pre_distinct_node,
    QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & settings)
{
    ActionsDAG::NodeRawConstPtrs outputs;
    for (const auto * computed_node : computed_sort_nodes)
        if (computed_node)
            outputs.push_back(computed_node);

    ActionsDAG dag = ActionsDAG::cloneSubDAG(outputs, /*remove_aliases=*/ false);
    for (const auto & dag_node : dag.getNodes())
        if (!dag_node.isDeterministic() || dag_node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

    if (pre_distinct_node.children.size() != 1)
        return false;
    auto * input_node = pre_distinct_node.children.front();
    const auto input_header = input_node->step->getOutputHeader();

    /// The new names of the computed sort columns, in the order of `outputs`.
    ActionsDAG::NodeRawConstPtrs new_outputs;
    Names computed_names;
    for (size_t i = 0; i < dag.getOutputs().size(); ++i)
    {
        String name = fmt::format("__distinct_top_k_sort_{}", i);
        if (input_header->has(name))
            return false;
        new_outputs.push_back(&dag.addAlias(*dag.getOutputs()[i], name));
        computed_names.push_back(std::move(name));
    }

    /// The expression passes through every input column.
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs;
    for (const auto * input : dag.getInputs())
        inputs.emplace(input->result_name, input);

    ActionsDAG::NodeRawConstPtrs passed_through;
    for (const auto & column : *input_header)
    {
        auto it = inputs.find(column.name);
        passed_through.push_back(it != inputs.end() ? it->second : &dag.addInput(column.name, column.type));
    }

    dag.getOutputs() = std::move(passed_through);
    dag.getOutputs().insert(dag.getOutputs().end(), new_outputs.begin(), new_outputs.end());

    auto & computing_node = nodes.emplace_back();
    computing_node.step = std::make_unique<ExpressionStep>(input_header, std::move(dag));
    computing_node.step->setStepDescription("Sort columns for top-K DISTINCT", settings.max_step_description_length);
    computing_node.children = {input_node};
    pre_distinct_node.children = {&computing_node};

    pre_distinct_node.step->updateInputHeader(computing_node.step->getOutputHeader());

    const auto & pre_distinct_header = *pre_distinct_node.step->getOutputHeader();
    auto & computing_expression = computing_expression_step.getExpression();
    for (const auto & name : computed_names)
        computing_expression.addInput(name, pre_distinct_header.getByName(name).type);
    computing_expression_step.updateInputHeader(pre_distinct_node.step->getOutputHeader());

    size_t computed_index = 0;
    for (size_t i = 0; i < computed_sort_nodes.size(); ++i)
        if (computed_sort_nodes[i])
            pre_distinct_sort_description[i].column_name = computed_names[computed_index++];

    return true;
}

/// Matches `Limit -> Distinct -> Sorting -> (Expression) -> Distinct (preliminary)`, the plan of `SELECT DISTINCT ... ORDER BY ... LIMIT N`,
/// and lets the preliminary `DISTINCT` keep only the `N` keys with the best rows in the sort order (see `DistinctTopKTransform`).
/// When its streams are already sorted (read in order), the preliminary `DISTINCT` instead stops after the first `N` keys of each.
/// Sort columns computed right above the preliminary `DISTINCT` are computed below it for the top-K (see `computeSortColumnsBelow`).
///
/// The final `DISTINCT` keeps the first row of each key in the sorted order, and the limit keeps the first `N` keys,
/// so only the `N` keys with the best rows, each with its best row, can reach the result. A key's best row is the best
/// row of the key in the stream holding it, and the key ranks among the `N` best keys of that stream: every key better
/// there is better globally. So it is enough for each stream to pass its own `N` best keys with their best rows.
void tryOptimizeDistinctTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    /// The distributed planner splits the steps into fragments itself.
    if (settings.make_distributed_plan)
        return;

    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return;

    /// With ties, the rows past the limit equal to the last one belong to the result, but other keys may hold them.
    if (limit_step->withTies() || limit_step->alwaysReadTillEnd())
        return;

    const size_t limit = limit_step->getLimitForSorting();
    if (limit == 0)
        return;

    if (settings.max_limit_for_top_k_optimization != 0 && limit > settings.max_limit_for_top_k_optimization)
        return;

    if (limit > max_distinct_top_k)
        return;

    if (parent_node->children.size() != 1)
        return;
    auto * distinct_node = parent_node->children.front();
    auto * distinct_step = typeid_cast<DistinctStep *>(distinct_node->step.get());
    if (!distinct_step || distinct_step->isPreliminary())
        return;

    if (distinct_node->children.size() != 1)
        return;
    auto * sorting_node = distinct_node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting_step || sorting_step->hasPartitions())
        return;

    /// Each input stream of the sorting is already sorted by the whole sort description (read in order), so the first row
    /// of each key in a stream is its best row there.
    const bool input_streams_sorted = sorting_step->getType() == SortingStep::Type::MergingSorted
        || (sorting_step->getType() == SortingStep::Type::FinishSorting
            && sorting_step->getPrefixDescription().size() >= sorting_step->getSortDescription().size());

    if (sorting_step->getType() != SortingStep::Type::Full && !input_streams_sorted)
        return;

    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.empty())
        return;

    for (const auto & column_description : sort_description)
        if (column_description.with_fill)
            return;

    /// No columns means all columns are keys; the expressions below may add some.
    Names key_names = distinct_step->getColumnNames();
    if (key_names.empty())
        return;

    /// The names of the sort columns and the keys at the preliminary `DISTINCT`: the planner puts expressions between
    /// it and the sorting (`Before ORDER BY`), which may rename and reorder the columns.
    SortDescription pre_distinct_sort_description = sort_description;
    /// Only needed by the top-K: over sorted streams the sort columns may be computed above the preliminary `DISTINCT`.
    bool sort_columns_passed_through = true;
    /// The sort columns computed by the expression right above the preliminary `DISTINCT` (`ORDER BY a + b`), by the index
    /// of the sort column. The top-K computes them below the preliminary `DISTINCT` too.
    std::vector<const ActionsDAG::Node *> computed_sort_nodes(sort_description.size());
    ExpressionStep * computing_expression_step = nullptr;

    if (sorting_node->children.size() != 1)
        return;
    auto * node = sorting_node->children.front();

    while (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        const auto & expression = expression_step->getExpression();

        /// Must not change the number of rows, or compute the columns from the block the preliminary `DISTINCT` outputs.
        if (expression.hasArrayJoin() || dependsOnItsBlock(expression))
            return;

        if (node->children.size() != 1)
            return;

        const bool above_pre_distinct = typeid_cast<const DistinctStep *>(node->children.front()->step.get()) != nullptr;

        for (size_t i = 0; i < pre_distinct_sort_description.size() && sort_columns_passed_through; ++i)
        {
            auto & column_description = pre_distinct_sort_description[i];
            if (auto input_name = findPassedThroughInput(expression, column_description.column_name))
            {
                column_description.column_name = std::move(*input_name);
            }
            else if (above_pre_distinct)
            {
                computed_sort_nodes[i] = expression.tryFindInOutputs(column_description.column_name);
                computing_expression_step = expression_step;
                if (!computed_sort_nodes[i])
                    sort_columns_passed_through = false;
            }
            else
            {
                sort_columns_passed_through = false;
            }
        }

        for (auto & key_name : key_names)
        {
            auto input_name = findPassedThroughInput(expression, key_name);
            if (!input_name)
                return;
            key_name = std::move(*input_name);
        }

        node = node->children.front();
    }

    auto * pre_distinct_node = node;
    auto * pre_distinct_step = typeid_cast<DistinctStep *>(pre_distinct_node->step.get());
    if (!pre_distinct_step || !pre_distinct_step->isPreliminary() || pre_distinct_step->getTopK())
        return;

    if (pre_distinct_step->getColumnNames() != key_names)
        return;

    /// The first `limit` keys of each sorted stream, each with its first row, are its `limit` best keys with their best rows,
    /// so the preliminary `DISTINCT` may stop reading its stream after them.
    if (input_streams_sorted)
    {
        pre_distinct_step->updateLimitHint(limit);
        /// The buffers read ahead of the merge, as for `ORDER BY` without a limit, but each stream stops after a few keys.
        /// `ORDER BY ... LIMIT` does not buffer either (see `optimizeReadInOrder`).
        sorting_step->disableBuffering();

        /// Like a filter, the preliminary `DISTINCT` makes the number of rows to read unknown, but at least `limit`. With the
        /// estimation the reading starts with small tasks instead of reading a block of each stream before it can stop.
        for (auto * read_node = pre_distinct_node; read_node->children.size() == 1;)
        {
            read_node = read_node->children.front();
            if (auto * read_step = typeid_cast<ReadFromMergeTree *>(read_node->step.get()))
            {
                if (read_step->readsInOrder())
                    read_step->setQueryTaskSizeLimitIfNotSet(limit);
                break;
            }
            if (!typeid_cast<ExpressionStep *>(read_node->step.get()) && !typeid_cast<FilterStep *>(read_node->step.get()))
                break;
        }
        LOG_TRACE(getLogger("optimizeDistinctTopK"), "The preliminary DISTINCT over sorted streams stops after {} keys", limit);
        return;
    }

    if (!sort_columns_passed_through)
        return;

    /// The stream is sorted by a prefix of the keys, and the in-order `DISTINCT` already holds at most one range of keys.
    if (!pre_distinct_step->getSortDescription().empty())
        return;

    /// The sorting may also sort by a subcolumn, which the transform does not look up.
    const auto & header = *pre_distinct_step->getOutputHeader();
    for (size_t i = 0; i < pre_distinct_sort_description.size(); ++i)
        if (!computed_sort_nodes[i] && !header.has(pre_distinct_sort_description[i].column_name))
            return;

    if (computing_expression_step
        && !computeSortColumnsBelow(
            computed_sort_nodes, pre_distinct_sort_description, *computing_expression_step, *pre_distinct_node, nodes, settings))
        return;

    TopKThresholdTrackerPtr threshold_tracker;
    const auto & first_sort_column = pre_distinct_sort_description.front();
    if (settings.use_top_k_dynamic_filtering && !computed_sort_nodes.front() && !first_sort_column.collator
        && pre_distinct_node->children.size() == 1)
    {
        std::vector<int> directions;
        std::vector<int> nulls_directions;
        for (const auto & column_description : pre_distinct_sort_description)
        {
            directions.push_back(column_description.direction);
            nulls_directions.push_back(column_description.nulls_direction);
        }

        /// The boundary depends on the keys: the more distinct keys per value of the sort columns, the tighter it is.
        const auto & pre_distinct_header = *pre_distinct_step->getInputHeaders().front();
        TopKDynamicFilterConsumer consumer{
            .column_name = first_sort_column.column_name,
            .limit = limit,
            .num_ranked_columns = pre_distinct_sort_description.size(),
            .directions = directions,
            .nulls_directions = nulls_directions,
            .salt_mark = "distinct_top_k",
            .add_salt = [&](SipHash & hash)
            {
                hash.update(key_names.size());
                for (const auto & key_name : key_names)
                {
                    hash.update(key_name);
                    hash.update(pre_distinct_header.getByName(key_name).type->getName());
                }
                hash.update(pre_distinct_sort_description.size());
                for (const auto & column_description : pre_distinct_sort_description)
                {
                    hash.update(column_description.column_name);
                    hash.update(pre_distinct_header.getByName(column_description.column_name).type->getName());
                }
            },
        };

        threshold_tracker = tryAttachTopKDynamicFilter(
            pre_distinct_node->children.front(), consumer, settings, nodes, getLogger("optimizeDistinctTopK"));
    }

    pre_distinct_step->applyTopK(
        {.k = limit, .sort_description = std::move(pre_distinct_sort_description), .threshold_tracker = std::move(threshold_tracker)});

    LOG_TRACE(getLogger("optimizeDistinctTopK"), "The preliminary DISTINCT keeps the {} best keys of each stream", limit);
}

}
