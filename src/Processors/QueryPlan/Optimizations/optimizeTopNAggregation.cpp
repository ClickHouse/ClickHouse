#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TopNAggregatingStep.h>
#include <Processors/TopKThresholdTracker.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <climits>

namespace DB
{
FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_);
}

namespace DB::QueryPlanOptimizations
{

static String traceColumnThroughDAG(const ActionsDAG & dag, const String & column_name)
{
    const auto * node = dag.tryFindInOutputs(column_name);
    if (!node)
        return {};

    while (true)
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
            return node->result_name;

        if (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        {
            node = node->children[0];
            continue;
        }

        return {};
    }
}

static ReadFromMergeTree * findReadFromMergeTree(QueryPlan::Node & node)
{
    auto * current = &node;
    while (current)
    {
        if (auto * read = typeid_cast<ReadFromMergeTree *>(current->step.get()))
            return read;

        if (current->children.size() != 1)
            return nullptr;

        if (typeid_cast<ExpressionStep *>(current->step.get()))
        {
            current = current->children[0];
            continue;
        }

        return nullptr;
    }
    return nullptr;
}

static String resolveOriginalArgName(const String & arg_name, QueryPlan::Node * child_node)
{
    String resolved = arg_name;
    while (child_node)
    {
        auto * child_expr = typeid_cast<ExpressionStep *>(child_node->step.get());
        if (!child_expr)
            break;

        auto traced = traceColumnThroughDAG(child_expr->getExpression(), resolved);
        if (!traced.empty())
            resolved = traced;

        if (child_node->children.size() != 1)
            break;
        child_node = child_node->children[0];
    }
    return resolved;
}

/**
 * Fuse GROUP BY ... ORDER BY aggregate LIMIT K into a single TopNAggregatingStep.
 *
 * Required plan pattern:
 *   LimitStep(K, no WITH TIES, no OFFSET)
 *     -> SortingStep(single column = aggregate output)
 *       -> [ExpressionStep] (optional column renames)
 *         -> AggregatingStep (no grouping sets, no overflow row)
 *
 * Applicability criteria:
 *   1. ORDER BY references exactly one column which is the output of an aggregate.
 *   2. That aggregate's getTopKAggregateInfo().determined_by_first_row_direction != 0.
 *   3. ALL aggregates in the query satisfy the same direction constraint.
 *   4. No HAVING, TOTALS, WITH TIES, grouping sets, overflow row, or OFFSET.
 *
 * Mode selection:
 *   Mode 1 (sorted_input): enabled when the ORDER BY aggregate's argument is the
 *     first column of the MergeTree sorting key AND output_ordered_by_sort_key is
 *     true (min/max only). Requests in-order reading for early termination.
 *   Mode 2 (threshold pruning): enabled only when the ORDER BY aggregate has
 *     output_ordered_by_sort_key=true, data is read from MergeTree without existing
 *     prewhere, and the aggregate argument is numeric + non-nullable. This is a
 *     conservative correctness gate: threshold comparisons in TopKThresholdTracker
 *     use plain direction-based ordering and do not model NULL ordering or string
 *     collation semantics. Pushes a __topKFilter prewhere to ReadFromMergeTree for
 *     granule-level skipping.
 *     Without prewhere, the standard Aggregator pipeline is faster due to its
 *     type-dispatched hashing (the custom SipHash-based approach cannot compete).
 */
void optimizeTopNAggregation(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(node.step.get());
    if (!limit_step || limit_step->getLimit() == 0 || limit_step->withTies() || limit_step->getOffset() > 0)
        return;

    if (node.children.size() != 1)
        return;

    auto * sort_node = node.children[0];
    auto * sorting_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sorting_step)
        return;

    const auto & sort_desc = sorting_step->getSortDescription();
    if (sort_desc.size() != 1 || sort_node->children.size() != 1)
        return;

    auto * after_sort_child = sort_node->children[0];
    ExpressionStep * expr_step = nullptr;
    QueryPlan::Node * expr_node = nullptr;
    QueryPlan::Node * agg_node = nullptr;

    if (auto * maybe_expr = typeid_cast<ExpressionStep *>(after_sort_child->step.get()))
    {
        expr_step = maybe_expr;
        expr_node = after_sort_child;
        if (expr_node->children.size() != 1)
            return;
        agg_node = expr_node->children[0];
    }
    else
    {
        agg_node = after_sort_child;
    }

    auto * aggregating_step = typeid_cast<AggregatingStep *>(agg_node->step.get());
    if (!aggregating_step || aggregating_step->isGroupingSets() || aggregating_step->getParams().overflow_row)
        return;

    const auto & params = aggregating_step->getParams();
    if (params.keys.empty())
        return;

    const auto & agg_descs = params.aggregates;
    if (agg_descs.empty())
        return;

    String sort_col_name = sort_desc[0].column_name;
    String original_sort_col_name = sort_col_name;
    if (expr_step)
    {
        auto traced = traceColumnThroughDAG(expr_step->getExpression(), sort_col_name);
        if (traced.empty())
            return;
        original_sort_col_name = traced;
    }

    size_t order_by_agg_idx = agg_descs.size();
    for (size_t i = 0; i < agg_descs.size(); ++i)
    {
        if (agg_descs[i].column_name == original_sort_col_name)
        {
            order_by_agg_idx = i;
            break;
        }
    }
    if (order_by_agg_idx >= agg_descs.size())
        return;

    const auto & order_agg = agg_descs[order_by_agg_idx];
    auto order_info = order_agg.function->getTopKAggregateInfo();
    if (order_info.determined_by_first_row_direction == 0)
        return;

    int required_direction = order_info.determined_by_first_row_direction;
    int sort_direction = sort_desc[0].direction;

    if (required_direction != INT_MAX && required_direction != sort_direction)
        return;
    if (required_direction == INT_MAX)
        required_direction = sort_direction;

    for (const auto & agg : agg_descs)
    {
        auto info = agg.function->getTopKAggregateInfo();
        if (info.determined_by_first_row_direction == 0)
            return;
        if (info.determined_by_first_row_direction != INT_MAX && info.determined_by_first_row_direction != required_direction)
            return;
    }

    size_t limit_value = limit_step->getLimit();

    SortDescription topn_sort_desc;
    SortColumnDescription sort_col_desc = sort_desc[0];
    sort_col_desc.column_name = original_sort_col_name;
    topn_sort_desc.push_back(std::move(sort_col_desc));

    bool sorted_input = false;
    bool enable_threshold_pruning = false;
    TopKThresholdTrackerPtr threshold_tracker;
    ReadFromMergeTree * read_from_mt = nullptr;
    String sorted_input_arg_col_name;

    if (agg_node->children.size() == 1)
        read_from_mt = findReadFromMergeTree(*agg_node->children[0]);

    /// Mode 1 requires output_ordered_by_sort_key because early termination depends
    /// on the aggregate result being monotonically ordered with the sort key. For example,
    /// argMin(payload, ts) returns payload which has different ordering than ts.
    if (read_from_mt && order_info.output_ordered_by_sort_key)
    {
        String order_arg_name = resolveOriginalArgName(
            order_agg.argument_names.back(),
            agg_node->children.size() == 1 ? agg_node->children[0] : nullptr);

        const auto & sorting_key_columns = read_from_mt->getStorageMetadata()->getSortingKeyColumns();
        if (!sorting_key_columns.empty() && sorting_key_columns[0] == order_arg_name)
        {
            if (read_from_mt->requestReadingInOrder(1, required_direction, limit_value))
            {
                sorted_input = true;
                /// TopNAggregatingStep consumes the pre-aggregation header where the
                /// argument is represented by the identifier name (for example,
                /// `__table1.start_time`) instead of the storage column name.
                sorted_input_arg_col_name = order_agg.argument_names.back();
            }
        }
    }

    /// Mode 2 (unsorted input): activate when reading from MergeTree with a numeric,
    /// non-nullable ORDER BY aggregate argument. This avoids mismatches between
    /// threshold comparison semantics and SQL ordering semantics for NULL / collation.
    /// TODO: Relax this gate after adding threshold/filter support for NULL ordering
    /// and collation-aware comparisons, so Mode 2 can handle broader ORDER BY types.
    /// The pruning_level setting controls which optimizations are layered on:
    ///   level 0 — direct compute only (no threshold, no filter); slower than baseline
    ///   level 1 — + in-transform threshold pruning (skip rows below K-th aggregate)
    ///   level 2 — + dynamic __topKFilter prewhere (requires use_top_k_dynamic_filtering;
    ///              falls back to level 1 behavior when that setting is off)
    /// Mode 2 requires at least level 1 to avoid the known direct-compute regression.
    bool mode2_eligible = false;
    UInt64 pruning_level = optimization_settings.topn_aggregation_pruning_level;

    if (!sorted_input && pruning_level >= 1
        && order_info.output_ordered_by_sort_key && read_from_mt && !read_from_mt->getPrewhereInfo())
    {
        String order_arg_name = resolveOriginalArgName(
            order_agg.argument_names.back(),
            agg_node->children.size() == 1 ? agg_node->children[0] : nullptr);

        const auto & mt_header = *read_from_mt->getOutputHeader();
        if (mt_header.has(order_arg_name))
        {
            const auto & arg_col = mt_header.getByName(order_arg_name);
            if (arg_col.type->isValueRepresentedByNumber() && !arg_col.type->isNullable())
            {
                mode2_eligible = true;
                enable_threshold_pruning = true;

                if (pruning_level >= 2 && optimization_settings.use_top_k_dynamic_filtering)
                {
                    threshold_tracker = std::make_shared<TopKThresholdTracker>(sort_direction);

                    auto new_prewhere_info = std::make_shared<PrewhereInfo>();
                    NameAndTypePair arg_name_type(order_arg_name, arg_col.type);
                    new_prewhere_info->prewhere_actions = ActionsDAG({arg_name_type});

                    auto filter_function = createInternalFunctionTopKFilterResolver(threshold_tracker);
                    const auto & prewhere_node = new_prewhere_info->prewhere_actions.addFunction(
                        filter_function, {new_prewhere_info->prewhere_actions.getInputs().front()}, {});
                    new_prewhere_info->prewhere_actions.getOutputs().push_back(&prewhere_node);
                    new_prewhere_info->prewhere_column_name = prewhere_node.result_name;
                    new_prewhere_info->remove_prewhere_column = true;
                    new_prewhere_info->need_filter = true;

                    auto initial_header = read_from_mt->getOutputHeader();
                    read_from_mt->updatePrewhereInfo(new_prewhere_info);
                    auto updated_header = read_from_mt->getOutputHeader();

                    if (!blocksHaveEqualStructure(*initial_header, *updated_header))
                    {
                        auto * mt_node = agg_node->children[0];
                        while (mt_node && !typeid_cast<ReadFromMergeTree *>(mt_node->step.get()))
                        {
                            if (mt_node->children.size() != 1)
                                break;
                            mt_node = mt_node->children[0];
                        }

                        if (mt_node && typeid_cast<ReadFromMergeTree *>(mt_node->step.get()))
                        {
                            auto dag = ActionsDAG::makeConvertingActions(
                                updated_header->getColumnsWithTypeAndName(),
                                initial_header->getColumnsWithTypeAndName(),
                                ActionsDAG::MatchColumnsMode::Name, read_from_mt->getContext());

                            auto converting_step = std::make_unique<ExpressionStep>(updated_header, std::move(dag));
                            auto & converting_node = nodes.emplace_back();
                            converting_node.step = std::move(converting_step);
                            converting_node.children = mt_node->children;
                            mt_node->children = {&converting_node};
                            std::swap(mt_node->step, converting_node.step);
                        }
                    }
                }
            }
        }
    }

    if (!sorted_input && !mode2_eligible)
        return;

    auto topn_step = std::make_unique<TopNAggregatingStep>(
        agg_node->step->getInputHeaders().front(),
        Names(params.keys.begin(), params.keys.end()),
        AggregateDescriptions(agg_descs.begin(), agg_descs.end()),
        topn_sort_desc,
        limit_value,
        sorted_input,
        enable_threshold_pruning,
        threshold_tracker,
        sorted_input_arg_col_name);

    if (expr_node)
    {
        auto & topn_node = nodes.emplace_back();
        topn_node.step = std::move(topn_step);
        topn_node.children = agg_node->children;

        node.step = std::move(expr_node->step);
        node.children = {&topn_node};
    }
    else
    {
        node.step = std::move(topn_step);
        node.children = agg_node->children;
    }
}

}
