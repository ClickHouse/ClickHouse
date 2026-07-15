#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/logger_useful.h>

/// Rewrites a grouped exact distinct count into a count over a deduplicating aggregation: a query
/// of the shape
///
///     SELECT k, uniqExact(x) FROM t GROUP BY k
///
/// is executed as
///
///     SELECT k, count(x) FROM (SELECT k, x FROM t GROUP BY k, x) GROUP BY k
///
/// In the plan, the single `AggregatingStep` is replaced by a deduplicating `AggregatingStep`
/// (the original keys plus the argument, no aggregate functions) with a `count(x)` step on top.
/// The rewrite is applied only when the hash-table statistics recorded by a previous run pass the gate conditions defined below.

namespace DB
{

namespace QueryPlanOptimizations
{

namespace
{

/// The upper bound of the allowed grouping key cardinality.
/// More group keys make the count aggregation the rewrite adds more expensive.
/// Local benchmarks win clearly up to a few hundred thousand observed
/// keys and lose at several million, so the bound sits at the low end of the gap.
constexpr UInt64 max_observed_group_keys = 1000000;

/// The lower bound on the average number of distinct argument values per group key.
/// The rewrite's savings come from the per-key distinct sets — the larger they are,
/// the more duplicated set maintenance and merging is eliminated — while its cost does not
/// depend on their size. In local benchmarks sets of <= 4 elements lose 2x, sets of ~256 break even,
/// and the winners have 1700+, so the bound sits between break-even and the winners.
constexpr UInt64 min_avg_distinct_values_per_key = 500;

/// Overlap: in how many per-thread hash tables the same group key appears, on average.
///
/// Every aggregating thread builds a private hash table from its share of the rows, so a group
/// key appears in the hash table of each thread that read at least one of its rows — and each
/// appearance holds its own copy of the key's distinct-value set. The final merge combines those
/// copies element by element. Eliminating this duplicated work is the entire saving of the
/// rewrite: no duplication, no saving.
///
/// Example with 4 threads and group keys A, B, C:
///   - unsorted source table: every thread reads rows of every key, so all 4 hash tables hold
///     {A, B, C}; the hash table sizes sum to 12, the merged result has 3 keys: overlap = 4;
///   - source table sorted by the key: every thread reads the rows of only one key; the sizes
///     sum to 3 against the same 3-key result: overlap = 1 — here the rewrite can only lose.
///
/// The gate computes the overlap as `sum_of_hash_table_sizes / merged_result_rows` and requires
/// at least this value: at 4 threads an overlap of 2 measurably loses while 3 wins.
constexpr UInt64 min_required_group_key_thread_overlap = 3;

/// The largest overlap the gate is allowed to require. It is not a limit on the overlap itself —
/// higher overlap means more duplication to eliminate and is always better; an observed overlap
/// above this value always passes.
///
/// Without the cap the requirements would become unsatisfiable on many threads: real key frequencies
/// are skewed, rare keys occur in only a few threads' rows and pull the average overlap down, so
/// the best measured workloads reach only 14-23x over 64 hash tables while "half of the tables"
/// would demand 32x. Requiring 8 is enough even on many thread aggregation, because the saving depends
/// on the absolute number of duplicated copies — at overlap 8 every set is built 8 times and merged 8-into-1
/// no matter how many threads there are — and the worst measured shape at exactly overlap 8 still wins.
constexpr UInt64 max_required_group_key_thread_overlap = 8;

Aggregator::Params cloneParamsWith(const Aggregator::Params & params, const Names & keys, const AggregateDescriptions & aggregates)
{
    Aggregator::Params result(
        keys,
        aggregates,
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        params.group_by_overflow_mode,
        params.group_by_two_level_threshold,
        params.group_by_two_level_threshold_bytes,
        params.max_bytes_before_external_group_by,
        params.empty_result_for_aggregation_by_empty_set,
        params.tmp_data_scope,
        params.max_threads,
        params.min_free_disk_space,
        params.compile_aggregate_expressions,
        params.min_count_to_compile_aggregate_expression,
        params.max_block_size,
        params.enable_prefetch,
        /*only_merge_=*/false,
        params.optimize_group_by_constant_keys,
        params.min_hit_rate_to_use_consecutive_keys_optimization,
        params.stats_collecting_params,
        params.enable_producing_buckets_out_of_order_in_aggregation,
        params.serialize_string_with_zero_byte);

    result.stats_collecting_params.setKey(0);
    return result;
}

/// Rewrites one grouped distinct count into a dedup aggregation plus a count over it when the
/// rewrite applies and the statistics gate approves. Returns whether the plan changed.
bool tryRewriteGroupedCountDistinct(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating || node.children.size() != 1)
        return false;

    const auto & params = aggregating->getParams();

    /// The plain, local, finalizing form: one `uniqExact` over one column, grouped by ordinary keys.
    if (params.aggregates.size() != 1 || params.keys_size == 0)
        return false;

    const auto & distinct_aggregate = params.aggregates.front();
    if (distinct_aggregate.function->getName() != "uniqExact" || !distinct_aggregate.parameters.empty()
        || distinct_aggregate.argument_names.size() != 1)
        return false;

    if (params.overflow_row || params.max_rows_to_group_by != 0 || params.only_merge)
        return false;

    if (!aggregating->getFinal() || aggregating->isGroupingSets() || aggregating->inOrder()
        || aggregating->shouldProduceResultsInBucketOrder() || aggregating->memoryBoundMergingWillBeUsed())
        return false;

    /// A key as the argument means one distinct value per group; nothing to rewrite.
    const auto & argument_name = distinct_aggregate.argument_names.front();
    if (std::find(params.keys.begin(), params.keys.end(), argument_name) != params.keys.end())
        return false;

    if (!params.stats_collecting_params.isCollectionAndUseEnabled())
        return false;

    const auto hint = getHashTablesStatistics<AggregationEntry>().getSizeHint(params.stats_collecting_params);
    if (!hint || hint->sum_of_hash_table_sizes == 0 || hint->sum_of_hash_table_sizes > max_observed_group_keys)
        return false;

    if (hint->merged_result_rows == 0 || hint->distinct_key_value_pairs < hint->merged_result_rows * min_avg_distinct_values_per_key)
        return false;

    const auto min_overlap
        = std::clamp<UInt64>(hint->merged_hash_tables / 2, min_required_group_key_thread_overlap, max_required_group_key_thread_overlap);

    if (hint->sum_of_hash_table_sizes < hint->merged_result_rows * min_overlap)
        return false;

    const auto & input_header = node.children.front()->step->getOutputHeader();
    if (!input_header->has(argument_name))
        return false;
    const auto & argument_type = input_header->getByName(argument_name).type;

    /// The dedup aggregation: group by the original keys plus the argument, no aggregates.
    Names dedup_keys = params.keys;
    dedup_keys.push_back(argument_name);
    auto dedup_step = std::make_unique<AggregatingStep>(
        input_header,
        cloneParamsWith(params, dedup_keys, {}),
        GroupingSetsParamsList{},
        /*final_=*/true,
        aggregating->getMaxBlockSize(),
        aggregating->getMaxBlockSizeForAggregationInOrder(),
        aggregating->getMergeThreads(),
        aggregating->getTemporaryDataMergeThreads(),
        /*storage_has_evenly_distributed_read_=*/false,
        aggregating->isGroupByUseNulls(),
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
        /*explicit_sorting_required_for_aggregation_in_order_=*/false,
        aggregating->isShardingAggregatorEnabled());

    AggregateDescription count_aggregate;
    AggregateFunctionProperties properties;
    count_aggregate.function = AggregateFunctionFactory::instance().get("count", NullsAction::EMPTY, {argument_type}, {}, properties);
    count_aggregate.argument_names = {argument_name};
    count_aggregate.column_name = distinct_aggregate.column_name;

    auto count_step = std::make_unique<AggregatingStep>(
        dedup_step->getOutputHeader(),
        cloneParamsWith(params, params.keys, {std::move(count_aggregate)}),
        GroupingSetsParamsList{},
        /*final_=*/true,
        aggregating->getMaxBlockSize(),
        aggregating->getMaxBlockSizeForAggregationInOrder(),
        aggregating->getMergeThreads(),
        aggregating->getTemporaryDataMergeThreads(),
        /*storage_has_evenly_distributed_read_=*/false,
        aggregating->isGroupByUseNulls(),
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
        /*explicit_sorting_required_for_aggregation_in_order_=*/false,
        aggregating->isShardingAggregatorEnabled());

    if (!blocksHaveEqualStructure(*count_step->getOutputHeader(), *aggregating->getOutputHeader()))
        return false;

    auto & dedup_node = nodes.emplace_back();
    dedup_node.step = std::move(dedup_step);
    dedup_node.children = {node.children.front()};
    node.children = {&dedup_node};
    node.step = std::move(count_step);

    LOG_DEBUG(
        getLogger("QueryPlanOptimizations"),
        "Rewrote uniqExact({}) into a count over a deduplicating aggregation (observed group keys: {}, thread overlap: {}x, "
        "avg distinct values per key: {})",
        argument_name,
        hint->merged_result_rows,
        hint->sum_of_hash_table_sizes / hint->merged_result_rows,
        hint->distinct_key_value_pairs / hint->merged_result_rows);
    return true;
}

}

bool rewriteGroupedCountDistinct(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!optimization_settings.rewrite_grouped_count_distinct || !optimization_settings.collect_hash_table_stats_during_aggregation)
        return false;

    bool changed = false;
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(&root);
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        changed |= tryRewriteGroupedCountDistinct(*node, nodes);
        for (auto * child : node->children)
            stack.push_back(child);
    }
    return changed;
}

}

}
