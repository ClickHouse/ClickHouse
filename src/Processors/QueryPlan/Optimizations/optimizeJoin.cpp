#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/safe_cast.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/System/StorageSystemOne.h>

#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ranges>
#include <base/types.h>

namespace ProfileEvents
{
    extern const Event JoinOptimizeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsBool use_statistics;
    extern const SettingsBool use_hash_table_stats_for_join_reordering;
}

RelationStats parseTableStatsHint(ContextPtr context, const String & table_name);
RelationStats parseTableStatsHint(const String & stats_hint_json, const String & table_name);
RelationStats getRandomizedStats(UInt64 seed, size_t relation_index, const String & table_name, const Block & header);

namespace QueryPlanOptimizations
{

static String dumpStatsForLogs(const RelationStats & stats);

/// If we have stats for storage column names, find the corresponding `ActionsDAG` outputs.
/// Both identity and weaker NDV-bound lineage are valid for this existing statistics use.
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    /// Column statistics are usually absent; do not pay for a full lineage walk of the
    /// `ActionsDAG` when there is nothing to remap.
    if (mapped.empty())
        return;

    std::unordered_map<String, ColumnStats> original;
    original.swap(mapped);

    const auto lineage = traceActionsDAGLineage(actions);
    const auto & inputs = actions.getInputs();
    const auto & outputs = actions.getOutputs();
    for (const auto & output_lineage : lineage)
    {
        if (!output_lineage.input)
            continue;

        const auto stats_it = original.find(inputs[output_lineage.input->input_position]->result_name);
        if (stats_it == original.end())
            continue;

        ColumnStats stats = stats_it->second;
        /// Add the offset, guarding against overflow when the source NDV is near the maximum.
        if (stats.num_distinct_values <= std::numeric_limits<UInt64>::max() - output_lineage.input->ndv_delta)
            stats.num_distinct_values += output_lineage.input->ndv_delta;
        /// A hop that changes the type (e.g. `toString(k)`) changes the value bytes, so drop the
        /// width to unknown.
        if (!output_lineage.input->preserves_width)
            stats.avg_bytes = 0;
        mapped[outputs[output_lineage.output_position]->result_name] = stats;
    }
}

struct RuntimeHashStatisticsContext
{
    /// `HashTablesStatistics` keys identify a specific hash table BUILT from a subtree AND
    /// keyed by specific columns — both pieces are needed: the same right-side subtree
    /// joined on `t2.a` and joined on `t2.b` produces two physically different hash tables
    /// with different sizes/NDVs, so they must NOT share a cache entry. The key encoding is:
    ///     cache_keys[N]  =  raw_hashes[N]  XOR  <per-side contribution of N's parent join>
    /// where the contribution hashes the parent join's equi-key columns on the side N sits on.
    /// Populated for every node by `calculateHashTableCacheKeys`; mutated during join reorder
    /// to reflect the post-reorder parent of each node.
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    /// Bottom-up hash of the subtree rooted at the node — does NOT include any parent-join
    /// contribution, so it identifies "what data" but not "what hash table keyed how". Used
    /// by join-reorder code in `chooseJoinOrder` to derive cache keys for sub-join nodes
    /// built during reorder, where the post-reorder parent's contribution differs from
    /// whatever was originally stamped into `cache_keys` during the pre-reorder walk.
    std::unordered_map<const QueryPlan::Node *, UInt64> raw_hashes;
    StatsCollectingParams params;

    RuntimeHashStatisticsContext(const QueryPlanOptimizationSettings & optimization_settings, const QueryPlan::Node & root_node)
        : params{
            /*key_=*/0,
            /*enable=*/ optimization_settings.collect_hash_table_stats_during_joins,
            optimization_settings.max_entries_for_hash_table_stats,
            optimization_settings.max_size_to_preallocate_for_joins}
    {
        if (optimization_settings.collect_hash_table_stats_during_joins)
        {
            calculateHashTableCacheKeys(root_node, cache_keys, raw_hashes);
        }
    }

    UInt64 getCachedKey(const QueryPlan::Node * node)
    {
        if (auto it = cache_keys.find(node); it != cache_keys.end())
            return it->second;
        return 0;
    }

    UInt64 getRawHash(const QueryPlan::Node * node) const
    {
        if (auto it = raw_hashes.find(node); it != raw_hashes.end())
            return it->second;
        return 0;
    }

    std::optional<size_t> getCachedHint(const QueryPlan::Node * node)
    {
        if (auto cache_key = getCachedKey(node))
        {
            auto & hash_table_stats = getHashTablesStatistics<HashJoinEntry>();
            if (auto hint = hash_table_stats.getSizeHint(params.setKey(cache_key)))
                return hint->source_rows;
        }
        return {};
    }

    /// What a previous run measured for a hash table built from the subtree whose parent-independent
    /// hash is `raw_hash` and keyed on exactly `key_nodes`. Unlike `getCachedHint`, the key set is
    /// given explicitly rather than taken from an existing join, so a candidate subset of the equi
    /// keys can be scored before this join's own key set is final (see `demoteHighNdvKeysToProbe`).
    std::optional<HashJoinEntry> getCachedHintForKeys(
        UInt64 raw_hash, const String & step_serialization_name, const std::vector<const ActionsDAG::Node *> & key_nodes)
    {
        if (!raw_hash || key_nodes.empty())
            return {};
        const UInt64 probe_key = raw_hash ^ calculateJoinStepCacheKeyContribution(step_serialization_name, key_nodes);
        return getHashTablesStatistics<HashJoinEntry>().getSizeHint(params.setKey(probe_key));
    }

    /// Mirror what `calculateHashTableCacheKeys` would have produced for an equivalent join in
    /// the original tree, but for `new_node` that the join-reorder pass is emitting on top of
    /// `left_child_node` and `right_child_node` (which can themselves be original leaves or
    /// sub-joins built earlier in the same reorder loop). Returns the derived right-side key,
    /// suitable for `JoinStepLogical::setRightHashTableCacheKey`.
    ///
    /// Each child's final cache key combines its parent-independent subtree hash with the
    /// parent join's per-side contribution (see `cache_keys` doc above for why both are
    /// needed). We start from `raw_hashes[child]` rather than the previously-xored value in
    /// `cache_keys[child]`, because under reorder the new parent's contribution can differ
    /// from the original tree's parent contribution that was stamped into `cache_keys`.
    struct DerivedJoinCacheKeys
    {
        UInt64 right_key = 0;
        UInt64 output_key = 0;
    };

    DerivedJoinCacheKeys deriveCacheKeysForNewJoin(
        const QueryPlan::Node * left_child_node,
        const QueryPlan::Node * right_child_node,
        const QueryPlan::Node & new_node,
        const JoinStepLogical & join_step)
    {
        if (cache_keys.empty())
            return {};

        UInt64 raw_left = getRawHash(left_child_node);
        UInt64 raw_right = getRawHash(right_child_node);
        UInt64 left_key = raw_left ^ calculateJoinStepCacheKeyContribution(join_step, JoinTableSide::Left);
        UInt64 right_key = raw_right ^ calculateJoinStepCacheKeyContribution(join_step, JoinTableSide::Right);

        /// Update cache_keys to reflect the new parent for these children (in case any
        /// downstream code reads them back via getCachedKey).
        cache_keys[left_child_node] = left_key;
        cache_keys[right_child_node] = right_key;

        /// Record this join's own raw hash so any outer reorder iteration can derive its key
        /// the same way; cache_keys gets the same value initially and may later be xored when
        /// new_node becomes a child of yet another reorder-built join.
        SipHash node_hash;
        node_hash.update(left_key);
        node_hash.update(right_key);
        UInt64 raw_new = node_hash.get64();
        raw_hashes[&new_node] = raw_new;
        cache_keys[&new_node] = raw_new;

        /// Derive a key for the join output stats that takes the kind, strictness
        /// and non-equi conditions into account, in addition to the equi conditions
        /// covered by `calculateJoinStepCacheKeyContribution`.
        SipHash output_hash;
        output_hash.update(raw_new);
        const auto & join_operator = join_step.getJoinOperator();
        output_hash.update(join_operator.kind);
        output_hash.update(join_operator.strictness);
        for (const auto & condition : join_operator.expression)
        {
            if (condition.isFunction(JoinConditionOperator::Equals) || condition.isFunction(JoinConditionOperator::NullSafeEquals))
                continue;
            condition.getNode()->updateHash(output_hash);
        }

        /// The equalities `demoteHighNdvKeysToProbe` moved out of `expression` are not covered by
        /// `calculateJoinStepCacheKeyContribution` either, which by design hashes only the keys the
        /// hash table is built on. They still filter the join output, so two joins over the same
        /// subtrees that keep the same key subset but probe on different extra equalities must not
        /// share a match-count hint - that hint drives the row-store decision in
        /// `chooseJoinAlgorithm`, and reusing it would size the decision on unrelated fanout.
        for (const auto & condition : join_operator.probe_conditions)
            condition.getNode()->updateHash(output_hash);

        return {right_key, output_hash.get64()};
    }
};

static RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    std::optional<Float64> total_number_of_distinct_values = 1;
    RelationStats aggregation_stats;
    /// Carry imprecision and source from the input, or the annotation is lost for aggregation subqueries.
    aggregation_stats.imprecise_estimate = input_stats.imprecise_estimate;
    aggregation_stats.source = input_stats.source;
    for (const auto & key : aggregator_params.keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        if (key_stats == input_stats.column_stats.end())
        {
            /// Cannot calculate total number of groups if we don't know NDV of any of the aggregation columns.
            /// The estimate then falls back to the input row count (an over-count of groups), so it is no longer
            /// precise. Flag it and surface a missing-statistics source so the EXPLAIN label and the
            /// join-reordering diagnostic reflect that the fallback was caused by missing column statistics.
            total_number_of_distinct_values.reset();
            aggregation_stats.imprecise_estimate = true;
            if (aggregation_stats.source == RowEstimateSource::Statistics || aggregation_stats.source == RowEstimateSource::NoSource)
                aggregation_stats.source = RowEstimateSource::NoStatistics;
            continue;
        }

        UInt64 key_number_of_distinct_values = key_stats->second.num_distinct_values;

        if (input_stats.estimated_rows)
            key_number_of_distinct_values = std::min(key_number_of_distinct_values, *input_stats.estimated_rows);

        aggregation_stats.column_stats[key].num_distinct_values = key_number_of_distinct_values;

        /// For now assume that aggregation columns are independent, so multiply their NDVs
        if (total_number_of_distinct_values)
            *total_number_of_distinct_values *= static_cast<Float64>(key_number_of_distinct_values);
    }

    if (total_number_of_distinct_values && input_stats.estimated_rows)
        total_number_of_distinct_values = std::min(*total_number_of_distinct_values, Float64(*input_stats.estimated_rows));
    else
        total_number_of_distinct_values = input_stats.estimated_rows;

    aggregation_stats.estimated_rows = total_number_of_distinct_values;

    return aggregation_stats;
}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr);
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();

        /// Analyze partition and primary-key ranges before estimating the relation so column
        /// statistics come only from parts that can satisfy the query. Reuse the result for
        /// the index-based fallback below.
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResult();
        if (!analyzed_result)
        {
            const auto & settings = reading->getContext()->getSettingsRef();
            const bool has_throwing_row_limit
                = (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
                || (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf]);

            /// Range analysis normally enforces throwing read limits and memoizes its result.
            /// At this stage, however, later planning may make the executed read exempt from those
            /// limits. In that case use an estimation-only analysis; execution will analyze again
            /// after its final read mode is known.
            analyzed_result = has_throwing_row_limit
                ? reading->selectRangesToReadForEstimation()
                : reading->selectRangesToRead();
        }

        /// An exact empty range selection proves that the relation is empty. Other empty
        /// analysis results can be placeholders for deferred work, so only propagate zero
        /// when `has_exact_ranges` is set.
        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return RelationStats{.estimated_rows = 0, .table_name = table_display_name};

        /// `STREAM` defers range analysis until execution. Its placeholder result has zero
        /// selected rows but does not mean that the relation is empty.
        if (reading->getQueryInfo().isStream() && analyzed_result && analyzed_result->selected_rows == 0)
        {
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto prewhere_info = reading->getPrewhereInfo();
                const ActionsDAG::Node * prewhere_node = prewhere_info
                    ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                    : nullptr;
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), filter, prewhere_node);
                RelationStats stats {
                    .estimated_rows = relation_profile.rows,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .source = RowEstimateSource::Statistics};
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpStatsForLogs(stats));
                return stats;
            }
        }
        if (auto stats_hint = parseTableStatsHint(reading->getContext(), table_display_name); !stats_hint.table_name.empty())
            return stats_hint;

        if (!analyzed_result)
            return RelationStats{.estimated_rows = {}, .table_name = table_display_name, .imprecise_estimate = true, .source = RowEstimateSource::NoStatistics};

        bool is_filtered_by_index = false;
        UInt64 total_parts = 0;
        UInt64 total_granules = 0;
        for (const auto & idx_stat : analyzed_result->index_stats)
        {
            /// We expect the first element to be an index with None type, which is used to estimate the total amount of data in the table.
            /// Further index_stats are used to estimate amount of filtered data after applying the index.
            if (ReadFromMergeTree::IndexType::None == idx_stat.type)
            {
                total_parts = idx_stat.num_parts_after;
                total_granules = idx_stat.num_granules_after;
                continue;
            }

            is_filtered_by_index = is_filtered_by_index
                || (total_parts && idx_stat.num_parts_after < total_parts)
                || (total_granules && idx_stat.num_granules_after < total_granules);

            if (is_filtered_by_index)
                break;
        }
        bool has_filter = filter || reading->getPrewhereInfo();

        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{.estimated_rows = {}, .table_name = table_display_name, .imprecise_estimate = true, .source = RowEstimateSource::NoStatistics};

        return RelationStats{.estimated_rows = analyzed_result->selected_rows, .table_name = table_display_name, .imprecise_estimate = true, .source = RowEstimateSource::PrimaryIndex};
    }

    if (typeid_cast<const ReadFromObjectStorageStep *>(step))
        return RelationStats{};

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_display_name = reading->getStorage()->getName();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = table_display_name, .source = RowEstimateSource::Statistics};
    }

    /// We cannot do typeid_cast<const ReadFromSystemOneStep *>(step)
    /// since this is defined in clickhouse_storages_system module,
    /// which is not linked to current module
    if (step->getName() == "ReadFromSystemOne")
    {
        /// system.one always produces exactly one row — used to implement constant SELECTs like `SELECT 1`.
        return RelationStats{.estimated_rows = 1, .table_name = "system.one"};
    }

    if (const auto * reading = typeid_cast<const CommonSubplanReferenceStep *>(step))
    {
        return estimateReadRowsCount(*reading->getSubplanReferenceRoot(), filter);
    }

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), filter);
        auto limit = limit_step->getLimit();
        if (!estimated.estimated_rows || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step); expression_step && !expression_step->getExpression().hasArrayJoin())
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));

        /// Both predicates of two stacked `FilterStep`s have to reach the estimator: passing only
        /// the inner one down would drop the outer one's selectivity and over-estimate the relation,
        /// which is what the join order and `query_plan_hash_join_subset_keys_auto` are sized from.
        /// The conjunction is owned by a DAG built here, so it must outlive the recursive call below.
        std::optional<ActionsDAG> conjunction_dag;
        const auto * filter_to_push = predicate ? predicate : filter;
        if (filter && predicate)
        {
            conjunction_dag = ActionsDAG::buildFilterActionsDAG({filter, predicate});
            if (!conjunction_dag || conjunction_dag->getOutputs().empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to combine filters for row count estimation");
            filter_to_push = conjunction_dag->getOutputs().front();
        }

        auto stats = estimateReadRowsCount(*node.children.front(), filter_to_push);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        auto aggregation_stats = estimateAggregatingStepStats(*aggregating_step, stats);
        return aggregation_stats;
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        return RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = join_step->hasImpreciseEstimate()};
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        if (sorting_step->getLimit())
        {
            if (!stats.estimated_rows || stats.estimated_rows > sorting_step->getLimit())
                stats.estimated_rows = sorting_step->getLimit();
        }
        return stats;
    }

    /// Estimates must see through exchanges: they do not change row counts, and an
    /// already-distributed subtree would otherwise report unknown cardinality, degrading
    /// broadcast-vs-shuffle and join order decisions.
    if (dynamic_cast<LogicalExchangeStep *>(step))
        return estimateReadRowsCount(*node.children.front(), filter);

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return estimateReadRowsCount(*node.children.front(), filter);

    return {};
}


bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes & /*nodes*/, const QueryPlanOptimizationSettings &)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2 || join_step->isOptimized())
        return false;

    const auto & join = join_step->getJoin();
    if (join->pipelineType() != JoinPipelineType::FillRightFirst || !join->isCloneSupported())
        return true;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);

    const auto & table_join = join->getTableJoin();

    /// Algorithms other than HashJoin may not support all JOIN kinds, so changing from LEFT to RIGHT is not always possible
    bool allow_outer_join = typeid_cast<const HashJoin *>(join.get());
    if (table_join.kind() != JoinKind::Inner && !allow_outer_join)
        return true;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    if (table_join.hasUsing() || table_join.strictness() != JoinStrictness::All)
        return true;

    bool need_swap = false;
    if (!join_step->swap_join_tables.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]).estimated_rows;
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]).estimated_rows;
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation ? toString(lhs_extimation.value()) : "unknown",
            rhs_extimation ? toString(rhs_extimation.value()) : "unknown");

        if (lhs_extimation && rhs_extimation && lhs_extimation < rhs_extimation)
            need_swap = true;
    }
    else if (join_step->swap_join_tables.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return true;

    const auto & headers = join_step->getInputHeaders();
    if (headers.size() != 2)
        return true;

    auto left_stream_input_header = headers.front();
    auto right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);

    /// After swapping, the join output may lose columns because TableJoin::swapSides
    /// swaps result_columns_from_left_table with columns_added_by_join, and the join
    /// algorithm may filter different columns from the (now swapped) left input.
    /// If any column required by downstream steps would be missing, skip the swap.
    auto original_output = join_step->getOutputHeader();
    auto swapped_algorithm_header = JoiningTransform::transformHeader(*right_stream_input_header, updated_join);
    for (const auto & col : *original_output)
    {
        if (!swapped_algorithm_header.has(col.name))
            return true;
    }

    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);

    return true;
}

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    bool keep_logical = optimization_settings.keep_logical_steps;
    /// Distributed plan keeps logical joins steps. They are converted to physical steps afterwards, when plan fragment is executed by a worker.
    keep_logical |= optimization_settings.make_distributed_plan;
    if (keep_logical)
        return false;
    if (!typeid_cast<JoinStepLogical *>(node.step.get()))
        return false;
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    JoinStepLogical::buildPhysicalJoin(node, optimization_settings, nodes);

    return true;
}

struct QueryGraphBuilder
{
    JoinExpressionActions expression_actions;

    std::vector<RelationStats> relation_stats;
    std::vector<QueryPlan::Node *> inputs;

    std::vector<JoinActionRef> join_edges;

    /// Outer joined relation should be joined after all other relations involved in its join expressions.
    /// It is joined with specified join kind.
    /// The `join_kinds` maps (join relation index) -> (set of relations it depends on, join kind)
    std::unordered_map<size_t, std::pair<BitSet, JoinKind>> join_kinds;
    std::unordered_map<size_t, ActionsDAG::NodeRawConstPtrs> type_changes;
    /// ON-clause predicates of outer joins, see QueryGraph::outer_join_conditions
    std::unordered_map<JoinActionRef, size_t> outer_join_conditions;

    /// One record per binary join operator of the original tree, captured for the optional conflict
    /// detector (CD-A/CD-C). Relation ids are local to this (sub)graph and shifted in `uniteGraphs`.
    /// See QueryGraph::conflict_ops / ConflictJoinOp.
    std::vector<ConflictJoinOp> conflict_ops;

    struct BuilderContext
    {
        const QueryPlanOptimizationSettings & optimization_settings;
        RuntimeHashStatisticsContext statistics_context;
        JoinSettings join_settings;
        SortingStep::Settings sorting_settings;
        String stats_hint;
        UInt64 effective_randomize_seed = 0;

        BuilderContext(
            const QueryPlanOptimizationSettings & optimization_settings_,
            const QueryPlan::Node & root_node,
            const JoinSettings & join_settings_,
            const SortingStep::Settings & sorting_settings_)
            : optimization_settings(optimization_settings_)
            , statistics_context(optimization_settings_, root_node)
            , join_settings(join_settings_)
            , sorting_settings(sorting_settings_)
            , effective_randomize_seed(optimization_settings_.query_plan_optimize_join_order_randomize)
        {
        }
    };

    std::shared_ptr<BuilderContext> context;

    explicit QueryGraphBuilder(std::shared_ptr<BuilderContext> context_)
        : context(std::move(context_)) {}

    QueryGraphBuilder(const QueryPlanOptimizationSettings & optimization_settings_, const QueryPlan::Node & root_node,
                      const JoinSettings & join_settings_, const SortingStep::Settings & sorting_settings_)
        : context(std::make_shared<BuilderContext>(optimization_settings_, root_node, join_settings_, sorting_settings_))
    {}

    bool hasCompatibleSettings(const JoinStepLogical & join_step) const
    {
        return context && join_step.getJoinSettings() == context->join_settings; // && join_step.getSortingSettings() == context->sorting_settings;
    }
};

static void uniteGraphs(QueryGraphBuilder & lhs, QueryGraphBuilder rhs)
{
    size_t shift = lhs.relation_stats.size();

    auto rhs_edges_raw = std::ranges::to<std::vector>(rhs.join_edges | std::views::transform([](const auto & e) { return e.getNode(); }));
    auto rhs_outer_conditions_raw = std::ranges::to<std::unordered_map>(rhs.outer_join_conditions | std::views::transform([](const auto & e) { return std::make_pair(e.first.getNode(), e.second); }));

    auto [rhs_actions_dag, rhs_expression_sources] = rhs.expression_actions.detachActionsDAG();

    lhs.expression_actions.getActionsDAG()->unite(std::move(rhs_actions_dag));
    for (auto & [node, sources] : rhs_expression_sources)
        sources.shift(shift);
    lhs.expression_actions.setNodeSources(rhs_expression_sources);

    lhs.relation_stats.append_range(std::move(rhs.relation_stats));
    lhs.inputs.append_range(std::move(rhs.inputs));

    lhs.join_edges.append_range(rhs_edges_raw | std::views::transform([&](auto p) { return JoinActionRef(p, lhs.expression_actions); }));

    for (auto & [id, restriction] : rhs.join_kinds)
    {
        restriction.first.shift(shift);
        lhs.join_kinds[id + shift] = std::move(restriction);
    }

    /// Shift each captured CD-A operator's relation sets into the parent's numbering and append.
    for (auto & op : rhs.conflict_ops)
    {
        op.left.shift(shift);
        op.right.shift(shift);
        op.nel.shift(shift);
        op.nr_rels.shift(shift);
        lhs.conflict_ops.push_back(std::move(op));
    }

    for (auto & [sources, nodes] : rhs.type_changes)
        lhs.type_changes[sources + shift] = std::move(nodes);

    for (const auto & [action, null_rel] : rhs_outer_conditions_raw)
        lhs.outer_join_conditions[JoinActionRef(action, lhs.expression_actions)] = null_rel + shift;
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit);

static String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format("{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? toString(stats.estimated_rows.value()) : "unknown",
        fmt::join(stats.column_stats | std::views::transform(
            [](const auto & p)
            {
                return fmt::format("{}: {}", p.first, p.second.num_distinct_values);
            }), ", "));
}


void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

constexpr bool isInnerOrCross(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

/// Semi/anti joins may be fully reordered (not just swapped) only when a conflict detector (CD-A or
/// CD-C) is on AND DPsub is the sole join-order algorithm. A conflict detector is the only validity
/// model that can express their non-commutativity, and only the DPsub solver consumes it, so
/// exposing semi/anti to any other solver (greedy/dpsize/dphyp) would let it build an invalid order.
static bool conflictDetectorReordersSemiAnti(const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto & algorithms = optimization_settings.query_plan_optimize_join_order_algorithm;
    return (optimization_settings.query_plan_optimize_join_order_use_conflict_detector_a
            || optimization_settings.query_plan_optimize_join_order_use_conflict_detector_c)
        && algorithms.size() == 1
        && algorithms.front() == JoinOrderAlgorithm::DPSUB;
}

/// `mergeInplace` binds a merged expression's inputs to the graph's outputs by `result_name`, and it
/// installs only the expression's outputs. An output named like one of its own inputs therefore
/// leaves a computed node over an input of that name, and resolving the name again applies the
/// expression a second time, so such an expression must not be merged into a join graph.
static bool hasOutputShadowingInputName(const ActionsDAG & dag)
{
    std::unordered_set<std::string_view> input_names;
    for (const auto * input : dag.getInputs())
        input_names.insert(input->result_name);

    for (const auto * output : dag.getOutputs())
    {
        if (output->type != ActionsDAG::ActionType::INPUT && input_names.contains(output->result_name))
            return true;
    }

    return false;
}

/// An `ExpressionStep` above a join may be merged into the flattened join graph when the setting
/// allows it and the expression cannot be applied twice by the name-based merge.
static bool canMergeExpressionIntoJoinGraph(const ActionsDAG & dag, bool merge_expression_into_join)
{
    return merge_expression_into_join && !hasOutputShadowingInputName(dag);
}

static size_t addChildQueryGraph(QueryGraphBuilder & graph, QueryPlan::Node * node, QueryPlan::Nodes & nodes, const String & label, int join_steps_limit)
{
    auto * join_node = node;
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (expression_step && node->children.size() == 1  && !expression_step->getExpression().hasArrayJoin())
    {
        if (isPassthroughActions(expression_step->getExpression()))
        {
            /// Just skip trivial expression step, it doesn't change any columns
            expression_step = nullptr;
            join_node = node->children[0];
            node = node->children[0];
        }
        else if (canMergeExpressionIntoJoinGraph(
                     expression_step->getExpression(), graph.context->optimization_settings.merge_expression_into_join))
        {
            join_node = node->children[0];
        }
    }

    {
        auto * child_join_step = typeid_cast<JoinStepLogical *>(join_node->step.get());
        if (child_join_step && !child_join_step->isOptimized())
        {
            auto child_join_kind = child_join_step->getJoinOperator().kind;
            bool allow_child_join_kind = isInnerOrCross(child_join_kind) || isLeft(child_join_kind) || isRight(child_join_kind);
            const auto child_strictness = child_join_step->getJoinOperator().strictness;
            /// Normally only plain (All) joins are flattened into the reorderable graph. With CD-A
            /// semi/anti reordering enabled, Semi/Anti children are flattened too so DPsub can
            /// reorder them under the CD-A conflict detector.
            const bool allow_child_strictness = child_strictness == JoinStrictness::All
                || (conflictDetectorReordersSemiAnti(graph.context->optimization_settings)
                    && (child_strictness == JoinStrictness::Semi || child_strictness == JoinStrictness::Anti));
            allow_child_join_kind = allow_child_join_kind && allow_child_strictness;
            /// Do not flatten joins that have type-changing sides (e.g., LEFT JOIN
            /// with `join_use_nulls` making right-side columns Nullable). Flattening
            /// such joins allows the optimizer to reorder them, which can separate
            /// a relation from the join that causes its type change, leading to
            /// type changes being applied at the wrong step and the exception
            /// "Cannot fold actions for projection".
            allow_child_join_kind = allow_child_join_kind && child_join_step->typeChangingSides().empty();
            if (graph.hasCompatibleSettings(*child_join_step) && join_steps_limit > 1 && allow_child_join_kind)
            {
                QueryGraphBuilder child_graph(graph.context);
                buildQueryGraph(child_graph, *join_node, nodes, join_steps_limit);

                if (expression_step)
                {
                    ActionsDAG::NodeMapping node_mapping;
                    child_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_step->getExpression()), node_mapping, true);
                }

                size_t count = child_graph.inputs.size();
                uniteGraphs(graph, std::move(child_graph));
                return count;
            }
            /// Optimize child subplan before continuing to get size estimation
            optimizeJoinLogicalImpl(child_join_step, *join_node, nodes, graph.context->optimization_settings);
        }
    }

    /// When the leaf is a subquery with Join-s wrapped in Expression/Aggregating steps, we cannot Joins to the graph, but we want to optimize
    /// those child Join to get proper statistics to use in the parent Join reordering.
    {
        auto * child_node = node;
        while (child_node->children.size() == 1)
        {
            child_node = child_node->children[0];
        }

        auto * child_join_step = typeid_cast<JoinStepLogical *>(child_node->step.get());
        if (child_join_step && !child_join_step->isOptimized())
        {
            optimizeJoinLogicalImpl(child_join_step, *child_node, nodes, graph.context->optimization_settings);
        }
    }

    graph.inputs.push_back(node);
    RelationStats stats = estimateReadRowsCount(*node);

    std::optional<size_t> num_rows_from_cache = graph.context->statistics_context.getCachedHint(node);
    if (graph.context->join_settings.use_hash_table_stats_for_join_reordering && num_rows_from_cache
        && (!stats.estimated_rows || num_rows_from_cache.value() < stats.estimated_rows.value()))
    {
        /// A measured row count beats statistics: take the minimum and mark it a precise cache value.
        stats.estimated_rows = num_rows_from_cache;
        stats.imprecise_estimate = false;
        stats.source = RowEstimateSource::HashTableCache;
    }

    if (!label.empty())
        stats.table_name = label;

    if (UInt64 seed = graph.context->effective_randomize_seed)
        stats = getRandomizedStats(seed, graph.relation_stats.size(), stats.table_name, *node->step->getOutputHeader());

    LOG_TRACE(getLogger("optimizeJoin"), "Estimated statistics{} for {} {}",
        num_rows_from_cache.has_value() ? " (from cache)" : "",
        node->step->getName(), dumpStatsForLogs(stats));
    graph.relation_stats.push_back(stats);
    return 1;
}

/// Names of scalar functions that propagate NULL: if any argument is NULL, the result is NULL.
/// Only these let us conclude a wrapped column reference is null when the relation's columns are.
/// The set is intentionally small and conservative -- an unknown function is treated as opaque
/// (contributes nothing), which can only make CD-A miss a valid reordering, never admit an invalid
/// one. It excludes NULL-blocking functions on purpose (`coalesce`, `ifNull`, `assumeNotNull`, ...).
static bool isNullPropagatingFunction(const String & name)
{
    static const std::unordered_set<std::string_view> names = {
        /// comparisons (the atoms of equi/theta-join predicates)
        "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
        /// arithmetic that may wrap a column inside a comparison, e.g. `a.x + 1 = b.y`
        "plus", "minus", "multiply", "divide", "modulo", "negate",
        /// a CAST of NULL is NULL
        "CAST", "_CAST",
    };
    return names.contains(name);
}

/// Relations R such that `node` evaluates to NULL when all of R's columns are NULL ("strict" on R).
/// Recurses only through null-propagating functions; any other node is opaque and contributes {}.
static BitSet strictOnRelations(const ActionsDAG::Node * node, const JoinExpressionActions & actions)
{
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
        case ActionsDAG::ActionType::PLACEHOLDER:
            /// A leaf column reference is null exactly on its own relation.
            return JoinActionRef(node, actions).getSourceRelations();
        case ActionsDAG::ActionType::ALIAS:
            return node->children.empty() ? BitSet{} : strictOnRelations(node->children.front(), actions);
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node->function_base || !isNullPropagatingFunction(node->function_base->getName()))
                return {};
            BitSet result;
            for (const auto * child : node->children)
                result |= strictOnRelations(child, actions);
            return result;
        }
        case ActionsDAG::ActionType::COLUMN:
        case ActionsDAG::ActionType::ARRAY_JOIN:
            return {};
    }
    return {};
}

/// Relations R such that the boolean `node` is false or unknown when all of R's columns are NULL
/// (null-rejecting). Conservative: when unsure it returns a subset of the true answer, which only
/// tightens the reordering constraints downstream and so stays correct.
static BitSet predicateNullRejectingRelations(const ActionsDAG::Node * node, const JoinExpressionActions & actions)
{
    if (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        return predicateNullRejectingRelations(node->children.front(), actions);

    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
    {
        const auto & name = node->function_base->getName();
        /// AND is rejecting on R if either conjunct is (a false/unknown conjunct makes AND false/unknown).
        if (name == "and")
        {
            BitSet result;
            for (const auto * child : node->children)
                result |= predicateNullRejectingRelations(child, actions);
            return result;
        }
        /// OR is rejecting on R only if both disjuncts are.
        if (name == "or" && !node->children.empty())
        {
            BitSet result = predicateNullRejectingRelations(node->children.front(), actions);
            for (size_t i = 1; i < node->children.size(); ++i)
                result = result & predicateNullRejectingRelations(node->children[i], actions);
            return result;
        }
    }

    /// Otherwise the predicate is rejecting wherever it becomes NULL (its false path is ignored,
    /// which is the safe under-approximation). Covers comparisons and bare nullable-bool columns.
    return strictOnRelations(node, actions);
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical expected");
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    QueryPlan::Node * lhs_plan = node.children[0];
    QueryPlan::Node * rhs_plan = node.children[1];
    auto [lhs_label, rhs_label] = join_step->getInputLabels();
    auto join_kind = join_step->getJoinOperator().kind;

    auto type_changing_sides = join_step->typeChangingSides();
    bool allow_left_subgraph = !type_changing_sides.contains(JoinTableSide::Left) && (isInnerOrCross(join_kind) || isLeft(join_kind));
    bool allow_right_subgraph = !type_changing_sides.contains(JoinTableSide::Right) && (isInnerOrCross(join_kind) || isRight(join_kind));

    /// Check if flattening children would cause column name clashes between sides.
    if (allow_left_subgraph || allow_right_subgraph)
    {
        auto get_exposed_columns = [&](QueryPlan::Node * plan, bool allow_flatten) -> NameSet
        {
            NameSet names;
            auto * check = plan;
            if (allow_flatten)
            {
                bool merge_expression_into_join = query_graph.context->optimization_settings.merge_expression_into_join;
                auto * expr = typeid_cast<ExpressionStep *>(check->step.get());
                if (expr && !expr->getExpression().hasArrayJoin()
                    && (isPassthroughActions(expr->getExpression())
                        || canMergeExpressionIntoJoinGraph(expr->getExpression(), merge_expression_into_join)))
                {
                    check = check->children[0];
                }
                auto * js = typeid_cast<JoinStepLogical *>(check->step.get());
                if (js && !js->isOptimized() && check->children.size() == 2)
                {
                    for (auto * child : check->children)
                        for (const auto & col : *child->step->getOutputHeader())
                            names.insert(col.name);
                    return names;
                }
            }
            for (const auto & col : *plan->step->getOutputHeader())
                names.insert(col.name);
            return names;
        };

        auto lhs_cols = get_exposed_columns(lhs_plan, allow_left_subgraph);
        auto rhs_cols = get_exposed_columns(rhs_plan, allow_right_subgraph);

        if (std::ranges::any_of(lhs_cols, [&](const auto & name) { return rhs_cols.contains(name); }))
        {
            LOG_DEBUG(getLogger("optimizeJoin"), "Column name clash detected between join sides, disabling subgraph flattening");
            allow_left_subgraph = false;
            allow_right_subgraph = false;
        }
    }

    size_t lhs_count = addChildQueryGraph(query_graph, lhs_plan, nodes, lhs_label, allow_left_subgraph ? join_steps_limit - 1 : 0);
    size_t rhs_count = addChildQueryGraph(query_graph, rhs_plan, nodes, rhs_label, allow_right_subgraph ? static_cast<int>(join_steps_limit - lhs_count) : 0);

    size_t total_inputs = query_graph.inputs.size();

    chassert(lhs_count && rhs_count && lhs_count + rhs_count == total_inputs && query_graph.relation_stats.size() == total_inputs);

    auto [expression_actions, join_operator] = join_step->detachExpressions();

    auto get_raw_nodes = std::views::transform([](const auto & ref) { return ref.getNode(); });
    auto join_expression = std::ranges::to<std::vector>(join_operator.expression | get_raw_nodes);
    auto residual_filter = std::ranges::to<std::vector>(join_operator.residual_filter | get_raw_nodes);

    auto [expression_actions_dag, expression_actions_sources] = expression_actions.detachActionsDAG();

    auto existing_outputs = std::ranges::to<std::unordered_set>(query_graph.expression_actions.getActionsDAG()->getOutputs());

    ActionsDAG::NodeMapping node_mapping;
    query_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_actions_dag), node_mapping, true);

    ActionsDAG::NodeRawConstPtrs join_outputs = query_graph.expression_actions.getActionsDAG()->getOutputs();

    JoinExpressionActions::NodeToSourceMapping new_sources;
    for (const auto & [old_node, sources] : expression_actions_sources)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        if (BitSet(sources).set(0, false).set(1, false))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression node {} is not a binary join: {}", old_node->result_name, toString(sources));
        if (lhs_count == 1 && sources.count() == 1 && sources.test(0))
            new_sources[new_node] = BitSet().set(0);
        if (rhs_count == 1 && sources.count() == 1 && sources.test(1))
            new_sources[new_node] = BitSet().set(total_inputs - 1);
    }
    query_graph.expression_actions.setNodeSources(new_sources);

    BitSet left_mask = BitSet::allSet(lhs_count);
    BitSet right_mask = BitSet::allSet(rhs_count);
    right_mask.shift(lhs_count);

    ActionsDAG::NodeRawConstPtrs left_changes_types;
    ActionsDAG::NodeRawConstPtrs right_changes_types;
    for (const auto * out_node : join_outputs)
    {
        if (out_node->type == ActionsDAG::ActionType::INPUT ||
            out_node->type == ActionsDAG::ActionType::COLUMN ||
            existing_outputs.contains(out_node))
            continue;

        auto source = JoinActionRef(out_node, query_graph.expression_actions).getSourceRelations();
        auto rel_id = source.getSingleBit();
        if (!rel_id.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine source relations for node {}", out_node->result_name);

        if (rel_id == 0)
            left_changes_types.push_back(out_node);
        else
            right_changes_types.push_back(out_node);
    }

    if (!left_changes_types.empty())
        query_graph.type_changes[0] = std::move(left_changes_types);
    if (!right_changes_types.empty())
        query_graph.type_changes[total_inputs - 1] = std::move(right_changes_types);

    BitSet join_expression_sources;
    /// Relations on which the whole ON clause rejects nulls, for the conflict detectors. The ON
    /// clause is the conjunction of the `join_expression` conjuncts, so a relation is rejecting for
    /// the operator as soon as any conjunct rejects on it -- hence the union.
    ///
    /// Null-rejection is only meaningful when an unmatched outer-join row is padded with a real SQL
    /// NULL, i.e. under `join_use_nulls = 1`. With `join_use_nulls = 0` (the default) the padded value
    /// is a type default (`0`/`''`), so a "null-rejecting" predicate like `t2.id = t3.id` still
    /// matches the padded `0`; trusting null-rejection then unlocks unsound assoc/asscom reorderings
    /// -- e.g. `(t1 LEFT JOIN t2) LEFT JOIN t3` becoming `t1 LEFT JOIN (t2 LEFT JOIN t3)`, which
    /// changes the result. So we claim no null-rejection unless `join_use_nulls` is on, which makes
    /// the detectors fall back to the conservative (correct) outer-join reordering in that case.
    const bool trust_null_rejection = query_graph.context->optimization_settings.join_use_nulls;
    BitSet cda_nr_rels;
    for (const auto * old_node : join_expression)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        auto & edge = query_graph.join_edges.emplace_back(new_node, query_graph.expression_actions);

        /// Collect all sources from join expressions
        join_expression_sources |= edge.getSourceRelations();
        if (trust_null_rejection)
            cda_nr_rels |= predicateNullRejectingRelations(new_node, query_graph.expression_actions);

        /// ON-clause predicates of an outer join must be applied exactly at the step
        /// that joins the null-supplying relation, in its ON clause.
        /// Predicates of inner joins are filters: the optimizer may apply them at any
        /// step where all their source relations are available (as a post-join filter
        /// when that step is an outer join).
        if (isRightOrFull(join_kind))
        {
            query_graph.outer_join_conditions[edge] = 0;
        }
        else if (isLeftOrFull(join_kind))
        {
            query_graph.outer_join_conditions[edge] = total_inputs - 1;
        }
    }

    /// Capture this operator for the CD-A conflict detector before `join_expression_sources` is
    /// stripped of the null-supplying singleton below. `left_mask`/`right_mask` are this
    /// operator's two input subtrees in the current (sub)graph's local numbering; `nel` is the
    /// full set of relations the ON clause references. Records are shifted into global numbering
    /// by `uniteGraphs`.
    query_graph.conflict_ops.push_back(ConflictJoinOp{left_mask, right_mask, join_expression_sources, cda_nr_rels, join_kind, join_operator.strictness});

    if (isRightOrFull(join_kind))
    {
        if (lhs_count != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical with RIGHT or FULL join must have exactly one left input, but has {}", lhs_count);
        join_expression_sources.set(0, false);
        query_graph.join_kinds[0] = std::make_pair(join_expression_sources, join_kind);
    }
    if (isLeftOrFull(join_kind))
    {
        if (rhs_count != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical with LEFT or FULL join must have exactly one right input, but has {}", rhs_count);
        join_expression_sources.set(total_inputs - 1, false);
        query_graph.join_kinds[total_inputs - 1] = std::make_pair(join_expression_sources, join_kind);
    }

    if (!residual_filter.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Residual filter is not supported in join reorder");
}

static std::vector<DPJoinEntry *> getJoinTreePostOrderSequence(DPJoinEntryPtr root)
{
    std::vector<DPJoinEntry *> result;
    result.reserve(root->relations.count() * 2);

    std::vector<DPJoinEntry *> stack;
    stack.push_back(root.get());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (!node->isLeaf())
        {
            if (!node->left || !node->right)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join node should have both left and right children");

            /// In post-order, we want LEFT -> RIGHT -> NODE
            /// Since we're using a stack (LIFO), we push RIGHT then LEFT
            stack.push_back(node->right.get());
            stack.push_back(node->left.get());
        }

        result.push_back(node);
    }

    /// Reverse the order to get post-order traversal
    std::reverse(result.begin(), result.end());

    return result;
}

/// Find single input node that is parent of the node
/// Only aliases and cast/toNullable functions are allowed in the path, otherwise an logical error is thrown
static const ActionsDAG::Node * trackInputColumn(const ActionsDAG::Node * node)
{
    while (!node->children.empty())
    {

        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & function_name = node->function_base->getName();
            if (function_name != "toNullable" && function_name != "_CAST" && function_name != "CAST")
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} is a function '{}', expected toNullable or CAST",
                    node->result_name, function_name);
        }

        size_t non_const_children = 0;
        for (const auto * child_node : node->children)
        {
            if (child_node->type == ActionsDAG::ActionType::COLUMN)
                continue;
            node = child_node;
            non_const_children++;
        }

        if (non_const_children != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} has {} non const children, expected 1", node->result_name, non_const_children);
    }
    return node;
}

constexpr bool isSwapOnlyJoinKind(JoinKind kind)
{
    return kind == JoinKind::Full;
}

constexpr bool isSwapOnlyJoinStrictness(JoinStrictness strictness)
{
    return strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
}

/// Nanoseconds of probe-time work one candidate row costs when an equality is checked during the
/// probe instead of being hashed into the key. Anchored on measurements of a FULL ALL join over
/// 10M rows: about 40 ns for a fixed-width key and about 290 ns for a String one. Both are
/// dominated by gathering the values out of the stored blocks through the row-ref lists, not by the
/// comparison itself, which is why the width of the key matters so much more than its type.
static Float64 probeCostPerCandidateNs(const DataTypes & demoted_types, JoinKind kind, JoinStrictness strictness)
{
    Float64 cost = 0.0;
    for (const auto & type : demoted_types)
    {
        const auto & inner = removeNullable(removeLowCardinality(type));
        if (!inner->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            cost += 290.0;
        else if (inner->getSizeOfValueInMemory() <= sizeof(UInt64))
            cost += 8.0;
        else
            cost += 16.0;
    }

    /// An outer join pays for bookkeeping an inner join does not: a probe row whose whole bucket
    /// fails still has to be NULL-extended (`add_missing`, so no early exit), and for RIGHT/FULL
    /// every surviving candidate writes a used-flag for the non-joined pass (`need_flags`).
    if (isRightOrFull(kind))
        cost *= 1.5;
    else if (isLeft(kind))
        cost *= 1.2;

    /// ANY/SEMI/ANTI stop at the first surviving candidate rather than walking the whole bucket.
    if (strictness != JoinStrictness::All)
        cost *= 0.5;

    return cost;
}

/// Bytes of hash-table cells the demotion would avoid allocating: what the full key set needs,
/// minus what the kept subset needs. Both sides go through `HashJoin` so the cell size and the
/// power-of-two growth match the map that will actually be built, including the case where dropping
/// a key moves the whole key set into a narrower method. Returns a signed value because a narrower
/// key set can land on a *larger* array once rounding is taken into account.
static Int64 estimatedTableBytesSaved(
    const auto & candidate,
    Float64 full_ndv,
    const std::vector<const ActionsDAG::Node *> & right_key_nodes,
    size_t equality_count,
    JoinStrictness strictness)
{
    DataTypes full_types;
    for (const auto * node : right_key_nodes)
        full_types.push_back(node->result_type);

    DataTypes kept_types;
    for (size_t i : candidate.indices)
        kept_types.push_back(right_key_nodes[i]->result_type);

    if (full_types.size() != equality_count || kept_types.empty())
        return 0;

    /// Two-level maps hold the same cell type, so the choice only shifts how the rounding falls;
    /// ask for the single-level variant and accept that slack.
    const auto full_method = HashJoin::chooseMethodForTypes(full_types, /*use_two_level_maps=*/ false);
    const auto kept_method = HashJoin::chooseMethodForTypes(kept_types, /*use_two_level_maps=*/ false);

    const size_t full_bytes = HashJoin::estimateTableBytes(
        static_cast<size_t>(full_ndv), full_method, strictness);
    const size_t kept_bytes = HashJoin::estimateTableBytes(
        candidate.ndv, kept_method, strictness);

    /// A variant whose cell size is unknown reports 0; do not turn that into a fictional saving.
    if (!full_bytes || !kept_bytes)
        return 0;

    return static_cast<Int64>(full_bytes) - static_cast<Int64>(kept_bytes);
}

/// Cardinality-driven optimization: when the equality keys of a JOIN together have a far higher NDV
/// than one of their subsets, build the hash table on that subset only and evaluate the remaining
/// equalities per row during the probe. This shrinks the hash table on multi-key joins whose trailing
/// keys are near-unique, e.g. `ON l.user_id = r.user_id AND l.request_id = r.request_id` where
/// `request_id` is unique per user: the table is then keyed on the `user_id` space instead of the
/// `user_id x request_id` space.
///
/// Runs here, while the reordered join is being emitted, rather than during physical conversion:
///   - the build-side row count and per-column NDVs are already known, so no extra
///     walk of the right subtree is needed;
///   - the demoted equalities leave `JoinOperator::expression` before
///     `deriveCacheKeysForNewJoin` runs, so the `HashTablesStatistics` cache key derived there
///     covers exactly the keys the hash table is built on, with no separate bookkeeping.
///
/// The demoted equalities move to `JoinOperator::probe_conditions`; `buildPhysicalJoinImpl` routes
/// those into `mixed_join_expression`, which is evaluated during the probe and therefore preserves
/// outer-join NULL-extension (a post-join filter would not).
///
/// Returns true if at least one equality was demoted.
static bool demoteHighNdvKeysToProbe(
    JoinStepLogical & join_step,
    std::optional<UInt64> build_rows,
    const std::unordered_map<String, ColumnStats> & build_column_stats,
    RuntimeHashStatisticsContext & statistics_context,
    UInt64 right_raw_hash)
{
    const auto & join_settings = join_step.getJoinSettings();
    if (!join_settings.query_plan_hash_join_subset_keys_auto)
        return false;

    /// The demoted equality survives only as a mixed join expression, which just the hash family
    /// evaluates: `chooseJoinAlgorithm` rejects a mixed condition outright unless hash, parallel
    /// hash or grace hash is enabled, and an algorithm that is applicable but blind to mixed
    /// conditions (any merge flavour, `auto`, `direct`, IEJoin) would be picked first and silently
    /// drop the equality, producing extra rows. Demote only when every enabled algorithm evaluates
    /// it - an allowlist, so a newly added algorithm is excluded until it is known to support this.
    auto evaluates_mixed_conditions = [](JoinAlgorithm algorithm)
    {
        return algorithm == JoinAlgorithm::HASH
            || algorithm == JoinAlgorithm::PARALLEL_HASH
            || algorithm == JoinAlgorithm::GRACE_HASH
            /// Deprecated spelling of `direct,hash`: `tryDirectJoin` declines a mixed condition and
            /// the join falls through to `HashJoin`.
            || algorithm == JoinAlgorithm::DEFAULT;
    };
    if (!std::ranges::all_of(join_settings.join_algorithms, evaluates_mixed_conditions))
        return false;

    auto & join_operator = join_step.getJoinOperator();
    if (!HashJoin::isAdditionalFilterSupported(join_operator.kind, join_operator.strictness))
        return false;

    if (!build_rows || *build_rows < join_settings.query_plan_hash_join_subset_keys_min_rows)
        return false;
    const Float64 rows = static_cast<Float64>(*build_rows);

    /// Positions in `join_operator.expression` of the plain cross-side equalities, with the build-side
    /// node of each. `NullSafeEquals` is left alone: the probe-time rewrite compares with `equals`,
    /// which does not match its semantics. Anything else (a residual predicate, a disjunction) is not
    /// a hash key to begin with.
    std::vector<size_t> equality_positions;
    std::vector<const ActionsDAG::Node *> right_key_nodes;
    for (size_t i = 0; i < join_operator.expression.size(); ++i)
    {
        auto [op, lhs, rhs] = join_operator.expression[i].asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        if (lhs.fromLeft() && rhs.fromRight())
            right_key_nodes.push_back(rhs.getNode());
        else if (lhs.fromRight() && rhs.fromLeft())
            right_key_nodes.push_back(lhs.getNode());
        else
            continue;
        equality_positions.push_back(i);
    }

    /// Demotion has to leave at least one hash key behind, so it needs at least two.
    if (equality_positions.size() < 2)
        return false;

    /// Candidate kept-key subsets, each with the NDV the hash table would have if built on it.
    /// Two sources:
    ///  (a) column statistics on the build side - one size-1 candidate per key with a known NDV;
    ///  (b) the `HashTablesStatistics` cache - a candidate for any subset some earlier query on this
    ///      same subtree happened to build a hash table on. That value is a measured joint NDV, so it
    ///      accounts for correlation between keys, which multiplying per-column NDVs cannot.
    struct Candidate
    {
        std::vector<size_t> indices;
        UInt64 ndv;
    };
    std::vector<Candidate> candidates;

    for (size_t i = 0; i < right_key_nodes.size(); ++i)
    {
        auto it = build_column_stats.find(right_key_nodes[i]->result_name);
        if (it == build_column_stats.end() || it->second.num_distinct_values == 0)
            continue;
        candidates.push_back({{i}, it->second.num_distinct_values});
    }

    if (right_raw_hash)
    {
        /// Enumerate subsets up to a small bound. Hits are sparse - only subsets the workload has
        /// actually built - so the cost is one SipHash plus one map probe per subset; the cap keeps
        /// the worst case bounded on joins with many equi keys.
        const size_t keys_count = right_key_nodes.size();
        const size_t max_subset_size = std::min<size_t>(keys_count, 4);
        std::vector<size_t> subset;
        std::vector<const ActionsDAG::Node *> subset_nodes;
        subset.reserve(max_subset_size);
        subset_nodes.reserve(max_subset_size);

        auto probe_subset = [&]
        {
            subset_nodes.clear();
            for (size_t i : subset)
                subset_nodes.push_back(right_key_nodes[i]);
            if (auto hint = statistics_context.getCachedHintForKeys(right_raw_hash, join_step.getSerializationName(), subset_nodes))
                candidates.push_back({subset, hint->ht_size});
        };

        auto enumerate = [&](auto & self, size_t start, size_t depth_remaining) -> void
        {
            if (!subset.empty())
                probe_subset();
            if (depth_remaining == 0)
                return;
            for (size_t i = start; i < keys_count; ++i)
            {
                subset.push_back(i);
                self(self, i + 1, depth_remaining - 1);
                subset.pop_back();
            }
        };
        enumerate(enumerate, 0, max_subset_size);
    }

    if (candidates.empty())
        return false;

    /// The same subset can be scored by both sources. Keep the smallest NDV for it - over-stated
    /// distinctness would under-state the bucket size and make demotion look better than it is.
    std::ranges::sort(candidates, [](const auto & lhs, const auto & rhs)
    {
        if (lhs.indices != rhs.indices)
            return lhs.indices < rhs.indices;
        return lhs.ndv < rhs.ndv;
    });
    candidates.erase(
        std::unique(candidates.begin(), candidates.end(), [](const auto & lhs, const auto & rhs) { return lhs.indices == rhs.indices; }),
        candidates.end());

    /// Smallest hash table first, ties broken toward fewer kept keys (cheaper per-row hashing).
    std::ranges::sort(candidates, [](const auto & lhs, const auto & rhs)
    {
        if (lhs.ndv != rhs.ndv)
            return lhs.ndv < rhs.ndv;
        return lhs.indices.size() < rhs.indices.size();
    });

    /// Candidates must still discriminate to `rows * min_kept_selectivity` distinct values, so the
    /// probe-time equalities run over a bounded bucket rather than a large fraction of the table.
    const Float64 target_ndv = rows * join_settings.query_plan_hash_join_subset_keys_min_kept_selectivity;

    /// The NDV of the whole key set, which is what the hash table is keyed on today. Prefer a
    /// measured joint value for it; otherwise assume the keys are independent, bounded by the row
    /// count. It is only ever used to size the table the demotion would avoid building.
    Float64 full_ndv = 1.0;
    for (const auto & candidate : candidates)
    {
        if (candidate.indices.size() == equality_positions.size())
        {
            full_ndv = static_cast<Float64>(candidate.ndv);
            break;
        }
        if (candidate.indices.size() == 1)
            full_ndv *= static_cast<Float64>(candidate.ndv);
    }
    full_ndv = std::min(full_ndv, rows);

    /// Both sides of the trade, per candidate:
    ///
    ///  - cost: the mean bucket the kept keys leave, times what one candidate check costs for the
    ///    keys being demoted. Demoting can never make the probe cheaper - it only shrinks the hash
    ///    table - so this is what is being paid, and it is capped rather than merely compared.
    ///  - benefit: the cell array the full key set allocates minus the one the kept subset would.
    ///
    /// The cheapest surviving candidate wins. Picking the smallest-NDV one instead, as an earlier
    /// version did, systematically demotes the most discriminating key, which is exactly the choice
    /// that inflates the bucket the most.
    const Candidate * chosen = nullptr;
    Float64 chosen_cost = std::numeric_limits<Float64>::infinity();
    for (const auto & candidate : candidates)
    {
        if (static_cast<Float64>(candidate.ndv) < target_ndv)
            continue;
        if (candidate.indices.size() == equality_positions.size())
            continue;

        DataTypes demoted_types;
        std::vector<bool> in_candidate(right_key_nodes.size(), false);
        for (size_t i : candidate.indices)
            in_candidate[i] = true;
        for (size_t i = 0; i < right_key_nodes.size(); ++i)
        {
            if (!in_candidate[i])
                demoted_types.push_back(right_key_nodes[i]->result_type);
        }
        if (demoted_types.empty())
            continue;

        const Float64 mean_bucket = rows / std::max(1.0, static_cast<Float64>(candidate.ndv));
        const Float64 cost = mean_bucket
            * probeCostPerCandidateNs(demoted_types, join_operator.kind, join_operator.strictness);
        if (cost > join_settings.query_plan_hash_join_subset_keys_max_probe_cost_ns)
            continue;

        const Int64 saving = estimatedTableBytesSaved(
            candidate, full_ndv, right_key_nodes, equality_positions.size(), join_operator.strictness);
        if (saving < static_cast<Int64>(join_settings.query_plan_hash_join_subset_keys_min_saving_bytes))
            continue;

        if (cost < chosen_cost)
        {
            chosen = &candidate;
            chosen_cost = cost;
        }
    }
    if (!chosen)
        return false;

    std::vector<bool> kept(right_key_nodes.size(), false);
    for (size_t i : chosen->indices)
        kept[i] = true;

    /// Move the demoted equalities out of the ON expression, leaving every other condition
    /// (a non-equi predicate that already had to be evaluated during the join) where it was.
    std::vector<JoinActionRef> kept_expression;
    kept_expression.reserve(join_operator.expression.size());
    size_t equality_index = 0;
    size_t demoted_count = 0;
    for (size_t i = 0; i < join_operator.expression.size(); ++i)
    {
        if (equality_index >= equality_positions.size() || equality_positions[equality_index] != i)
        {
            kept_expression.push_back(join_operator.expression[i]);
            continue;
        }

        if (kept[equality_index])
            kept_expression.push_back(join_operator.expression[i]);
        else
        {
            join_operator.probe_conditions.push_back(join_operator.expression[i]);
            ++demoted_count;
        }
        ++equality_index;
    }
    join_operator.expression = std::move(kept_expression);

    LOG_DEBUG(
        getLogger("optimizeJoin"),
        "Demoted {} of {} JOIN equality keys to probe-time conditions (right_rows={}, kept_ndv={}, target_ndv={})",
        demoted_count, equality_positions.size(), *build_rows, chosen->ndv, target_ndv);

    return true;
}

static QueryPlan::Node chooseJoinOrder(QueryGraphBuilder query_graph_builder, QueryPlan::Nodes & nodes, JoinStrictness join_strictness)
{
    QueryGraph query_graph;
    query_graph.relation_stats = std::move(query_graph_builder.relation_stats);
    query_graph.edges = std::move(query_graph_builder.join_edges);
    query_graph.join_kinds = std::move(query_graph_builder.join_kinds);
    query_graph.outer_join_conditions = std::move(query_graph_builder.outer_join_conditions);
    query_graph.conflict_ops = std::move(query_graph_builder.conflict_ops);

    LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "Optimizing join order for query graph with {} relations", query_graph.relation_stats.size());

    std::unordered_map<BitSet, RelationEstimateInfo> relation_infos;
    Strings relations_without_statistics;
    std::vector<UInt8> leaf_imprecise(query_graph.relation_stats.size());
    /// Leaves whose row count had to be guessed because column statistics are missing. Distinct from
    /// `leaf_imprecise`, which is also set for the synthetic test sources (a stats hint, randomized
    /// stats) - those carry usable NDVs, a primary-index guess does not.
    std::vector<UInt8> leaf_missing_statistics(query_graph.relation_stats.size());
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        leaf_imprecise[i] = rel.imprecise_estimate;
        leaf_missing_statistics[i] = isMissingStatisticsSource(rel.source);

        relation_infos[BitSet().set(i)] = RelationEstimateInfo{
            .name = rel.table_name.empty() ? fmt::format("R{}", i) : rel.table_name,
            .estimated_rows = rel.estimated_rows,
            .source = rel.source,
            .imprecise_estimate = rel.imprecise_estimate};

        if (isMissingStatisticsSource(rel.source))
            relations_without_statistics.push_back(rel.table_name.empty() ? fmt::format("table{}", i) : rel.table_name);
    }

    /// The listed names can be aliases or subquery labels, so no concrete `ALTER` command is suggested.
    if (!relations_without_statistics.empty())
        LOG_DEBUG(
            getLogger("optimizeJoin"),
            "Join order optimization uses imprecise row count estimates derived from the primary index "
            "because the following relation(s) have no column statistics available for join reordering: {}. "
            "The chosen join order may be suboptimal. Consider creating column statistics for the joined tables "
            "(for example, `ALTER TABLE <table> MATERIALIZE STATISTICS ALL`) and enabling the 'use_statistics' setting",
            fmt::join(relations_without_statistics, ", "));

    auto global_expression_actions = std::move(query_graph_builder.expression_actions);
    auto global_actions_dag = global_expression_actions.getActionsDAG();
    if (!global_actions_dag)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global expression actions DAG is not set");

    const auto & optimization_settings = query_graph_builder.context->optimization_settings;
    const UInt64 cluster_id = ++optimization_settings.join_reorder_next_cluster_id;

    auto optimized = optimizeJoinOrder(std::move(query_graph), optimization_settings);
    auto sequence = getJoinTreePostOrderSequence(optimized);

    if (sequence.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join tree is empty");

    /// Mapping from node to maximal step position where it is used
    /// It's used to drop unused expressions
    std::unordered_map<const ActionsDAG::Node *, size_t> usage_level_map;
    {
        std::deque<std::pair<const ActionsDAG::Node *, size_t>> stack;

        /// Join expressions used by i-th join step
        for (size_t i = 0; i < sequence.size(); ++i)
        {
            const auto & entry = sequence[i];
            for (const auto & action : entry->join_operator.expression)
                stack.emplace_back(action.getNode(), i);
            for (const auto & action : entry->join_operator.residual_filter)
                stack.emplace_back(action.getNode(), i);
        }
        /// Outputs at the end
        for (const auto & out : global_actions_dag->getOutputs())
            stack.emplace_back(out, sequence.size());

        while (!stack.empty())
        {
            auto [node, level] = stack.back();
            stack.pop_back();

            /// Stack is sorted with respect to level
            /// If we processed node it means that it and its children used by higher level
            auto [_, inserted] = usage_level_map.try_emplace(node, level);
            if (!inserted)
                continue;

            for (const auto * child : node->children)
                stack.push_back(std::make_pair(child, level));
        }
    }

    std::stack<QueryPlan::Node *> nodeStack;
    auto & input_nodes = query_graph_builder.inputs;

    if (!query_graph_builder.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryGraphBuilder context is not set");

    auto join_settings = std::move(query_graph_builder.context->join_settings);
    auto sorting_settings = std::move(query_graph_builder.context->sorting_settings);

    /// After applying OUTER joins some columns may change their types (in case of join_use_nulls or JOIN USING)
    /// We need to track this change, this map tracks input columns and how they are transformed
    /// Each next step uses mapped columns as inputs

    /// Input in global dag -> it's position
    std::unordered_map<const ActionsDAG::Node *, size_t> input_node_map;

    /// input_position -> (relation no, input)
    std::vector<std::pair<size_t, const ActionsDAG::Node *>> current_input_nodes;

    const auto & global_inputs = global_actions_dag->getInputs();
    for (size_t input_idx = 0; input_idx < global_inputs.size(); ++input_idx)
    {
        const auto * input = global_inputs[input_idx];
        auto src_rels = JoinActionRef(input, global_expression_actions).getSourceRelations();
        auto rel_idx = src_rels.getSingleBit();
        if (!rel_idx.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input node {} has {} source relations, expected 1", input->result_name, toString(src_rels));

        current_input_nodes.emplace_back(rel_idx.value(), input);
        input_node_map[input] = input_idx;
    }

    /// Decide once whether this graph's per-entry strictnesses are authoritative. `buildPhysicalPlan`
    /// leaves every DP entry `All` except a semi/anti join that a conflict detector (CD-A/CD-C)
    /// admitted while reordering, which carries its own strictness. If any entry carries a specific
    /// (non-`All`) strictness the graph is mixed -- inner joins around a reordered semi/anti join,
    /// which may be nested anywhere, even under an inner top join -- and each entry's own strictness
    /// must be kept. Otherwise the graph is uniform and its single `join_strictness` must be stamped
    /// onto the reconstructed joins (e.g. an ANY/SEMI/ANTI join that was only swapped, or the whole
    /// graph under the default greedy algorithm, which never populates per-entry strictness).
    bool graph_has_mixed_strictness = false;
    for (const auto * seq_entry : sequence)
        if (!seq_entry->isLeaf() && seq_entry->join_operator.strictness != JoinStrictness::All)
            graph_has_mixed_strictness = true;

    for (size_t entry_idx = 0; entry_idx < sequence.size(); ++entry_idx)
    {
        auto * entry = sequence[entry_idx];
        if (entry->isLeaf())
        {
            /// Base relation, use this step as input node
            size_t relation_id = safe_cast<size_t>(entry->relation_id);
            if (relation_id >= input_nodes.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid relation id: {}, input nodes size: {}", relation_id, input_nodes.size());
            nodeStack.push(input_nodes[relation_id]);
        }
        else
        {
            /// Combine two nodes from the stack into a single join operation
            auto * left_child_node = nodeStack.top();
            nodeStack.pop();
            auto * right_child_node = nodeStack.top();
            nodeStack.pop();

            auto join_operator = std::move(entry->join_operator);
            /// See `graph_has_mixed_strictness` above: keep each entry's own strictness in a mixed
            /// graph, otherwise stamp the graph's single strictness.
            if (!graph_has_mixed_strictness)
                join_operator.strictness = join_strictness;

            /// The optimizer reconstructs an unconnected Inner pair (e.g. `INNER JOIN ... ON 1`,
            /// which produces no join edges) as Cross. That is equivalent only for ALL strictness:
            /// a Cross join ignores strictness, so `ANY INNER JOIN ... ON 1` would degrade to a full
            /// cartesian product. Restore Inner so that the physical join falls back to the
            /// constant-key join (`__lhs_const = __rhs_const`) that preserves strictness semantics.
            if (join_strictness != JoinStrictness::All && join_operator.kind == JoinKind::Cross)
                join_operator.kind = JoinKind::Inner;
            auto left_rels = entry->left->relations;
            auto right_rels = entry->right->relations;

            bool has_prepared_storage_at_right = bool(typeid_cast<const JoinStepLogicalLookup *>(right_child_node->step.get()));
            bool has_prepared_storage_at_left = bool(typeid_cast<const JoinStepLogicalLookup *>(left_child_node->step.get()));

            auto lhs_estimation = entry->left->estimated_rows;
            auto rhs_estimation = entry->right->estimated_rows;

            bool swap_on_sizes = optimization_settings.join_swap_table.has_value()
                ? optimization_settings.join_swap_table.value()
                : entry->join_method == JoinMethod::Hash && lhs_estimation && rhs_estimation
                    && lhs_estimation.value() < rhs_estimation.value();

            bool flip_join = has_prepared_storage_at_left || (!has_prepared_storage_at_right && swap_on_sizes);

            /// fixme: USING clause handled specially in join algorithm, so swap breaks it
            /// At the time of writing, we're not able to swap inputs for ANY or SEMI partial merge join, because it only supports inner or left joins, but not right
            /// ANTI partial merge join is not supported for any join kind
            const bool partial_merge_join_can_be_selected = std::ranges::any_of(
                join_settings.join_algorithms,
                [](JoinAlgorithm alg)
                { return alg == JoinAlgorithm::PARTIAL_MERGE || alg == JoinAlgorithm::PREFER_PARTIAL_MERGE || alg == JoinAlgorithm::AUTO; });
            const bool should_worry_about_partial_merge_join = partial_merge_join_can_be_selected
                && (!MergeJoin::isSupported(join_operator.kind, join_operator.strictness)
                    || !MergeJoin::isSupported(reverseJoinKind(join_operator.kind), join_operator.strictness));
            bool skip_flip_any_join = join_settings.join_any_take_last_row && join_operator.strictness == JoinStrictness::Any;
            const bool suitable_swap_only_join = isSwapOnlyJoinStrictness(join_operator.strictness)
                && !should_worry_about_partial_merge_join
                && !skip_flip_any_join;
            if (join_operator.strictness != JoinStrictness::All && !suitable_swap_only_join)
                flip_join = false;

            if (flip_join)
            {
                /// For hash joins, we want to keep the smaller side on the right
                std::swap(left_rels, right_rels);
                std::swap(left_child_node, right_child_node);
                join_operator.kind = reverseJoinKind(join_operator.kind);
            }

            auto left_header_ptr = left_child_node->step->getOutputHeader();
            auto right_header_ptr = right_child_node->step->getOutputHeader();

            const auto & left_header = *left_header_ptr;
            const auto & right_header = *right_header_ptr;

            ActionsDAG::NodeRawConstPtrs required_output_nodes;

            /// input pos -> new input node
            std::unordered_map<size_t, const ActionsDAG::Node *> current_step_type_changes;

            for (auto rel_id : {left_rels.getSingleBit(), right_rels.getSingleBit()})
            {
                if (!rel_id.has_value())
                    continue;
                const auto & new_inputs = query_graph_builder.type_changes[rel_id.value()];
                for (const auto * new_input : new_inputs)
                {
                    const auto * input_node = trackInputColumn(new_input);
                    auto it = input_node_map.find(input_node);
                    if (it == input_node_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", input_node->result_name, global_actions_dag->dumpDAG());
                    current_step_type_changes[it->second] = new_input;
                }
            }

            auto joined_mask = entry->relations;
            ActionsDAG::NodeMapping current_inputs;
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                current_inputs[input_node] = input_node;

                const auto * out_node = input_node;
                if (auto it2 = current_step_type_changes.find(input_pos); it2 != current_step_type_changes.end())
                    out_node = it2->second;
                /// add input (possibly which changed type) to required output
                required_output_nodes.push_back(out_node);
            }

            for (const auto & action : join_operator.expression)
                required_output_nodes.push_back(action.getNode());
            for (const auto & action : join_operator.residual_filter)
                required_output_nodes.push_back(action.getNode());

            if (entry_idx == sequence.size() - 1)
            {
                for (const auto * output_node : global_actions_dag->getOutputs())
                    required_output_nodes.push_back(output_node);
            }

            JoinExpressionActions current_expression_actions(left_header, right_header,
                ActionsDAG::foldActionsByProjection(current_inputs, required_output_nodes));

            auto current_dag = current_expression_actions.getActionsDAG();
            auto & dag_outputs = current_dag->getOutputs();
            if (required_output_nodes.size() != dag_outputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output nodes size {} does not match current output nodes size in dag {}", required_output_nodes.size(), current_dag->dumpDAG());

            auto join_expression_map = std::ranges::to<ActionsDAG::NodeMapping>(std::views::zip(required_output_nodes, dag_outputs));
            auto remap_action = [&](JoinActionRef & action)
            {
                const auto * mapped_node = join_expression_map[action.getNode()];
                if (!mapped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} not found in current dag {}", action.getNode()->result_name, current_dag->dumpDAG());
                action = JoinActionRef(mapped_node, current_expression_actions);
            };
            for (auto & action : join_operator.expression)
                remap_action(action);
            for (auto & action : join_operator.residual_filter)
                remap_action(action);

            /// Setup outputs after join
            dag_outputs.clear();
            for (auto [input_pos, new_input] : current_step_type_changes)
            {
                current_input_nodes.at(input_pos).second = new_input;
            }

            const ActionsDAG::Node * first_dropped_node = nullptr;
            size_t first_dropped_node_pos = 0;
            /// Columns returned from JOIN is input with possibly corrected type
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto & [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                if (usage_level_map[input_node] <= entry_idx)
                {
                    if (!first_dropped_node)
                    {
                        auto mapped_it = join_expression_map.find(input_node);
                        if (mapped_it == join_expression_map.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", input_node->result_name);
                        first_dropped_node = mapped_it->second;
                        first_dropped_node_pos = input_pos;
                    }
                    input_node = nullptr;
                    continue;
                }

                auto mapped_it = join_expression_map.find(input_node);
                if (mapped_it == join_expression_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", input_node->result_name);
                dag_outputs.push_back(mapped_it->second);
            }
            auto actions_after_join = dag_outputs;

            /// Last step, output should correspond to the global actions DAG
            if (entry_idx == sequence.size() - 1)
            {
                dag_outputs.clear();
                for (const auto * output_node : global_actions_dag->getOutputs())
                {
                    auto mapped_it = join_expression_map.find(output_node);
                    if (mapped_it == join_expression_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", output_node->result_name);
                    dag_outputs.push_back(mapped_it->second);
                    /// Include COLUMN Const nodes (like `__join_result_dummy`) in actions_after_join.
                    /// These are `fromNone` and will be placed in left_dag, ensuring the left pre-join
                    /// always produces at least one output column (so blocks have correct row count).
                    if (mapped_it->second->type == ActionsDAG::ActionType::COLUMN)
                        actions_after_join.push_back(mapped_it->second);
                }
            }

            if (actions_after_join.empty())
            {
                if (!first_dropped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned from join: {}", current_dag->dumpDAG());
                actions_after_join.push_back(first_dropped_node);
            }

            if (dag_outputs.empty())
            {
                if (!first_dropped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned from join: {}", current_dag->dumpDAG());
                dag_outputs.push_back(first_dropped_node);
                current_input_nodes.at(first_dropped_node_pos).second = first_dropped_node;
            }

            auto join_step = std::make_unique<JoinStepLogical>(
                left_header_ptr,
                right_header_ptr,
                std::move(join_operator),
                std::move(current_expression_actions),
                actions_after_join,
                join_settings,
                sorting_settings);

            /// Diagnostic only: a join is imprecise if any of its leaves was (see `leaf_imprecise` above).
            bool imprecise_estimate = false;
            for (size_t i = 0; i < leaf_imprecise.size(); ++i)
                if (entry->relations.test(i))
                    imprecise_estimate |= leaf_imprecise[i];

            join_step->setInputRelations(relation_infos[left_rels], relation_infos[right_rels]);
            relation_infos[entry->relations] = RelationEstimateInfo{
                .name = join_step->getReadableRelationName(),
                .estimated_rows = entry->estimated_rows,
                .imprecise_estimate = imprecise_estimate,
                .composite = true};

            join_step->setOptimized(entry->estimated_rows, entry->column_stats, imprecise_estimate, entry->cost, entry->selectivity, cluster_id);

            /// Demote before the cache keys below are derived: they hash the equalities left in the ON
            /// expression, so the key then identifies the hash table that is actually built.
            /// Skipped when the build-side row count had to be guessed because column statistics are
            /// missing - choosing a key subset from a primary-index guess can shrink the wrong side.
            bool build_side_missing_statistics = false;
            for (size_t i = 0; i < leaf_missing_statistics.size(); ++i)
                if (right_rels.test(i))
                    build_side_missing_statistics |= leaf_missing_statistics[i];

            if (!build_side_missing_statistics)
            {
                /// `left_rels`/`right_rels` and the child nodes were swapped together above, so the
                /// build side is the DP entry the post-swap right child came from.
                const auto & build_entry = flip_join ? *entry->left : *entry->right;
                auto & statistics_context = query_graph_builder.context->statistics_context;
                demoteHighNdvKeysToProbe(
                    *join_step,
                    build_entry.estimated_rows,
                    build_entry.column_stats,
                    statistics_context,
                    statistics_context.getRawHash(right_child_node));
            }

            auto & new_node = nodes.emplace_back();

            auto join_cache_keys = query_graph_builder.context->statistics_context
                .deriveCacheKeysForNewJoin(left_child_node, right_child_node, new_node, *join_step);
            if (join_cache_keys.right_key)
                join_step->setRightHashTableCacheKey(join_cache_keys.right_key);
            if (join_cache_keys.output_key)
                join_step->setJoinOutputCacheKey(join_cache_keys.output_key);

            new_node.step = std::move(join_step);
            new_node.children = {left_child_node, right_child_node};
            nodeStack.push(&new_node);
        }
    }

    if (nodeStack.size() != 1 || nodeStack.top() != &nodes.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal join sequence produced: [{}]",
            fmt::join(sequence | std::views::transform([](const auto * e) { return e ? e->dump() : "null"; }), ", "));

    /// Return the current node by value and remove it from the nodes list
    /// Caller may put the node back into the list if needed or replace existing node
    auto result = std::move(nodes.back());
    nodes.pop_back();
    return result;
}

static void collectJoinGraphRelationHeaders(
    const QueryPlan::Node * node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children);

/// Mirrors `buildQueryGraph` for a single join node: collects the output headers of the relations
/// that the join order optimizer would produce for it (descending into flattenable child joins).
/// `allow_semi_anti_children` must match `addChildQueryGraph`'s flatten gate so this header shadow
/// stays in sync when CD-A semi/anti reordering is enabled.
static void collectJoinGraphRelationHeadersForJoin(
    const QueryPlan::Node & join_node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children)
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(join_node.step.get());
    if (!join_step || join_node.children.size() != 2)
    {
        relation_headers.push_back(join_node.step->getOutputHeader());
        return;
    }

    const auto join_kind = join_step->getJoinOperator().kind;
    const auto type_changing_sides = join_step->typeChangingSides();

    const bool allow_left_subgraph
        = !type_changing_sides.contains(JoinTableSide::Left) && (isInnerOrCross(join_kind) || isLeft(join_kind));
    const size_t lhs_before = relation_headers.size();
    collectJoinGraphRelationHeaders(join_node.children[0], allow_left_subgraph ? join_steps_limit - 1 : 0, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children);
    const size_t lhs_count = relation_headers.size() - lhs_before;

    const bool allow_right_subgraph
        = !type_changing_sides.contains(JoinTableSide::Right) && (isInnerOrCross(join_kind) || isRight(join_kind));
    collectJoinGraphRelationHeaders(
        join_node.children[1], allow_right_subgraph ? static_cast<int>(join_steps_limit - lhs_count) : 0, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children);
}

/// Mirrors `addChildQueryGraph`: either flattens a child join into multiple relations, or treats
/// the (possibly expression-step-peeled) node as a single relation and records its output header.
static void collectJoinGraphRelationHeaders(
    const QueryPlan::Node * node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children)
{
    /// Peeling must match `addChildQueryGraph`: a passthrough expression is always peeled, a
    /// non-passthrough one only under `merge_expression_into_join`, which merges it into the join
    /// and flattens the child join underneath.
    const auto * effective = node;
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(effective->step.get());
        expression_step && effective->children.size() == 1 && !expression_step->getExpression().hasArrayJoin()
        && (isPassthroughActions(expression_step->getExpression())
            || canMergeExpressionIntoJoinGraph(expression_step->getExpression(), merge_expression_into_join)))
    {
        effective = effective->children[0];
    }

    if (const auto * child_join_step = typeid_cast<const JoinStepLogical *>(effective->step.get());
        child_join_step && !child_join_step->isOptimized())
    {
        const auto child_join_kind = child_join_step->getJoinOperator().kind;
        const auto child_strictness = child_join_step->getJoinOperator().strictness;
        /// Keep in sync with `addChildQueryGraph`: normally only All strictness flattens; with CD-A
        /// semi/anti reordering, Semi/Anti children flatten too.
        const bool allow_child_strictness = child_strictness == JoinStrictness::All
            || (allow_semi_anti_children
                && (child_strictness == JoinStrictness::Semi || child_strictness == JoinStrictness::Anti));
        const bool allow_child_join_kind
            = (isInnerOrCross(child_join_kind) || isLeft(child_join_kind) || isRight(child_join_kind))
            && allow_child_strictness
            && child_join_step->typeChangingSides().empty();

        if (child_join_step->getJoinSettings() == join_settings && join_steps_limit > 1 && allow_child_join_kind)
        {
            collectJoinGraphRelationHeadersForJoin(*effective, join_steps_limit, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children);
            return;
        }
    }

    relation_headers.push_back(node->step->getOutputHeader());
}

/// The join order optimizer reconstructs join steps using `JoinExpressionActions`, which requires
/// column names to be unique across the two sides of every reconstructed join. When two relations in
/// the join graph share a column name, reordering can place them on opposite sides of a reconstructed
/// join and hit a `LOGICAL_ERROR`. This can happen, for example, when parallel replicas or distributed
/// query rewriting reuses table identifiers (e.g. both relations produce `__table1`), or when scalar
/// subquery results collide with join table aliases. The unoptimized join handles such names
/// correctly, so detect this up front and skip the reordering in that case.
static bool joinGraphHasOverlappingColumnNames(
    const QueryPlan::Node & join_node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    bool allow_semi_anti_children)
{
    std::vector<SharedHeader> relation_headers;
    collectJoinGraphRelationHeadersForJoin(join_node, join_steps_limit, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children);

    std::unordered_set<std::string_view> seen_names;
    for (const auto & header : relation_headers)
    {
        if (!header)
            continue;
        for (const auto & column : *header)
        {
            if (!seen_names.insert(column.name).second)
                return true;
        }
    }
    return false;
}

void optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || join_step->isOptimized())
        return;

    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);
    optimizeJoinLogicalImpl(join_step, node, nodes, optimization_settings);
}

void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    for (auto * child : node.children)
    {
        if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
            lookup_step->optimize(optimization_settings);
    }

    const auto & join_operator = join_step->getJoinOperator();
    auto strictness = join_operator.strictness;
    auto kind = join_operator.kind;
    auto locality = join_operator.locality;
    if (!optimization_settings.query_plan_optimize_join_order_limit
        || (strictness != JoinStrictness::All && !isSwapOnlyJoinStrictness(strictness))
        || locality != JoinLocality::Unspecified
        || kind == JoinKind::Paste
        || !join_operator.residual_filter.empty()
    )
    {
        join_step->setOptimized();
        return;
    }

    /// When CD-A semi/anti reordering is enabled, Semi/Anti joins are fully reorderable rather than
    /// swap-only, so we keep the full graph size limit for them. Full joins (swap-only *kind*) and
    /// the Any strictness stay capped -- CD-A does not model those for reordering.
    const bool cda_reorder_semi_anti = conflictDetectorReordersSemiAnti(optimization_settings)
        && (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti);

    int query_graph_size_limit = safe_cast<int>(optimization_settings.query_plan_optimize_join_order_limit);
    if ((isSwapOnlyJoinStrictness(strictness) || isSwapOnlyJoinKind(kind)) && query_graph_size_limit > 2 && !cda_reorder_semi_anti)
        /// Do not reorder joins, only allow swap
        query_graph_size_limit = 2;

    /// Skip join order optimization when the join graph contains relations with overlapping column
    /// names, which the `JoinExpressionActions`-based reconstruction does not support. See the comment
    /// on `joinGraphHasOverlappingColumnNames`. This generalizes a check over the immediate children to
    /// the whole flattened relation set, so it also covers overlaps that only appear after flattening.
    if (joinGraphHasOverlappingColumnNames(
            node, query_graph_size_limit, join_step->getJoinSettings(),
            optimization_settings.merge_expression_into_join, conflictDetectorReordersSemiAnti(optimization_settings)))
    {
        join_step->setOptimized();
        return;
    }

    QueryGraphBuilder query_graph_builder(optimization_settings, node, join_step->getJoinSettings(), join_step->getSortingSettings());
    query_graph_builder.context->stats_hint = join_step->getTableStatsHint();

    buildQueryGraph(query_graph_builder, node, nodes, query_graph_size_limit);
    node = chooseJoinOrder(std::move(query_graph_builder), nodes, strictness);
}

}

}
