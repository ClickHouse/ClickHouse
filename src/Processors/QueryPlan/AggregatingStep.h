#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

Block appendGroupingSetColumn(Block header);
Block generateOutputHeader(const Block & input_header, const Names & keys, bool use_nulls);

/// Whether an aggregation over `keys` - or, when `grouping_sets_params` is not empty, over any of its grouping sets -
/// can dispatch to the single-`String` method, i.e. whether `enable_packed_string_keys_in_aggregation` can affect it
/// at all. See `AggregatedDataVariants::chooseMethod` and `Aggregator::Params::enable_packed_string_keys`.
/// Returns `true` when a key type cannot be resolved from `header`, so that a caller which uses this to decide whether
/// the choice has to be communicated to a remote peer errs on the side of communicating it.
bool aggregationCanUsePackedStringKeys(const Block & header, const Names & keys, const GroupingSetsParamsList & grouping_sets_params);

/// Whether `dag` forwards the column `name` unchanged (possibly through aliases). Guards the GROUP BY top-K
/// optimization: the heap ranks the aggregation keys, so every expression between the aggregation and the sort
/// must hand the sorted key through untouched. If such an expression computed a new value and published it under
/// the key's name, the sort would order by something the heap never ranked and pruning could drop real winners.
bool isSortKeyPassThrough(const ActionsDAG & dag, const String & name);

class AggregatingProjectionStep;

/// Aggregation. See AggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:

    enum class AggregatingStage : size_t
    {
        PartialAggregation = 0,
        FinalAggregation = 1,
        Scatter = 2,
        AggregatingSharded = 3,
    };

    AggregatingStep(
        const SharedHeader & input_header_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        size_t max_block_size_,
        size_t aggregation_in_order_max_block_bytes_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        bool group_by_use_nulls_,
        SortDescription sort_description_for_merging_,
        SortDescription group_by_sort_description_,
        bool should_produce_results_in_order_of_bucket_number_,
        bool memory_bound_merging_of_aggregation_results_enabled_,
        bool explicit_sorting_required_for_aggregation_in_order_,
        bool enable_sharding_aggregator_);

    static Block appendGroupingColumn(const Block & block, const Names & keys, bool has_grouping, bool use_nulls);

    String getName() const override { return "Aggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    std::vector<size_t> getStepGroups() const override;
    String getStepGroupName(size_t group) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }
    bool isFinal() const { return final; }

    /// See `Aggregator::Params::bucket_top_k`; called by the plan optimization.
    void enableBucketTopK(size_t n, bool ascending, size_t count_index)
    {
        params.bucket_top_k = n;
        params.bucket_top_k_ascending = ascending;
        params.bucket_top_k_count_index = count_index;
    }

    const auto & getGroupingSetsParamsList() const { return grouping_sets_params; }
    bool isGroupByUseNulls() const { return group_by_use_nulls; }

    bool inOrder() const { return !sort_description_for_merging.empty(); }
    bool explicitSortingRequired() const { return explicit_sorting_required_for_aggregation_in_order; }
    bool isGroupingSets() const { return !grouping_sets_params.empty(); }
    void applyOrder(SortDescription sort_description_for_merging_, SortDescription group_by_sort_description_);
    void applyTopKOptimization(Aggregator::Params::TopKParams top_k);
    bool memoryBoundMergingWillBeUsed() const;
    void skipMerging() { skip_merging = true; }
    void setLimitHint(size_t limit) { limit_hint = limit; }
    size_t getLimitHint() const { return limit_hint; }
    const SortDescription & getGroupBySortDescription() const { return group_by_sort_description; }

    const SortDescription & getSortDescription() const override;

    bool canUseProjection() const;
    bool canUseShardedAggregation(const QueryPipelineBuilder & pipeline) const;
    /// Returns nullptr when the adaptive aggregator can engage, and otherwise a short reason
    /// for the trace log.
    const char * adaptiveAggregatorRejectionReason(const QueryPipelineBuilder & pipeline) const;
    /// When we apply aggregate projection (which is full), this step will only merge data.
    /// Argument input_stream replaces current single input.
    /// Probably we should replace this step to MergingAggregated later? (now, aggregation-in-order will not work)
    void requestOnlyMergeForAggregateProjection(const SharedHeader & input_header);
    /// When we apply aggregate projection (which is partial), this step should be replaced to AggregatingProjection.
    /// Argument input_stream would be the second input (from projection).
    std::unique_ptr<AggregatingProjectionStep> convertToAggregatingProjection(const SharedHeader & input_header) const;

    static ActionsDAG makeCreatingMissingKeysForGroupingSetDAG(
        const Block & in_header,
        const Block & out_header,
        const GroupingSetsParamsList & grouping_sets_params,
        UInt64 group,
        bool group_by_use_nulls);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override
    {
        return sort_description_for_merging.empty() && !explicit_sorting_required_for_aggregation_in_order;
    }

    /// Cascades cross-group identity. Field audit of every member of `AggregatingStep`,
    /// `ITransformingStep` and `IQueryPlanStep`. The identity encoding always serializes with
    /// `for_cache_key = false`, so `final` and the hash-table statistics key ARE on the wire and are
    /// not re-added below. `supportsCascadesIdentity()` implies `isSerializable()`, i.e.
    /// `sort_description_for_merging` is empty and `explicit_sorting_required_for_aggregation_in_order`
    /// is false, so `transformPipeline` cannot take the aggregation-in-order branch.
    ///
    /// Own fields:
    ///  - `params` - see below.
    ///  - `grouping_sets_params` - on the wire (`serialize` writes each set's `used_keys`;
    ///    `missing_keys` is recomputed at deserialize from `keys` minus `used_keys`).
    ///  - `final` - on the wire (`serialize`, flags bit 1, because `for_cache_key` is false).
    ///  - `max_block_size`, `aggregation_in_order_max_block_bytes` - on the wire (`serializeSettings`).
    ///  - `merge_threads`, `temporary_data_merge_threads` - **extras**. Not on the wire: `deserialize`
    ///    passes 0 for both and `updateThreadsValues` re-derives them from session settings.
    ///    `transformPipeline` resizes the merge stage to `merge_threads` and hands both to
    ///    `AggregatingTransform`, so they are the parallelism of the physical plan; the Cascades
    ///    `AggregationImplementations` rule also copies them into the `MergingAggregatedStep` it
    ///    builds.
    ///  - `skip_merging` - **extras**. Not on the wire. It makes `transformPipeline` finalize each
    ///    stream on its own instead of merging them (and `adaptiveAggregatorRejectionReason` rejects
    ///    the adaptive path for it), which is only correct for input streams with disjoint keys.
    ///  - `storage_has_evenly_distributed_read` - **extras**. Not on the wire (`deserialize` passes
    ///    `false`). It suppresses the `resize` before the aggregation, so it changes the stream layout.
    ///  - `group_by_use_nulls` - on the wire (`serialize`, flags bit 4).
    ///  - `sort_description_for_merging` - covered by the predicate: `isSerializable()` requires it to
    ///    be empty (and `serialize` writes flags bit 32 from it).
    ///  - `group_by_sort_description` - **extras**. `serialize` writes it only together with a
    ///    non-empty `sort_description_for_merging`, so for a serializable instance it is not on the
    ///    wire. Every current reader (`getSortDescription`, the in-order transforms,
    ///    `optimizeLimitForAggregationInOrder`) is gated on the step being in-order, but nothing ties
    ///    the field to that: the constructors and `applyOrder` set the two descriptions
    ///    independently, so it is encoded rather than argued away.
    ///  - `should_produce_results_in_order_of_bucket_number` - on the wire (`serializeSettings`).
    ///  - `memory_bound_merging_of_aggregation_results_enabled` - on the wire (`serializeSettings`).
    ///  - `explicit_sorting_required_for_aggregation_in_order` - covered by the predicate:
    ///    `isSerializable()` requires it to be false (and `serialize` writes flags bit 64 from it).
    ///  - `enable_sharding_aggregator` - **extras**. Not on the wire (`deserialize` passes `false`).
    ///    `canUseShardedAggregation` returns false without it, so it selects the shard-by-hash
    ///    aggregation pipeline.
    ///  - `limit_hint` - **extras**. Not on the wire. Like `group_by_sort_description` its readers
    ///    (`AggregatingInOrderTransform`, `FinishAggregatingInOrderTransform`,
    ///    `optimizeLimitForAggregationInOrder`) sit on the in-order path, and it is a free field that
    ///    `setLimitHint` writes independently; it truncates the result where it is read, so it is
    ///    encoded rather than argued away.
    ///  - `aggregating_in_order`, `aggregating_sorted`, `finalizing`, `scatter`, `aggregating` -
    ///    runtime instrumentation for `describePipeline`, excluded.
    ///
    /// `params` (`Aggregator::Params`), field by field:
    ///  - `keys`, `aggregates` - on the wire (`serialize`).
    ///  - `keys_size`, `aggregates_size` - derived, excluded: always equal `keys.size()` /
    ///    `aggregates.size()`.
    ///  - `overflow_row` - on the wire (`serialize`, flags bit 2).
    ///  - `max_rows_to_group_by`, `group_by_overflow_mode`, `group_by_two_level_threshold`,
    ///    `group_by_two_level_threshold_bytes`, `max_bytes_before_external_group_by`,
    ///    `empty_result_for_aggregation_by_empty_set`, `min_free_disk_space`,
    ///    `compile_aggregate_expressions`, `min_count_to_compile_aggregate_expression`,
    ///    `enable_prefetch`, `optimize_group_by_constant_keys`,
    ///    `min_hit_rate_to_use_consecutive_keys_optimization`,
    ///    `enable_producing_buckets_out_of_order_in_aggregation`, `enable_parallel_single_level_merge`,
    ///    `serialize_string_with_zero_byte` - on the wire (`serializeSettings`). Unlike in
    ///    `MergingAggregatedStep`, this step runs `Aggregator::executeOnBlock`, so all of them are also
    ///    reachable - but they need no extra tag.
    ///  - `enable_adaptive_aggregator`, `adaptive_aggregator_freeze_threshold` - on the wire: their
    ///    `serializeSettings` branch requires version >= 7 and the identity encoding always uses
    ///    `DBMS_QUERY_PLAN_SERIALIZATION_VERSION` (9).
    ///  - `enable_packed_string_keys` - on the wire: at version >= 5 (always, see above)
    ///    `serializeSettings` writes the name exactly when the value is `false`, so present-vs-absent
    ///    distinguishes the two values.
    ///  - `stats_collecting_params` - on the wire (`serialize`'s flags bit 16 plus the key, since
    ///    `for_cache_key` is false; `serializeSettings` for the thresholds).
    ///  - `tmp_data_scope` - excluded: a runtime resource, taken from the global context at
    ///    deserialize; it is not a property of the plan.
    ///  - `max_threads` (`params.max_threads`) - **extras**. Not on the wire (`deserialize` passes 0,
    ///    `updateThreadsValues` re-resolves it from the session). `transformPipeline` resizes to it,
    ///    derives the shard count from it and `adaptiveAggregatorRejectionReason` tests it.
    ///  - `max_block_size` (`params.max_block_size`) - **extras**. `serializeSettings` writes only the
    ///    step's own `max_block_size`, and `deserialize` seeds both from that one value; nothing
    ///    enforces that the two stay equal, and `params.max_block_size` is what splits the
    ///    aggregation result into chunks.
    ///  - `only_merge` - **extras**. Not on the wire (`deserialize` passes `false`).
    ///    `requestOnlyMergeForAggregateProjection` sets it when an aggregate projection makes this step
    ///    a pure merge; it changes both the step's header (`Params::getHeader`) and which `Aggregator`
    ///    path runs, and `adaptiveAggregatorRejectionReason` tests it.
    ///  - `bucket_top_k`, `bucket_top_k_ascending`, `bucket_top_k_count_index` - **extras**.
    ///    Deliberately kept out of the plan serialization, but `Aggregator::convertOneBucketToChunk`
    ///    reads them on `final`, so two aggregations differing only here produce different row counts.
    ///  - `top_k` - **extras**. Not on the wire; set by `applyTopKOptimization`.
    ///    `Aggregator::executeOnBlock` / `executeOnBlockSmall` / `executeImplUntilAdaptiveFreeze` rank
    ///    the keys in a heap of size `k` and drop the rest, so it changes the result.
    ///  - `aggregation_in_order` - excluded: it only picks `method_chosen_for_in_order`, read solely by
    ///    `executeOnBlockSmall` (`AggregatingInOrderTransform`), which a serializable instance never
    ///    reaches; `transformPipeline` sets it itself on the in-order branch.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: `Params::getHeader` drops every input column that is
    ///    not a key or an aggregate argument, and the transforms resolve columns by name from the live
    ///    pipeline header, not from this recorded one. (`serializeSettings` reads the input header to
    ///    decide whether the packed-string-keys name must go on the wire, which only makes the wire
    ///    bytes more specific, never less.)
    ///  - `transform_traits`, `data_stream_traits` - derived, excluded: computed by `getTraits` at
    ///    construction from fields that are on the wire.
    ///  - `collect_processors` - derived, excluded: always default for this step.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    ///
    /// `serialize` throws only for the in-order flags below query plan serialization version 2, and
    /// the identity encoding always uses version 9. `hasCorrelatedExpressions()` is `false` by
    /// construction (the step holds no `ActionsDAG`), so no extra guard is needed.
    bool supportsCascadesIdentity() const override { return isSerializable(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void enableMemoryBoundMerging() { memory_bound_merging_of_aggregation_results_enabled = true; }

    /// AggregatingStep does not contain any ActionDAGs.
    /// All the expressions used in the AggregatingStep must be evaluated before that.
    bool hasCorrelatedExpressions() const override { return false; }

    Aggregator::Params getAggregatorParameters() const { return params; }
    /// Set during query-plan optimization (see setAggregationHashTableCacheKeys). A non-zero key
    /// enables hash-table-size preallocation; StatsCollectingParams treats key == 0 as disabled.
    void setStatsCacheKey(UInt64 stats_cache_key) { params.stats_collecting_params.setKey(stats_cache_key); }
    bool getFinal() const noexcept { return final; }
    void setFinal(bool new_value);
    void setProduceResultsInBucketOrder(bool new_value) { should_produce_results_in_order_of_bucket_number = new_value; }
    size_t getMaxBlockSize() const noexcept { return max_block_size; }
    size_t getMaxBlockSizeForAggregationInOrder() const noexcept { return aggregation_in_order_max_block_bytes; }
    size_t getMergeThreads() const noexcept { return merge_threads; }
    size_t getTemporaryDataMergeThreads() const noexcept { return temporary_data_merge_threads; }
    bool shouldProduceResultsInBucketOrder() const noexcept { return should_produce_results_in_order_of_bucket_number; }
    void setShouldProduceResultsInBucketOrder(bool new_value) { should_produce_results_in_order_of_bucket_number = new_value; }
    bool usingMemoryBoundMerging() const noexcept { return memory_bound_merging_of_aggregation_results_enabled; }

    bool supportsDataflowStatisticsCollection() const override
    {
        return grouping_sets_params.empty();
    }

private:
    void updateOutputHeader() override;

    Aggregator::Params params;
    GroupingSetsParamsList grouping_sets_params;
    bool final;
    size_t max_block_size;
    size_t aggregation_in_order_max_block_bytes;
    size_t merge_threads;
    size_t temporary_data_merge_threads;
    bool skip_merging = false; // if we aggregate partitioned data merging is not needed

    bool storage_has_evenly_distributed_read;
    bool group_by_use_nulls;

    /// Both sort descriptions are needed for aggregate-in-order optimization.
    /// Both sort descriptions are subset of GROUP BY key columns (or monotonic functions over it).
    /// Sort description for merging is a sort description for input and a prefix of group_by_sort_description.
    /// group_by_sort_description contains all GROUP BY keys and is used for final merging of aggregated data.
    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;

    /// These settings are used to determine if we should resize pipeline to 1 at the end.
    bool should_produce_results_in_order_of_bucket_number;
    bool memory_bound_merging_of_aggregation_results_enabled;
    bool explicit_sorting_required_for_aggregation_in_order;
    bool enable_sharding_aggregator;

    size_t limit_hint = 0;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors scatter;
    Processors aggregating;
};

class AggregatingProjectionStep : public IQueryPlanStep
{
public:
    AggregatingProjectionStep(
        SharedHeaders input_headers_,
        Aggregator::Params params_,
        bool final_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_
    );

    String getName() const override { return "AggregatingProjection"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    std::vector<size_t> getStepGroups() const override;
    String getStepGroupName(size_t group) const override;

    const Aggregator::Params & getParams() const { return params; }


private:
    void updateOutputHeader() override;

    Aggregator::Params params;
    bool final;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    Processors aggregating;
};

}
