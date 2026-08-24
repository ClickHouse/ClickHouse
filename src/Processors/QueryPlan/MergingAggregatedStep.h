#pragma once
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// This step finishes aggregation. See AggregatingSortedTransform.
class MergingAggregatedStep : public ITransformingStep
{
public:
    MergingAggregatedStep(
        const SharedHeader & input_header_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        bool memory_efficient_aggregation_,
        size_t memory_efficient_merge_threads_,
        bool should_produce_results_in_order_of_bucket_number_,
        size_t max_block_size_,
        size_t memory_bound_merging_max_block_bytes_,
        bool memory_bound_merging_of_aggregation_results_enabled_);

    String getName() const override { return "MergingAggregated"; }
    const Aggregator::Params & getParams() const { return params; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void applyOrder(SortDescription input_sort_description);
    const SortDescription & getSortDescription() const override;
    const SortDescription & getGroupBySortDescription() const { return group_by_sort_description; }

    bool memoryBoundMergingWillBeUsed() const;

    bool isGroupingSets() const { return !grouping_sets_params.empty(); }
    const auto & getGroupingSetsParamsList() const { return grouping_sets_params; }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `MergingAggregatedStep`,
    /// `ITransformingStep` and `IQueryPlanStep`. Reachability below is checked against the actual
    /// transforms `transformPipeline` builds - `MergingAggregatedTransform` (plain merge, via
    /// `Aggregator::mergeBlocks(BucketToChunks&, ...)` + `convertToChunks`),
    /// `FinishAggregatingInOrderTransform` / `MergingAggregatedBucketTransform` (memory-bound
    /// merge), `MergingAggregatedMemoryEfficientTransform` (memory-efficient merge, via
    /// `Aggregator::mergeBlocks(AggregatedChunks&, ...)` + `convertBlockToTwoLevel`) - not against
    /// `Aggregator::executeOnBlock`/`AggregatingTransform`, which this step never instantiates.
    ///
    /// Own fields:
    ///  - `grouping_sets_params` - on the wire (`serialize` writes each set's `used_keys`;
    ///    `missing_keys` is recomputed at deserialize from `keys` minus `used_keys`, so it is
    ///    derived, not extra payload).
    ///  - `final` - on the wire (`serialize`, flags bit 1). Also gates whether `bucket_top_k` is
    ///    read (see below).
    ///  - `group_by_sort_description` - on the wire (`serialize`, unconditionally).
    ///  - `should_produce_results_in_order_of_bucket_number` - on the wire (`serialize`, flags bit 16).
    ///  - `memory_bound_merging_of_aggregation_results_enabled` - on the wire (`serialize`, flags bit 32).
    ///  - `max_block_size` - on the wire (`serializeSettings`). Distinct from `params.max_block_size`,
    ///    see below.
    ///  - `memory_bound_merging_max_block_bytes` - on the wire (`serializeSettings`).
    ///  - `memory_efficient_aggregation` - on the wire (`serializeSettings`).
    ///  - `max_threads` - **extras**. Not on the wire: `deserialize` re-derives it from the
    ///    session's `max_threads` setting regardless of this instance's value. Controls how many
    ///    streams `transformPipeline` resizes the pipeline to (parallelism of the physical plan);
    ///    mirrors `params.max_threads` until `transformPipeline` re-resolves both.
    ///  - `memory_efficient_merge_threads` - **extras**. Not on the wire (re-derived from a session
    ///    setting at deserialize the same way). Controls the thread count of the memory-efficient
    ///    merge branch of `transformPipeline`.
    ///
    /// `params` (`Aggregator::Params`), field by field:
    ///  - `keys`, `aggregates` - on the wire (`serialize`).
    ///  - `keys_size`, `aggregates_size` - derived, excluded: always equal `keys.size()`/`aggregates.size()`.
    ///  - `overflow_row` - on the wire (`serialize`, flags bit 2).
    ///  - `max_rows_to_group_by`, `group_by_overflow_mode` - **extras**. Not on the wire.
    ///    `Aggregator::checkLimits` reads both, and is reached from `mergeBlocks(BucketToChunks&, ...)`
    ///    (`MergingAggregatedTransform::generate`'s plain merge) for chunks with no known bucket
    ///    (`bucket_to_chunks[-1]`, i.e. single-level input): it can throw `TOO_MANY_ROWS`, drop
    ///    remaining rows (`BREAK`), or route further keys to the overflow row (`ANY`) - a
    ///    correctness-affecting divergence, not merely a performance one.
    ///  - `group_by_two_level_threshold`, `group_by_two_level_threshold_bytes` - excluded: read only
    ///    by `Aggregator::initDataVariantsWithSizeHint`/`executeOnBlock`/`executeOnBlockSmall`
    ///    (execute path) and by `AggregatingTransform`'s own per-thread parallel-partition merge
    ///    (`worthParallelPartitionMergeSingleLevel`/`mergeSingleLevelPartitionImpl`), never by the
    ///    transforms this step builds.
    ///  - `max_bytes_before_external_group_by`, `min_free_disk_space`, `tmp_data_scope` - excluded:
    ///    `writeToTemporaryFile` is only called from `executeAndMergeColumns` and `mergeOnBlock`,
    ///    both execute-path/`AggregatingTransform`-only; neither `mergeBlocks` overload this step
    ///    uses calls it.
    ///  - `empty_result_for_aggregation_by_empty_set` - excluded: read only in
    ///    `AggregatingTransform::consume`/`initGenerate`.
    ///  - `compile_aggregate_expressions`, `min_count_to_compile_aggregate_expression` - excluded:
    ///    `compiled_aggregate_functions_holder` is consulted only by the execute path and by
    ///    `AggregatingTransform`'s per-thread merge (`mergeSingleLevelDataImplFixedMap`); the merge
    ///    path this step uses (`mergeStreamsImplCase`) always calls `aggregate_functions[j]->mergeBatch`
    ///    directly.
    ///  - `max_threads` - **extras**, folded into the step's own `max_threads` tag above: equal to
    ///    it at construction and re-resolved together in `transformPipeline`, so one tag covers both.
    ///  - `enable_prefetch` - excluded: every read site (`executeOnBlock`-family,
    ///    `mergeSingleLevelDataImpl`, `mergeSingleLevelPartitionImpl`) belongs to the execute path
    ///    or to `AggregatingTransform`'s per-thread merge, not to this step's transforms.
    ///  - `optimize_group_by_constant_keys` - excluded: read only in `executeOnBlock`.
    ///  - `max_block_size` (`params.max_block_size`) - **extras**. `convertToChunks` calls
    ///    `prepareChunkAndFillSingleLevel<false>` (`return_single_block = false`), so this value
    ///    (not the step's own, wire-covered `max_block_size` above - nothing enforces the two stay
    ///    equal) controls how the single-level merge result is split into chunks.
    ///  - `only_merge` - excluded, not a free field of this step: every construction site (the
    ///    merge-only `Aggregator::Params` constructor used by `Planner.cpp` and by `deserialize`,
    ///    and `TwoStageAggregationTransformation` which sets it explicitly) forces it to `true`.
    ///  - `min_hit_rate_to_use_consecutive_keys_optimization` - on the wire (`serializeSettings`).
    ///  - `stats_collecting_params` - on the wire (`serialize`'s enabled flag + key;
    ///    `serializeSettings` for the thresholds).
    ///  - `enable_adaptive_aggregator`, `adaptive_aggregator_freeze_threshold` - excluded: read only
    ///    in `executeImplUntilAdaptiveFreeze`/`executeOnBlock` (adaptive learning over raw rows).
    ///  - `bucket_top_k`, `bucket_top_k_ascending`, `bucket_top_k_count_index` - **extras**.
    ///    `Aggregator::convertOneBucketToChunk` reads them unconditionally on `final` (already on
    ///    the wire) regardless of `only_merge`, and is reached from
    ///    `MergingAggregatedTransform::generate` via `convertToChunks` -
    ///    `prepareChunksAndFillTwoLevel(Impl)`. The wire-serialization argument ("a deserialized
    ///    plan re-runs without the optimization, which is the safe direction") does not transfer to
    ///    identity: two merge steps differing only here produce different output row counts.
    ///  - `enable_producing_buckets_out_of_order_in_aggregation` - excluded: read only in
    ///    `AggregatingTransform` (also serialized separately by `AggregatingStep`, never by this step).
    ///  - `enable_parallel_single_level_merge` - excluded: read only in
    ///    `AggregatingTransform::worthParallelPartitionMergeSingleLevel` (also serialized separately
    ///    by `AggregatingStep`, never by this step).
    ///  - `serialize_string_with_zero_byte` - on the wire (`serializeSettings`).
    ///  - `top_k` (`group_by_top_k_optimization`) - excluded: every read site is within
    ///    `executeOnBlock`/`executeOnBlockSmall`/`executeImplUntilAdaptiveFreeze` (execute path).
    ///  - `enable_packed_string_keys` - on the wire (`serializeSettings`, with version-dependent
    ///    narrowing logic in `serializeSettings` itself).
    ///  - `aggregation_in_order` - excluded: only sets `method_chosen_for_in_order`, which is read
    ///    only by `executeOnBlockSmall` (`AggregatingInOrderTransform`'s per-run state
    ///    construction); "the whole-block paths (including `mergeBlocks`) keep `method_chosen`"
    ///    per the field's own comment in `Aggregator.h`.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: any input column not needed by `keys`/`aggregates`
    ///    is silently dropped by `Aggregator::Params::getHeader`, and columns are matched by name
    ///    at runtime (`pipeline.getSharedHeader()`, not this recorded header), so neither extra
    ///    columns nor input column order constrain execution.
    ///  - `transform_traits`, `data_stream_traits` - derived, excluded: computed by `getTraits` at
    ///    construction, never mutated.
    ///  - `collect_processors` - derived, excluded: always default for this step.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    bool supportsCascadesIdentity() const override { return isSerializable(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    Aggregator::Params params;
    GroupingSetsParamsList grouping_sets_params;
    bool final;
    const bool memory_efficient_aggregation;
    size_t max_threads;
    size_t memory_efficient_merge_threads;
    const size_t max_block_size;
    const size_t memory_bound_merging_max_block_bytes;
    SortDescription group_by_sort_description;

    /// These settings are used to determine if we should resize pipeline to 1 at the end.
    const bool should_produce_results_in_order_of_bucket_number;
    const bool memory_bound_merging_of_aggregation_results_enabled;
};

}
