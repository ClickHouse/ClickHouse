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
    /// `ITransformingStep` and `IQueryPlanStep`:
    ///  - on the wire via `serialize`: `grouping_sets_params` (only `used_keys` per set;
    ///    `missing_keys` is recomputed at deserialize from `keys` minus `used_keys`, so it is
    ///    derived, not extra payload), `final`, `group_by_sort_description` (always serialized,
    ///    unconditionally), `should_produce_results_in_order_of_bucket_number`,
    ///    `memory_bound_merging_of_aggregation_results_enabled`.
    ///  - on the wire via `serializeSettings`: `max_block_size`,
    ///    `memory_bound_merging_max_block_bytes`, `memory_efficient_aggregation`.
    ///  - `params` (`Aggregator::Params`) - on the wire as a whole via `serialize` (`keys`,
    ///    `aggregates`, `overflow_row`, `stats_collecting_params`) and `serializeSettings`
    ///    (`min_hit_rate_to_use_consecutive_keys_optimization`, `stats_collecting_params`'s
    ///    thresholds, `serialize_string_with_zero_byte`, `enable_packed_string_keys`), *except*
    ///    `params.max_threads`, which `deserialize` always re-derives from the session's
    ///    `max_threads` setting regardless of this instance's value - see the step's own
    ///    `max_threads` below, which mirrors it before `transformPipeline` runs and carries the
    ///    extra. `params.only_merge` is not a free field of this step: every construction site
    ///    (the merge-only `Aggregator::Params` constructor used by `Planner.cpp` and by
    ///    `deserialize`, and `TwoStageAggregationTransformation` which sets it explicitly) forces
    ///    it to `true`. The remaining `Params` fields (raw-row grouping thresholds, external
    ///    aggregation spilling, compiled-expression settings, adaptive aggregation, `top_k`) are
    ///    read only by the execute path (building hash tables from raw rows), unreachable here
    ///    because `only_merge` is always true; `bucket_top_k` is, by its own comment in
    ///    `Aggregator.h`, deliberately kept out of plan serialization because losing it is "the
    ///    safe direction" - the same reasoning excludes it from this identity.
    ///  - extras: `max_threads` - not on the wire (see above); controls how many streams
    ///    `transformPipeline` resizes the pipeline to, i.e. the parallelism of the physical plan.
    ///    `memory_efficient_merge_threads` - not on the wire (re-derived from a session setting at
    ///    deserialize the same way); controls the thread count of the memory-efficient merge
    ///    branch of `transformPipeline`.
    ///  - derived: `input_headers` - any input column not needed by `keys`/`aggregates` is silently
    ///    dropped by `Aggregator::Params::getHeader` and columns are matched by name at runtime
    ///    (`pipeline.getSharedHeader()`, not this recorded header), so neither extra columns nor
    ///    input column order constrain execution. `transform_traits` and `data_stream_traits` -
    ///    computed from `getTraits` at construction and never mutated. `collect_processors` -
    ///    always default for this step.
    ///  - display or runtime instrumentation only: `step_description`, `step_index`, `processors`,
    ///    `dataflow_cache_updater`.
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
