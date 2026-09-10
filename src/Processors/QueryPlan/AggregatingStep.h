#pragma once

#include <Core/Block.h>
#include <Core/Defines.h>
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

struct AggregatingWire;

/// Aggregation. See AggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:

    enum class AggregatingStage : size_t
    {
        PartialAggregation = 0,
        FinalAggregation = 1,
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
        bool explicit_sorting_required_for_aggregation_in_order_);

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

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `AggregatingStep.cpp` declares.
    AggregatingWire toWire() const;
    static QueryPlanStepPtr fromWire(AggregatingWire wire, Deserialization & ctx);

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
    /// Re-bases the aggregation onto a new input with a different key set; aggregates unchanged.
    void rebaseOntoInput(const SharedHeader & new_input_header, Names new_keys);
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
    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings, UInt64 version) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
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

    size_t limit_hint = 0;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

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

/// What `AggregatingStep` puts on the wire in the framed format. The members from `max_block_size` on
/// travel through the settings channel.
struct AggregatingWire
{
    Names keys;
    AggregateDescriptions aggregates;
    /// The used keys of every grouping set; the missing keys of a set are the other keys.
    std::vector<Names> grouping_sets;
    /// Stage marker: a partial aggregation emits states, a final one emits values.
    bool final = false;
    bool overflow_row = false;
    bool group_by_use_nulls = false;
    /// Set on the merge step synthesized by the Cascades aggregation pushdown; changes how the input
    /// columns are read, so it must round-trip and take part in the cache key.
    bool only_merge = false;
    /// Both non-empty for aggregation in order.
    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;
    bool explicit_sorting_required_for_aggregation_in_order = false;
    /// The key of the hash table statistics; 0 when collection is off.
    UInt64 hash_table_stats_key = 0;

    UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
    UInt64 aggregation_in_order_max_block_bytes = 50000000;
    bool aggregation_sort_result_by_bucket_number = true;
    bool aggregation_in_order_memory_bound_merging = true;
    UInt64 max_rows_to_group_by = 0;
    OverflowMode group_by_overflow_mode = OverflowMode::THROW;
    UInt64 group_by_two_level_threshold = 100000;
    UInt64 group_by_two_level_threshold_bytes = 50000000;
    UInt64 max_bytes_before_external_group_by = 0;
    bool empty_result_for_aggregation_by_empty_set = false;
    UInt64 min_free_disk_space_for_temporary_data = 0;
    bool compile_aggregate_expressions = true;
    UInt64 min_count_to_compile_aggregate_expression = 3;
    bool enable_software_prefetch_in_aggregation = true;
    bool optimize_group_by_constant_keys = true;
    Float32 min_hit_rate_to_use_consecutive_keys_optimization = 0.5;
    bool collect_hash_table_stats_during_aggregation = true;
    UInt64 max_entries_for_hash_table_stats = 10000;
    UInt64 max_size_to_preallocate_for_aggregation = 100000000;
    bool enable_producing_buckets_out_of_order_in_aggregation = true;
    bool enable_parallel_single_level_merge = false;
    bool enable_adaptive_aggregator = false;
    UInt64 adaptive_aggregator_freeze_threshold = 0;
    UInt64 adaptive_aggregator_freeze_threshold_bytes = 0;
    bool serialize_string_in_memory_with_zero_byte = true;
    bool enable_packed_string_keys_in_aggregation = true;
};

}
