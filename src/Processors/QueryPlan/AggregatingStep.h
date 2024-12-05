#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

Block appendGroupingSetColumn(Block header);
Block generateOutputHeader(const Block & input_header, const Names & keys, bool use_nulls);

class AggregatingProjectionStep;

/// Aggregation. See AggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:
    AggregatingStep(
        const Header & input_header_,
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

    static Block appendGroupingColumn(Block block, const Names & keys, bool has_grouping, bool use_nulls);

    String getName() const override { return "Aggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }

    const auto & getGroupingSetsParamsList() const { return grouping_sets_params; }
    bool isGroupByUseNulls() const { return group_by_use_nulls; }

    bool inOrder() const { return !sort_description_for_merging.empty(); }
    bool explicitSortingRequired() const { return explicit_sorting_required_for_aggregation_in_order; }
    bool isGroupingSets() const { return !grouping_sets_params.empty(); }
    void applyOrder(SortDescription sort_description_for_merging_, SortDescription group_by_sort_description_);
    bool memoryBoundMergingWillBeUsed() const;
    void skipMerging() { skip_merging = true; }

    const SortDescription & getSortDescription() const override;

    bool canUseProjection() const;
    /// When we apply aggregate projection (which is full), this step will only merge data.
    /// Argument input_stream replaces current single input.
    /// Probably we should replace this step to MergingAggregated later? (now, aggregation-in-order will not work)
    void requestOnlyMergeForAggregateProjection(const Header & input_header);
    /// When we apply aggregate projection (which is partial), this step should be replaced to AggregatingProjection.
    /// Argument input_stream would be the second input (from projection).
    std::unique_ptr<AggregatingProjectionStep> convertToAggregatingProjection(const Header & input_header) const;

    static ActionsDAG makeCreatingMissingKeysForGroupingSetDAG(
        const Block & in_header,
        const Block & out_header,
        const GroupingSetsParamsList & grouping_sets_params,
        UInt64 group,
        bool group_by_use_nulls);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    void enableMemoryBoundMerging() { memory_bound_merging_of_aggregation_results_enabled = true; }

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
    const bool should_produce_results_in_order_of_bucket_number;
    bool memory_bound_merging_of_aggregation_results_enabled;
    bool explicit_sorting_required_for_aggregation_in_order;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors aggregating;
};

class AggregatingProjectionStep : public IQueryPlanStep
{
public:
    AggregatingProjectionStep(
        Blocks input_headers_,
        Aggregator::Params params_,
        bool final_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_
    );

    String getName() const override { return "AggregatingProjection"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputHeader() override;

    Aggregator::Params params;
    bool final;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    Processors aggregating;
};

}
