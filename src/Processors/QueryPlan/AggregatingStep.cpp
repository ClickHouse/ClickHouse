#include <algorithm>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <cstddef>
#include <memory>
#include <numeric>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Core/ProtocolDefines.h>
#include <Core/SettingsEnums.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const QueryPlanSerializationSettingsBool aggregation_in_order_memory_bound_merging;
    extern const QueryPlanSerializationSettingsBool aggregation_sort_result_by_bucket_number;
    extern const QueryPlanSerializationSettingsBool collect_hash_table_stats_during_aggregation;
    extern const QueryPlanSerializationSettingsBool compile_aggregate_expressions;
    extern const QueryPlanSerializationSettingsBool empty_result_for_aggregation_by_empty_set;
    extern const QueryPlanSerializationSettingsBool enable_software_prefetch_in_aggregation;
    extern const QueryPlanSerializationSettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold_bytes;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_group_by;
    extern const QueryPlanSerializationSettingsUInt64 max_entries_for_hash_table_stats;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_to_group_by;
    extern const QueryPlanSerializationSettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const QueryPlanSerializationSettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const QueryPlanSerializationSettingsBool optimize_group_by_constant_keys;
    extern const QueryPlanSerializationSettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const QueryPlanSerializationSettingsBool enable_parallel_single_level_merge;
    extern const QueryPlanSerializationSettingsBool enable_adaptive_aggregator;
    extern const QueryPlanSerializationSettingsUInt64 adaptive_aggregator_freeze_threshold;
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
    extern const QueryPlanSerializationSettingsBool enable_packed_string_keys_in_aggregation;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

static bool memoryBoundMergingWillBeUsed(
    bool should_produce_results_in_order_of_bucket_number,
    bool memory_bound_merging_of_aggregation_results_enabled,
    SortDescription sort_description_for_merging)
{
    return should_produce_results_in_order_of_bucket_number && memory_bound_merging_of_aggregation_results_enabled && !sort_description_for_merging.empty();
}

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

static bool keysCanUsePackedStringMethod(const Block & header, const Names & keys)
{
    for (const auto & key : keys)
    {
        if (!header.has(key))
            return true;
    }

    Sizes key_sizes;
    return AggregatedDataVariants::chooseMethod(header, keys, key_sizes) == AggregatedDataVariants::Type::key_packed_string;
}

bool aggregationCanUsePackedStringKeys(const Block & header, const Names & keys, const GroupingSetsParamsList & grouping_sets_params)
{
    if (grouping_sets_params.empty())
        return keysCanUsePackedStringMethod(header, keys);

    /// Every grouping set gets its own `Aggregator` over its own subset of the keys, so the method is chosen per set.
    for (const auto & grouping_set : grouping_sets_params)
    {
        if (keysCanUsePackedStringMethod(header, grouping_set.used_keys))
            return true;
    }

    return false;
}

bool isSortKeyPassThrough(const ActionsDAG & dag, const String & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return false;

    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node->type == ActionsDAG::ActionType::INPUT && node->result_name == name;
}

Block appendGroupingSetColumn(Block header)
{
    Block res;
    res.insert({std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    for (auto & col : header)
        res.insert(std::move(col));

    return res;
}

static inline void convertToNullable(Block & header, const Names & keys)
{
    for (const auto & key : keys)
    {
        auto & column = header.getByName(key);

        column.type = makeNullableOrLowCardinalityNullableSafe(column.type);
        column.column = makeNullableOrLowCardinalityNullableSafe(column.column);
    }
}

Block generateOutputHeader(const Block & input_header, const Names & keys, bool use_nulls)
{
    auto header = appendGroupingSetColumn(input_header);
    if (use_nulls)
        convertToNullable(header, keys);
    return header;
}


Block AggregatingStep::appendGroupingColumn(const Block & block, const Names & keys, bool has_grouping, bool use_nulls)
{
    if (!has_grouping)
        return block;

    return generateOutputHeader(block, keys, use_nulls);
}

AggregatingStep::AggregatingStep(
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
    bool enable_sharding_aggregator_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(appendGroupingColumn(params_.getHeader(*input_header_, final_), params_.keys, !grouping_sets_params_.empty(), group_by_use_nulls_)),
        getTraits(should_produce_results_in_order_of_bucket_number_),
        false)
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(final_)
    , max_block_size(max_block_size_)
    , aggregation_in_order_max_block_bytes(aggregation_in_order_max_block_bytes_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_use_nulls(group_by_use_nulls_)
    , sort_description_for_merging(std::move(sort_description_for_merging_))
    , group_by_sort_description(std::move(group_by_sort_description_))
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , memory_bound_merging_of_aggregation_results_enabled(memory_bound_merging_of_aggregation_results_enabled_)
    , explicit_sorting_required_for_aggregation_in_order(explicit_sorting_required_for_aggregation_in_order_)
    , enable_sharding_aggregator(enable_sharding_aggregator_)
{
}

void AggregatingStep::applyOrder(SortDescription sort_description_for_merging_, SortDescription group_by_sort_description_)
{
    sort_description_for_merging = std::move(sort_description_for_merging_);
    group_by_sort_description = std::move(group_by_sort_description_);
    explicit_sorting_required_for_aggregation_in_order = false;
}

void AggregatingStep::applyTopKOptimization(Aggregator::Params::TopKParams top_k)
{
    params.top_k = std::move(top_k);
}

std::vector<size_t> AggregatingStep::getStepGroups() const
{
    return {
        static_cast<size_t>(AggregatingStage::PartialAggregation),
        static_cast<size_t>(AggregatingStage::FinalAggregation),
        static_cast<size_t>(AggregatingStage::Scatter),
        static_cast<size_t>(AggregatingStage::AggregatingSharded)
    };
}

String AggregatingStep::getStepGroupName(size_t group) const
{
    switch (static_cast<AggregatingStage>(group))
    {
        case AggregatingStage::PartialAggregation: return "partial aggregation";
        case AggregatingStage::FinalAggregation: return "final aggregation";
        case AggregatingStage::Scatter: return "scatter";
        case AggregatingStage::AggregatingSharded: return "shard aggregation";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown AggregatingStep group {}", group);
}


const SortDescription & AggregatingStep::getSortDescription() const
{
    if (memoryBoundMergingWillBeUsed())
        return group_by_sort_description;

    return IQueryPlanStep::getSortDescription();
}

static void updateThreadsValues(
    size_t & new_merge_threads,
    size_t & new_temporary_data_merge_threads,
    Aggregator::Params & params,
    const BuildQueryPipelineSettings & settings)
{
    /// Update values from settings if plan was deserialized.
    if (new_merge_threads == 0)
        new_merge_threads = settings.max_threads;

    if (new_temporary_data_merge_threads == 0)
        new_temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads;
    if (new_temporary_data_merge_threads == 0)
        new_temporary_data_merge_threads = new_merge_threads;

    if (params.max_threads == 0)
        params.max_threads = settings.max_threads;
}

ActionsDAG AggregatingStep::makeCreatingMissingKeysForGroupingSetDAG(
    const Block & in_header,
    const Block & out_header,
    const GroupingSetsParamsList & grouping_sets_params,
    UInt64 group,
    bool group_by_use_nulls)
{
    /// Here we create a DAG which fills missing keys and adds `__grouping_set` column
    ActionsDAG dag(in_header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.reserve(out_header.columns() + 1);

    ColumnConst::Ptr grouping_col = ColumnConst::create(ColumnUInt64::create(1, group), 0);
    const auto * grouping_node = &dag.addColumn(
        std::move(grouping_col), std::make_shared<DataTypeUInt64>(), "__grouping_set");

    grouping_node = &dag.materializeNode(*grouping_node);
    outputs.push_back(grouping_node);

    const auto & missing_columns = grouping_sets_params[group].missing_keys;
    const auto & used_keys = grouping_sets_params[group].used_keys;

    auto to_nullable_function = FunctionFactory::instance().get("toNullable", nullptr);
    for (size_t i = 0; i < out_header.columns(); ++i)
    {
        const auto & col = out_header.getByPosition(i);
        const auto missing_it = std::find_if(
            missing_columns.begin(), missing_columns.end(), [&](const auto & missing_col) { return missing_col == col.name; });
        const auto used_it = std::find_if(
            used_keys.begin(), used_keys.end(), [&](const auto & used_col) { return used_col == col.name; });
        if (missing_it != missing_columns.end())
        {
            auto column_with_default = col.column->cloneEmpty();
            col.type->insertDefaultInto(*column_with_default);
            column_with_default->finalize();

            ColumnConst::Ptr column = ColumnConst::create(std::move(column_with_default), 0);
            const auto * node = &dag.addColumn(std::move(column), col.type, col.name);
            node = &dag.materializeNode(*node);
            outputs.push_back(node);
        }
        else
        {
            const auto * column_node = dag.getOutputs()[in_header.getPositionByName(col.name)];
            if (used_it != used_keys.end() && group_by_use_nulls && removeLowCardinality(column_node->result_type)->canBeInsideNullable())
                outputs.push_back(&dag.addFunction(to_nullable_function, { column_node }, col.name));
            else
                outputs.push_back(column_node);
        }
    }

    dag.getOutputs().swap(outputs);
    return dag;
}

/// Sharded aggregation: pre-partition rows by hash(key) % N before aggregation.
/// As a result, same key from different rows will always go to the same shard and we can aggregate
/// each shard independently without merge phase.
bool AggregatingStep::canUseShardedAggregation(const QueryPipelineBuilder & pipeline) const
{
    if (!enable_sharding_aggregator)
        return false;

    /// Respect pipeline width — do not fan out a single stream into shards.
    if (pipeline.getNumStreams() <= 1)
        return false;
    if (params.max_threads <= 1)
        return false;

    /// Avoid too much overhead from routing
    if (pipeline.getNumStreams() * params.max_threads >= 100'000)
        return false;

    /// TODO(nihalzp): `max_rows_to_group_by` is enforced globally during the merge phase in normal
    /// aggregation. Could be supported by a post-step that counts total keys across shards.
    if (params.max_rows_to_group_by != 0)
        return false;

    /// Skip no-key aggregation as sharding does not give any benefit and has overhead.
    if (params.keys_size < 1)
        return false;

    /// We do not want to take over cases covered by InOrder Aggregation as those are faster.
    if (!sort_description_for_merging.empty())
        return false;

    if (!grouping_sets_params.empty())
        return false;

    /// TODO(nihalzp): Support this when we will have external aggregation
    if (should_produce_results_in_order_of_bucket_number)
        return false;

    /// Sharding is useful for high cardinality keys. For single-key, skip 1-byte types
    /// (UInt8/Int8 have at most 256 distinct values) and LowCardinality. For multi-key, skip
    /// if combined cardinality is low enough.
    constexpr size_t low_cardinality_threshold_bytes = 1;
    const bool is_low_cardinality_keyspace
        = std::accumulate(
              params.keys.begin(),
              params.keys.end(),
              size_t{0},
              [&](size_t sum, const String & key) -> size_t
              {
                  const auto & type = pipeline.getHeader().getByName(key).type;
                  if (type->lowCardinality())
                      return sum;
                  const auto inner = removeNullable(type);
                  return sum
                      + (inner->haveMaximumSizeOfValue() ? inner->getMaximumSizeOfValueInMemory() : low_cardinality_threshold_bytes + 1);
              })
        <= low_cardinality_threshold_bytes;
    if (is_low_cardinality_keyspace)
        return false;

    return true;
}

const char * AggregatingStep::adaptiveAggregatorRejectionReason(const QueryPipelineBuilder & pipeline) const
{
    if (!params.enable_adaptive_aggregator)
        return "disabled by the setting";

    if (pipeline.getNumStreams() <= 1 || params.max_threads <= 1)
        return "the aggregation is single-stream";

    if (params.only_merge)
        return "the step only merges";

    /// TODO (nihalzp): Support the group-by limits and the overflow row.
    if (params.max_rows_to_group_by != 0 || params.overflow_row)
        return "group-by limits or the overflow row are set";

    if (params.keys_size < 1)
        return "the aggregation has no keys";

    if (!sort_description_for_merging.empty())
        return "the aggregation is in order";

    if (!grouping_sets_params.empty())
        return "grouping sets are used";

    if (should_produce_results_in_order_of_bucket_number)
        return "the output must be bucket-ordered";

    if (skip_merging)
        return "the merge phase is skipped";

    if (params.group_by_two_level_threshold == 0 && params.group_by_two_level_threshold_bytes == 0)
        return "two-level aggregation is disabled";

    /// A prior run measured the query's staged stream as repeat-dominated and thawed: freezing
    /// cannot pay for this query, so do not engage it again. The verdict lives in the hash-table
    /// statistics; a run without it takes the ordinary path with the statistics-driven
    /// initialization, exactly as if the feature were off.
    if (params.stats_collecting_params.isCollectionAndUseEnabled())
    {
        const auto hint = getHashTablesStatistics<AggregationEntry>().getSizeHint(params.stats_collecting_params);
        if (hint && hint->adaptive_staging_repeat_dominated)
            return "a prior run measured the staged stream as repeat-dominated";
    }

    /// TODO (nihalzp): Support LowCardinality and Nullable keys.
    for (const auto & key : params.keys)
    {
        const auto & type = pipeline.getHeader().getByName(key).type;
        if (type->lowCardinality() || type->isNullable())
            return "a key is LowCardinality or Nullable";
    }

    Sizes key_sizes;
    const auto method = AggregatedDataVariants::chooseMethod(pipeline.getHeader(), params.keys, key_sizes);
    if (!AggregatedDataVariants::isConvertibleToTwoLevel(method))
        return "the aggregation method has no two-level form";

    return nullptr;
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    size_t new_merge_threads = merge_threads;
    size_t new_temporary_data_merge_threads = temporary_data_merge_threads;
    updateThreadsValues(new_merge_threads, new_temporary_data_merge_threads, params, settings);

    /// If the read step deliberately reduced the stream count (e.g. ReadFromMergeTree
    /// chose fewer streams because data is small), don't expand beyond what was produced.
    /// This avoids overhead from mostly-empty streams in subsequent steps.
    /// Note: must be computed after updateThreadsValues, which resolves params.max_threads from 0 to settings.max_threads.
    const size_t max_threads = pipeline.getReadStreamCountWasReduced()
        ? std::min(params.max_threads, pipeline.getNumStreams())
        : params.max_threads;

    /// Clear after use so it does not leak into downstream JOIN/UNION pipeline compositions.
    pipeline.setReadStreamCountWasReduced(false);

    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;

    /// optimize_aggregation_in_order
    if (!sort_description_for_merging.empty())
    {
        /// two-level aggregation is not supported anyway for in order aggregation.
        allow_to_use_two_level_group_by = false;

        /// It is incorrect for in order aggregation.
        params.stats_collecting_params.disable();

        /// Aggregation in order rebuilds the aggregation-method state for every run of equal
        /// order-key values, so the whole-block `prealloc_serialized` method would make it
        /// quadratic. Fall back to the plain `serialized` method (see `Params::aggregation_in_order`).
        params.aggregation_in_order = true;
    }

    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    const bool use_sharded_aggregation = canUseShardedAggregation(pipeline);

    const char * adaptive_rejection
        = use_sharded_aggregation ? "the sharded aggregation is used instead" : adaptiveAggregatorRejectionReason(pipeline);
    const bool use_adaptive_aggregator = adaptive_rejection == nullptr;
    if (!use_adaptive_aggregator && params.enable_adaptive_aggregator)
        LOG_TRACE(getLogger("AggregatingStep"), "Adaptive aggregation is not engaged: {}", adaptive_rejection);
    params.enable_adaptive_aggregator = use_adaptive_aggregator;

    if (use_sharded_aggregation)
    {
        /// Even though there is no merge phase, two-level can help keep each hash table small
        /// and make hash table operations faster. However, after benchmarking, there have been
        /// mostly slowdowns for most common queries. Therefore, disable two-level for sharded aggregation.
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;

        /// Sharded aggregation does not implement temporary-file spill/merge yet.
        params.max_bytes_before_external_group_by = 0;

        /// TODO(nihalzp): Support it
        params.stats_collecting_params.disable();
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    const auto & src_header = pipeline.getSharedHeader();
    auto transform_params = std::make_shared<AggregatingTransformParams>(src_header, std::move(params), final);

    if (!grouping_sets_params.empty())
    {
        const size_t grouping_sets_size = grouping_sets_params.size();

        const size_t streams = pipeline.getNumStreams();

        auto input_header = std::make_shared<const Block>(pipeline.getHeader());

        if (grouping_sets_size > 1)
        {
            pipeline.transform([&](const OutputPortRawPtrs & ports)
            {
                Processors copiers;

                for (auto * port : ports)
                {
                    auto copier = std::make_shared<CopyTransform>(input_header, grouping_sets_size);
                    connect(*port, copier->getInputPort());
                    copiers.push_back(copier);
                }

                return copiers;
            });
        }

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            chassert(streams * grouping_sets_size == ports.size());
            Processors processors;
            for (size_t i = 0; i < grouping_sets_size; ++i)
            {
                Aggregator::Params params_for_set = transform_params->params.cloneWithKeys(grouping_sets_params[i].used_keys, false);
                auto transform_params_for_set = std::make_shared<AggregatingTransformParams>(src_header, std::move(params_for_set), final);

                if (streams > 1)
                {
                    auto many_data = std::make_shared<ManyAggregatedData>(streams);
                    for (size_t j = 0; j < streams; ++j)
                    {
                        auto aggregation_for_set = std::make_shared<AggregatingTransform>(
                            input_header,
                            transform_params_for_set,
                            many_data,
                            j,
                            new_merge_threads,
                            new_temporary_data_merge_threads,
                            should_produce_results_in_order_of_bucket_number,
                            skip_merging,
                            nullptr);
                        // For each input stream we have `grouping_sets_size` copies, so port index
                        // for transform #j should skip ports of first (j-1) streams.
                        connect(*ports[i + grouping_sets_size * j], aggregation_for_set->getInputs().front());
                        ports[i + grouping_sets_size * j] = &aggregation_for_set->getOutputs().front();
                        processors.push_back(aggregation_for_set);
                    }
                }
                else
                {
                    auto aggregation_for_set
                        = std::make_shared<AggregatingTransform>(input_header, transform_params_for_set, dataflow_cache_updater);
                    connect(*ports[i], aggregation_for_set->getInputs().front());
                    ports[i] = &aggregation_for_set->getOutputs().front();
                    processors.push_back(aggregation_for_set);
                }
            }

            if (streams > 1)
            {
                OutputPortRawPtrs new_ports;
                new_ports.reserve(grouping_sets_size);

                for (size_t i = 0; i < grouping_sets_size; ++i)
                {
                    size_t output_it = i;
                    auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getSharedHeader(), streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += grouping_sets_size, ++input_it)
                        connect(*ports[output_it], *input_it);
                    new_ports.push_back(&resize->getOutputs().front());
                    processors.push_back(resize);
                }

                ports.swap(new_ports);
            }

            chassert(ports.size() == grouping_sets_size);
            auto output_header = transform_params->getHeader();
            if (group_by_use_nulls)
                convertToNullable(output_header, params.keys);

            for (size_t set_counter = 0; set_counter < grouping_sets_size; ++set_counter)
            {
                const auto & header = ports[set_counter]->getSharedHeader();

                auto dag = makeCreatingMissingKeysForGroupingSetDAG(*header, output_header, grouping_sets_params, set_counter, group_by_use_nulls);
                auto expression = std::make_shared<ExpressionActions>(std::move(dag), settings.getActionsSettings());
                auto transform = std::make_shared<ExpressionTransform>(header, expression);

                connect(*ports[set_counter], transform->getInputPort());
                processors.emplace_back(std::move(transform));
            }

            return processors;
        });

        /// After grouping sets aggregation, the stream count equals grouping_sets_size (typically 2-3),
        /// which is artificially low and unrelated to data volume. Always expand to the full max_threads
        /// (ignoring the read-stream-reduced cap) so downstream steps can process the result in parallel.
        pipeline.resize(params.max_threads);

        aggregating = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));
        return;
    }

    if (!sort_description_for_merging.empty())
    {
        /// We don't rely here on input_stream.sort_description because it is not correctly propagated for now in all cases
        /// see https://github.com/ClickHouse/ClickHouse/pull/45892#discussion_r1094503048
        if (explicit_sorting_required_for_aggregation_in_order)
        {
            /// We don't really care about optimality of this sorting, because it's required only in fairly marginal cases.
            SortingStep::fullSortStreams(
                pipeline, SortingStep::Settings(params.max_block_size), sort_description_for_merging, 0 /* limit */);
        }

        if (pipeline.getNumStreams() > 1)
        {
            /** The pipeline is the following:
             *
             * --> AggregatingInOrder                                                  --> MergingAggregatedBucket
             * --> AggregatingInOrder --> FinishAggregatingInOrder --> ResizeProcessor --> MergingAggregatedBucket
             * --> AggregatingInOrder                                                  --> MergingAggregatedBucket
             */

            auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
            size_t counter = 0;
            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                /// We want to merge aggregated data in batches of size
                /// not greater than 'aggregation_in_order_max_block_bytes'.
                /// So, we reduce 'max_bytes' value for aggregation in 'merge_threads' times.
                return std::make_shared<AggregatingInOrderTransform>(
                    header,
                    transform_params,
                    sort_description_for_merging,
                    group_by_sort_description,
                    max_block_size,
                    aggregation_in_order_max_block_bytes / new_merge_threads,
                    many_data,
                    counter++,
                    limit_hint,
                    nullptr // `dataflow_cache_updater` will be passed to `MergingAggregatedBucketTransform` below
                );
            });

            if (skip_merging)
            {
                pipeline.addSimpleTransform([&](const SharedHeader & header)
                                            { return std::make_shared<FinalizeAggregatedTransform>(header, transform_params); });
                pipeline.resize(max_threads);
                aggregating_in_order = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));
                return;
            }

            aggregating_in_order = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));

            auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                pipeline.getSharedHeader(),
                pipeline.getNumStreams(),
                transform_params,
                group_by_sort_description,
                max_block_size,
                aggregation_in_order_max_block_bytes,
                limit_hint);

            pipeline.addTransform(std::move(transform));

            /// Do merge of aggregated data in parallel.
            pipeline.resize(new_merge_threads);

            const auto & required_sort_description = memoryBoundMergingWillBeUsed() ? group_by_sort_description : SortDescription{};
            pipeline.addSimpleTransform(
                [&](const SharedHeader &)
                { return std::make_shared<MergingAggregatedBucketTransform>(transform_params, required_sort_description, dataflow_cache_updater); });

            if (memoryBoundMergingWillBeUsed())
            {
                pipeline.addTransform(
                    std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipeline.getHeader(), pipeline.getNumStreams()));
            }

            aggregating_sorted = collector.detachProcessors(static_cast<size_t>(AggregatingStage::FinalAggregation));
        }
        else
        {
            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<AggregatingInOrderTransform>(
                    header, transform_params,
                    sort_description_for_merging, group_by_sort_description,
                    max_block_size, aggregation_in_order_max_block_bytes,
                    limit_hint,
                    dataflow_cache_updater);
            });

            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<FinalizeAggregatedTransform>(header, transform_params);
            });

            aggregating_in_order = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));
        }

        return;
    }

    /// Sharded aggregation: shard rows by hash(key) % N, then aggregate per shard independently.
    if (use_sharded_aggregation)
    {
        /// TODO(nihalzp): Compare perf against always choosing a power of two.
        const size_t num_shards = max_threads;
        const size_t num_streams = pipeline.getNumStreams();

        /// Resolve key column positions for BufferedShardByHashTransform.
        auto stream_header = pipeline.getSharedHeader();
        ColumnNumbers key_columns;
        key_columns.reserve(transform_params->params.keys.size());
        for (const auto & key : transform_params->params.keys)
            key_columns.push_back(stream_header->getPositionByName(key));

        /// Add BufferedShardByHashTransform to each stream (1 input -> num_shards outputs).
        /// After this the pipeline has num_streams * num_shards output ports.
        pipeline.transform(
            [&, stream_header, key_columns](OutputPortRawPtrs ports)
            {
                Processors shard_transforms;
                for (auto * port : ports)
                {
                    auto shard_transform = std::make_shared<BufferedShardByHashTransform>(stream_header, num_shards, key_columns);
                    connect(*port, shard_transform->getInputs().front());
                    shard_transforms.push_back(shard_transform);
                }
                return shard_transforms;
            });

        /// For each shard, collect outputs from all sharding transforms and merge them with Resize(num_streams -> 1).
        /// After this the pipeline has num_shards output ports (one per shard).
        if (num_streams > 1)
        {
            pipeline.transform(
                [&, stream_header](OutputPortRawPtrs ports)
                {
                    chassert(ports.size() == num_streams * num_shards);
                    Processors resize_processors;

                    for (size_t shard = 0; shard < num_shards; ++shard)
                    {
                        /// Shard k from sharding transform i is at index: i * num_shards + shard
                        auto resize = std::make_shared<ResizeProcessor>(stream_header, num_streams, 1);
                        auto & resize_inputs = resize->getInputs();
                        auto input_it = resize_inputs.begin();

                        /// For shard `s`, connect the `s`-th output of each BufferedShardByHashTransform
                        /// to this ResizeProcessor input. BufferedShardByHashTransform routes rows by
                        /// `hash(group_by_key) % num_shards`, so identical GROUP BY keys always
                        /// land on the same shard.
                        for (size_t stream = 0; stream < num_streams; ++stream, ++input_it)
                            connect(*ports[stream * num_shards + shard], *input_it);

                        resize_processors.push_back(resize);
                    }

                    return resize_processors;
                });
        }

        scatter = collector.detachProcessors(static_cast<size_t>(AggregatingStage::Scatter));

        pipeline.addSimpleTransform(
            [&](const SharedHeader & shard_header)
            { return std::make_shared<AggregatingTransform>(shard_header, transform_params, dataflow_cache_updater); });

        chassert(!should_produce_results_in_order_of_bucket_number);

        aggregating = collector.detachProcessors(static_cast<size_t>(AggregatingStage::AggregatingSharded));
        return;
    }

    /// An aggregation without keys produces at most one row, so fanning its output out to
    /// multiple streams would only add processors and scheduling overhead to every downstream
    /// step (and to the whole pipeline execution) without any parallelism to gain.
    const size_t streams_after_aggregation = (should_produce_results_in_order_of_bucket_number || params.keys.empty()) ? 1 : max_threads;

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// But not if we execute aggregation over partitioned data in which case data streams shouldn't be mixed.
        if (!storage_has_evenly_distributed_read && !skip_merging)
            pipeline.resize(pipeline.getNumStreams(), true, settings.min_outstreams_per_resize_after_split);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
        if (use_adaptive_aggregator)
            many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();

        size_t counter = 0;
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header)
            {
                return std::make_shared<AggregatingTransform>(
                    header,
                    transform_params,
                    many_data,
                    counter++,
                    new_merge_threads,
                    new_temporary_data_merge_threads,
                    should_produce_results_in_order_of_bucket_number,
                    skip_merging,
                    dataflow_cache_updater);
            });

        pipeline.resize(streams_after_aggregation, false, settings.min_outstreams_per_resize_after_split);

        aggregating = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));
    }
    else
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<AggregatingTransform>(header, transform_params, dataflow_cache_updater); });

        pipeline.resize(streams_after_aggregation);

        aggregating = collector.detachProcessors(static_cast<size_t>(AggregatingStage::PartialAggregation));
    }
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    params.explain(settings);

    if (!sort_description_for_merging.empty())
    {
        settings.out << prefix << "Order: ";
        dumpSortDescription(sort_description_for_merging, settings);
        settings.out << '\n';
    }
    settings.out << prefix << "Skip merging: " << skip_merging << '\n';

    if (params.bucket_top_k)
        settings.out << prefix << "Bucket top-K: " << params.bucket_top_k << (params.bucket_top_k_ascending ? " ascending" : " descending")
                     << '\n';
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    if (!sort_description_for_merging.empty())
        map.add("Order", dumpSortDescription(sort_description_for_merging));
    if (params.bucket_top_k)
    {
        auto bucket_top_k_map = std::make_unique<JSONBuilder::JSONMap>();
        bucket_top_k_map->add("Limit", params.bucket_top_k);
        bucket_top_k_map->add("Ascending", params.bucket_top_k_ascending);
        map.add("Bucket Top-K", std::move(bucket_top_k_map));
    }
    map.add("Skip merging", skip_merging);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
    {
        IQueryPlanStep::describePipeline(aggregating, settings);
        IQueryPlanStep::describePipeline(scatter, settings);
    }
    else
    {
        /// Processors are printed in reverse order.
        IQueryPlanStep::describePipeline(aggregating_sorted, settings);
        IQueryPlanStep::describePipeline(aggregating_in_order, settings);
    }
}

bool AggregatingStep::canUseProjection() const
{
    /// For now, grouping sets are not supported.
    /// Aggregation in order should be applied after projection optimization if projection is full.
    /// Skip it here just in case.
    return grouping_sets_params.empty() && sort_description_for_merging.empty();
}

void AggregatingStep::requestOnlyMergeForAggregateProjection(const SharedHeader & input_header)
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    auto output_header = getOutputHeader();

    /// The projection header may have different types for key columns due to metadata-only ALTERs
    /// (e.g., extending an Enum). We need to adapt the input header to match the expected output types.
    /// See https://github.com/ClickHouse/ClickHouse/issues/56334
    auto adapted_header = std::make_shared<Block>();
    for (const auto & column : *input_header)
    {
        if (output_header->has(column.name))
        {
            /// Use the type from expected output header for columns that exist in output
            const auto & expected_column = output_header->getByName(column.name);
            adapted_header->insert({expected_column.type->createColumn(), expected_column.type, column.name});
        }
        else
        {
            /// Keep original for columns not in output (e.g., intermediate aggregate states)
            adapted_header->insert(column.cloneEmpty());
        }
    }

    input_headers.front() = adapted_header;
    params.only_merge = true;
    updateOutputHeader();
    assertBlocksHaveEqualStructure(*output_header, *getOutputHeader(), "AggregatingStep");
}

std::unique_ptr<AggregatingProjectionStep> AggregatingStep::convertToAggregatingProjection(const SharedHeader & input_header) const
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    /// The projection pipeline never runs the adaptive admission and never creates the
    /// adaptive shared state, so the flag it receives must not claim otherwise: it would only
    /// mis-drive the size-hint branch of `initDataVariantsWithSizeHint`.
    auto params_without_adaptive = params;
    params_without_adaptive.enable_adaptive_aggregator = false;

    auto aggregating_projection = std::make_unique<AggregatingProjectionStep>(
        SharedHeaders{input_headers.front(), input_header},
        params_without_adaptive,
        final,
        merge_threads,
        temporary_data_merge_threads
    );

    assertBlocksHaveEqualStructure(*getOutputHeader(), *aggregating_projection->getOutputHeader(), "AggregatingStep");
    return aggregating_projection;
}

void AggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(appendGroupingColumn(params.getHeader(*input_headers.front(), final), params.keys, !grouping_sets_params.empty(), group_by_use_nulls));
}

bool AggregatingStep::memoryBoundMergingWillBeUsed() const
{
    return DB::memoryBoundMergingWillBeUsed(
        should_produce_results_in_order_of_bucket_number, memory_bound_merging_of_aggregation_results_enabled, sort_description_for_merging);
}

AggregatingProjectionStep::AggregatingProjectionStep(
    SharedHeaders input_headers_,
    Aggregator::Params params_,
    bool final_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_)
    : params(std::move(params_))
    , final(final_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
    updateInputHeaders(std::move(input_headers_));
}

std::vector<size_t> AggregatingProjectionStep::getStepGroups() const
{
    return {
        static_cast<size_t>(AggregatingStep::AggregatingStage::PartialAggregation),
        static_cast<size_t>(AggregatingStep::AggregatingStage::FinalAggregation)
    };
}

String AggregatingProjectionStep::getStepGroupName(size_t group) const
{
    switch (static_cast<AggregatingStep::AggregatingStage>(group))
    {
        case AggregatingStep::AggregatingStage::PartialAggregation: return "partial aggregation";
        case AggregatingStep::AggregatingStage::FinalAggregation: return "final aggregation";
        case AggregatingStep::AggregatingStage::Scatter: [[fallthrough]];
        case AggregatingStep::AggregatingStage::AggregatingSharded: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown AggregatingProjectionStep group {}", group);
}

void AggregatingProjectionStep::updateOutputHeader()
{
    if (input_headers.size() != 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "AggregatingProjectionStep is expected to have two input streams, got {}",
            input_headers.size());

    auto normal_parts_header = params.getHeader(*input_headers.front(), final);
    params.only_merge = true;
    auto projection_parts_header = params.getHeader(*input_headers.back(), final);
    params.only_merge = false;

    assertBlocksHaveEqualStructure(normal_parts_header, projection_parts_header, "AggregatingProjectionStep");
    output_header = std::make_shared<const Block>(std::move(normal_parts_header));
}

QueryPipelineBuilderPtr AggregatingProjectionStep::updatePipeline(
    QueryPipelineBuilders pipelines,
    const BuildQueryPipelineSettings & settings)
{
    size_t new_merge_threads = merge_threads;
    size_t new_temporary_data_merge_threads = temporary_data_merge_threads;
    updateThreadsValues(new_merge_threads, new_temporary_data_merge_threads, params, settings);

    auto & normal_parts_pipeline = pipelines.front();
    auto & projection_parts_pipeline = pipelines.back();

    /// Here we create shared ManyAggregatedData for both projection and ordinary data.
    /// For ordinary data, AggregatedData is filled in a usual way.
    /// For projection data, AggregatedData is filled by merging aggregation states.
    /// When all AggregatedData is filled, we merge aggregation states together in a usual way.
    /// Pipeline will look like:
    /// ReadFromProjection   -> Aggregating (only merge states) ->
    /// ReadFromProjection   -> Aggregating (only merge states) ->
    /// ...                                                     -> Resize -> ConvertingAggregatedToChunks
    /// ReadFromOrdinaryPart -> Aggregating (usual)             ->           (added by last Aggregating)
    /// ReadFromOrdinaryPart -> Aggregating (usual)             ->
    /// ...
    auto many_data = std::make_shared<ManyAggregatedData>(normal_parts_pipeline->getNumStreams() + projection_parts_pipeline->getNumStreams());
    size_t counter = 0;

    AggregatorListPtr aggregator_list_ptr = std::make_shared<AggregatorList>();

    /// TODO apply optimize_aggregation_in_order here somehow
    auto build_aggregate_pipeline = [&](QueryPipelineBuilder & pipeline, bool projection)
    {
        auto params_copy = params;
        if (projection)
            params_copy.only_merge = true;

        AggregatingTransformParamsPtr transform_params = std::make_shared<AggregatingTransformParams>(
            pipeline.getHeader(), std::move(params_copy), aggregator_list_ptr, final);

        pipeline.resize(pipeline.getNumStreams(), true);

        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<AggregatingTransform>(
                header, transform_params, many_data, counter++, new_merge_threads, new_temporary_data_merge_threads);
        });
    };

    build_aggregate_pipeline(*normal_parts_pipeline, false);
    build_aggregate_pipeline(*projection_parts_pipeline, true);

    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    for (auto & cur_pipeline : pipelines)
        assertBlocksHaveEqualStructure(cur_pipeline->getHeader(), *getOutputHeader(), "AggregatingProjectionStep");

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), 0, &processors);
    pipeline->resize(1);
    return pipeline;
}


void AggregatingStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const
{
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
    settings[QueryPlanSerializationSetting::aggregation_in_order_max_block_bytes] = aggregation_in_order_max_block_bytes;

    settings[QueryPlanSerializationSetting::aggregation_sort_result_by_bucket_number] = should_produce_results_in_order_of_bucket_number;
    settings[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging] = memory_bound_merging_of_aggregation_results_enabled;

    settings[QueryPlanSerializationSetting::max_rows_to_group_by] = params.max_rows_to_group_by;
    settings[QueryPlanSerializationSetting::group_by_overflow_mode] = params.group_by_overflow_mode;

    settings[QueryPlanSerializationSetting::group_by_two_level_threshold] = params.group_by_two_level_threshold;
    settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes] = params.group_by_two_level_threshold_bytes;

    settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by] = params.max_bytes_before_external_group_by;
    settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set] = params.empty_result_for_aggregation_by_empty_set;

    settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = params.min_free_disk_space;

    settings[QueryPlanSerializationSetting::compile_aggregate_expressions] = params.compile_aggregate_expressions;
    settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression] = params.min_count_to_compile_aggregate_expression;

    settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation] = params.enable_prefetch;
    settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys] = params.optimize_group_by_constant_keys;
    settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization] = params.min_hit_rate_to_use_consecutive_keys_optimization;

    settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_aggregation] = params.stats_collecting_params.isCollectionAndUseEnabled();
    settings[QueryPlanSerializationSetting::max_entries_for_hash_table_stats] = params.stats_collecting_params.max_entries_for_hash_table_stats;
    settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_aggregation] = params.stats_collecting_params.max_size_to_preallocate;

    settings[QueryPlanSerializationSetting::enable_producing_buckets_out_of_order_in_aggregation] = params.enable_producing_buckets_out_of_order_in_aggregation;
    settings[QueryPlanSerializationSetting::enable_parallel_single_level_merge] = params.enable_parallel_single_level_merge;

    /// `QueryPlanSerializationSettings` is a strict named schema, so these two names may go on the wire only
    /// towards a peer whose version knows them; see the comment below on the packed-string-keys setting.
    /// A peer that predates them has no adaptive aggregator at all, so leaving them out is also the correct
    /// behaviour and not merely the safe one: the receiver then runs the ordinary aggregation, which is what
    /// it would do with the setting off. The result is identical either way - the adaptive path is exact.
    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ADAPTIVE_AGGREGATOR)
    {
        settings[QueryPlanSerializationSetting::enable_adaptive_aggregator] = params.enable_adaptive_aggregator;
        settings[QueryPlanSerializationSetting::adaptive_aggregator_freeze_threshold] = params.adaptive_aggregator_freeze_threshold;
    }

    /// Both values, every version: a peer predating the name serializes String keys the way `false` does, so
    /// omitting either one would silently leave it on the other layout.
    settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte] = params.serialize_string_with_zero_byte;

    /// A peer whose query-plan serialization version knows the name (this `version` is already the minimum of ours
    /// and the peer's) receives the value whenever the legacy method is requested, so the setting always takes
    /// effect on remote aggregation under `serialize_query_plan = 1`.
    ///
    /// Towards an older peer the name is written only when the legacy method is requested *and* this step can
    /// actually choose the single-`String` method *and* the plan can go two-level.
    /// `QueryPlanSerializationSettings` is a strict named schema: `writeChangedBinary` writes every touched
    /// entry by name and `readBinary` throws on a name it does not know, so writing this one whenever the session
    /// setting is off would make plans for `count()` or `GROUP BY UInt64` - where the setting cannot change anything -
    /// unreadable by a peer that predates it. Leaving it out keeps the receiver at the default (the packed method),
    /// and a peer too old to know the name fails closed on an explicit `false` instead of silently aggregating with
    /// the other method.
    ///
    /// Failing closed is deliberate, and it is *not* made redundant by the two-level fence in
    /// `MultiplexedConnections::sendQuery` / `HedgedConnections::sendQuery`. Those zero
    /// `group_by_two_level_threshold` / `group_by_two_level_threshold_bytes` in the `Settings` sent alongside the
    /// query, but a deserialized `AggregatingStep` takes both thresholds from the plan's own
    /// `QueryPlanSerializationSettings` (see `deserialize` below), which were written here from the initiator's
    /// unmodified `params`. So under `serialize_query_plan = 1` the fence does not reach the remote aggregation: a
    /// peer that silently used the other method could still go two-level, and two-level bucket numbering differs
    /// between the two methods, which corrupts memory-efficient distributed merging. The exception is the only safe
    /// outcome for that combination.
    ///
    /// When both serialized two-level thresholds are `0`, however, the mismatch cannot be observed, so towards an
    /// old peer the name is left off the wire and the peer may run the plan with its default method. The receiver
    /// takes both thresholds from the very settings written above, and with both at `0` every path to a two-level
    /// state is closed:
    /// `worthConvertToTwoLevel` is false for any size (also in the size-hint path of `initDataVariantsWithSizeHint`),
    /// the external-group-by spill in `Aggregator::executeOnBlock` additionally requires `worth_convert_to_two_level`,
    /// and the conversion in `Aggregator::mergeVariants` fires only when some variant is two-level already. The step
    /// then only ever produces single-level blocks (`bucket_num = -1`), whose rows and serialized aggregate states do
    /// not depend on the hash-table method, and every consumer merges them as a plain set-union by key.
    if (!params.enable_packed_string_keys
        && (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PACKED_STRING_KEYS_SETTING
            || ((params.group_by_two_level_threshold != 0 || params.group_by_two_level_threshold_bytes != 0)
                && aggregationCanUsePackedStringKeys(*input_headers.front(), params.keys, grouping_sets_params))))
        settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation] = false;
}

void AggregatingStep::serialize(Serialization & ctx) const
{
    /// Flags encode boolean properties that affect the data format or plan structure.
    /// Bit layout: 1=final, 2=overflow_row, 4=group_by_use_nulls, 8=grouping_sets,
    ///             16=stats_key, 32=in_order_aggregation, 64=explicit_sorting_required.
    UInt8 flags = 0;
    if (final && !ctx.for_cache_key)
        flags |= 1;
    if (params.overflow_row)
        flags |= 2;
    if (group_by_use_nulls)
        flags |= 4;
    if (!grouping_sets_params.empty())
        flags |= 8;
    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        flags |= 16;
    if (!sort_description_for_merging.empty())
        flags |= 32;
    if (explicit_sorting_required_for_aggregation_in_order)
        flags |= 64;

    /// The in-order aggregation payload exists only since query plan serialization version 2.
    /// Throw rather than send bytes the other side would misread (deserialize checks the same).
    if ((flags & (32 | 64)) && ctx.version < 2)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "In-order aggregation in a distributed plan requires query plan serialization "
            "version >= 2; all nodes must run the same version");

    writeIntBinary(flags, ctx.out);

    if (!sort_description_for_merging.empty())
    {
        serializeSortDescription(sort_description_for_merging, ctx.out);
        serializeSortDescription(group_by_sort_description, ctx.out);
    }

    writeVarUInt(params.keys.size(), ctx.out);
    for (const auto & key : params.keys)
        writeStringBinary(key, ctx.out);

    if (!grouping_sets_params.empty())
    {
        writeVarUInt(grouping_sets_params.size(), ctx.out);
        for (const auto & grouping_set : grouping_sets_params)
        {
            /// Only used keys are needed.
            writeVarUInt(grouping_set.used_keys.size(), ctx.out);
            for (const auto & used_key : grouping_set.used_keys)
                writeStringBinary(used_key, ctx.out);
        }
    }

    serializeAggregateDescriptions(params.aggregates, ctx.out);

    if (params.stats_collecting_params.isCollectionAndUseEnabled() && !ctx.for_cache_key)
        writeIntBinary(params.stats_collecting_params.key, ctx.out);
}

QueryPlanStepPtr AggregatingStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "AggregatingStep must have one input stream");

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool final = bool(flags & 1);
    bool overflow_row = bool(flags & 2);
    bool group_by_use_nulls = bool(flags & 4);
    bool has_grouping_sets = bool(flags & 8);
    bool has_stats_key = bool(flags & 16);
    bool has_in_order = bool(flags & 32);
    bool explicit_sorting_required = bool(flags & 64);

    /// The in-order aggregation payload exists only since query plan serialization version 2;
    /// on an older stream these bits are garbage, so reject them (serialize checks the same).
    if ((has_in_order || explicit_sorting_required) && ctx.version < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "In-order aggregation flags in a version {} query plan stream; they require version >= 2",
            ctx.version);

    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;
    if (has_in_order)
    {
        deserializeSortDescription(sort_description_for_merging, ctx.in);
        deserializeSortDescription(group_by_sort_description, ctx.in);
    }

    UInt64 num_keys = 0;
    readVarUInt(num_keys, ctx.in);
    Names keys(num_keys);
    for (auto & key : keys)
        readStringBinary(key, ctx.in);

    GroupingSetsParamsList grouping_sets_params;
    if (has_grouping_sets)
    {
        UInt64 num_groups = 0;
        readVarUInt(num_groups, ctx.in);
        for (size_t group_num = 0; group_num < num_groups; ++group_num)
        {
            auto & grouping_set = grouping_sets_params.emplace_back();
            UInt64 num_used_keys = 0;
            readVarUInt(num_used_keys, ctx.in);
            grouping_set.used_keys.resize(num_used_keys);
            NameSet used_keys_set;
            for (auto & used_key : grouping_set.used_keys)
            {
                readStringBinary(used_key, ctx.in);
                used_keys_set.insert(used_key);
            }
            if (num_keys > num_used_keys)
                grouping_set.missing_keys.reserve(num_keys - num_used_keys);
            for (const auto & key : keys)
                if (!used_keys_set.contains(key))
                    grouping_set.missing_keys.push_back(key);
        }
    }

    AggregateDescriptions aggregates;
    deserializeAggregateDescriptions(aggregates, ctx.in, ctx.max_type_complexity);

    UInt64 stats_key = 0;
    if (has_stats_key)
        readIntBinary(stats_key, ctx.in);

    StatsCollectingParams stats_collecting_params(
        stats_key,
        ctx.settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_aggregation],
        ctx.settings[QueryPlanSerializationSetting::max_entries_for_hash_table_stats],
        ctx.settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_aggregation]);

    Aggregator::Params params{
        keys,
        aggregates,
        overflow_row,
        ctx.settings[QueryPlanSerializationSetting::max_rows_to_group_by],
        ctx.settings[QueryPlanSerializationSetting::group_by_overflow_mode],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes],
        ctx.settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by],
        ctx.settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set],
        Context::getGlobalContextInstance()->getTempDataOnDisk(),
        0, //settings[QueryPlanSerializationSetting::max_threads],
        ctx.settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data],
        ctx.settings[QueryPlanSerializationSetting::compile_aggregate_expressions],
        ctx.settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression],
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation],
        /* only_merge */ false,
        ctx.settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys],
        ctx.settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params,
        ctx.settings[QueryPlanSerializationSetting::enable_producing_buckets_out_of_order_in_aggregation],
        ctx.settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte],
        ctx.settings[QueryPlanSerializationSetting::enable_parallel_single_level_merge],
        ctx.settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation],
        ctx.settings[QueryPlanSerializationSetting::enable_adaptive_aggregator],
        ctx.settings[QueryPlanSerializationSetting::adaptive_aggregator_freeze_threshold]};

    auto aggregating_step = std::make_unique<AggregatingStep>(
        ctx.input_headers.front(),
        std::move(params),
        std::move(grouping_sets_params),
        final,
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::aggregation_in_order_max_block_bytes],
        0, //merge_threads,
        0, //temporary_data_merge_threads,
        false, // storage_has_evenly_distributed_read, TODO: later
        group_by_use_nulls,
        std::move(sort_description_for_merging),
        std::move(group_by_sort_description),
        ctx.settings[QueryPlanSerializationSetting::aggregation_sort_result_by_bucket_number],
        ctx.settings[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging],
        explicit_sorting_required,
        false);

    return aggregating_step;
}

QueryPlanStepPtr AggregatingStep::clone() const
{
    return std::make_unique<AggregatingStep>(
        input_headers.front(),
        params,
        grouping_sets_params,
        final,
        max_block_size,
        aggregation_in_order_max_block_bytes,
        merge_threads,
        temporary_data_merge_threads,
        storage_has_evenly_distributed_read,
        group_by_use_nulls,
        sort_description_for_merging,
        group_by_sort_description,
        should_produce_results_in_order_of_bucket_number,
        memory_bound_merging_of_aggregation_results_enabled,
        explicit_sorting_required_for_aggregation_in_order,
        enable_sharding_aggregator
    );
}

void AggregatingStep::setFinal(bool new_value)
{
    if (new_value == final)
        return;

    final = new_value;

    /// Output header is different for partial and final result, so it needs to be updated when we switch between them.
    updateOutputHeader();
}

void registerAggregatingStep(QueryPlanStepRegistry & registry);
void registerAggregatingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Aggregating", AggregatingStep::deserialize);
}


}
