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
#include <Processors/Transforms/HotKeyState.h>
#include <Processors/Transforms/ShardedAggregationSelectors.h>
#include <Processors/Transforms/BufferedScatterTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

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
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
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
    }

    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    const bool use_sharded_aggregation = canUseShardedAggregation(pipeline);

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
                            skip_merging);
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

        aggregating = collector.detachProcessors(0);
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
                    nullptr // `dataflow_cache_updater` will be passed to `MergingAggregatedBucketTransform` below
                );
            });

            if (skip_merging)
            {
                pipeline.addSimpleTransform([&](const SharedHeader & header)
                                            { return std::make_shared<FinalizeAggregatedTransform>(header, transform_params); });
                pipeline.resize(max_threads);
                aggregating_in_order = collector.detachProcessors(0);
                return;
            }

            aggregating_in_order = collector.detachProcessors(0);

            auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                pipeline.getSharedHeader(),
                pipeline.getNumStreams(),
                transform_params,
                group_by_sort_description,
                max_block_size,
                aggregation_in_order_max_block_bytes);

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

            aggregating_sorted = collector.detachProcessors(1);
        }
        else
        {
            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<AggregatingInOrderTransform>(
                    header, transform_params,
                    sort_description_for_merging, group_by_sort_description,
                    max_block_size, aggregation_in_order_max_block_bytes,
                    dataflow_cache_updater);
            });

            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<FinalizeAggregatedTransform>(header, transform_params);
            });

            aggregating_in_order = collector.detachProcessors(0);
        }

        finalizing = collector.detachProcessors(2);
        return;
    }

    /// Sharded aggregation.
    ///
    /// The core idea is that every row is sharded by the hash of its key into one of the aggregating
    /// shards. This gives each shard a disjoint set of keys, so the shards can be aggregated independently.
    ///
    /// The basic sharding approach is vulnerable to skew: when some keys are far more frequent than others,
    /// the shards they fall into become much larger and slower than the rest. For example, if a single key
    /// (often a default value or NULL) accounts for 90% of the rows, then one shard holds 90% of the data
    /// and serializes the whole pipeline.
    ///
    /// To avoid that, while scattering we detect which keys are hot by counting them on the fly over the
    /// first ~130K rows of each scatter. Once a key is found to be hot, we divert it during scatter to a
    /// separate hot shard, so each scatter feeds one hot shard for the hot keys and N cold shards for the
    /// rest.
    ///
    /// During the warmup some rows of a hot key may have landed in a cold shard before that key was
    /// detected. Afterwards we collect those rows (the "residue") from the cold shards and merge them
    /// together with the hot keys, while the remaining cold keys are aggregated independently with no merge
    /// at all. The number of hot keys is usually very small (for example about twice the number of
    /// aggregating threads), so merging them is cheap.

    /// The pipeline is built in few top level steps: scatter each stream into cold shards and a hot shard;
    /// the cold path, which aggregates and finalizes the cold keys; the hot path, which aggregates
    /// the hot keys; and the final merge, which finalizes the hot keys together with the residue from cold shards.
    if (use_sharded_aggregation)
    {
        chassert(!should_produce_results_in_order_of_bucket_number);

        /// TODO(nihalzp): Compare perf against always choosing a power of two.
        const size_t num_cold_shards = max_threads;
        const size_t num_streams = pipeline.getNumStreams();

        /// Each stream is scattered into one port per cold shard plus a single hot shard.
        /// The cold shards are shared between all streams but each stream has its own hot shard to send the hot keys.
        const size_t ports_per_stream = num_cold_shards + 1;

        const auto input_header = pipeline.getSharedHeader();

        /// The cold and hot shards are aggregated without finalizing, so that we can steal the hot-key
        /// residue from the cold shards before finalizing the hot keys together.
        auto intermediate_transform_params
            = std::make_shared<AggregatingTransformParams>(input_header, transform_params->params, /*final_=*/false);
        const auto intermediate_header = std::make_shared<const Block>(intermediate_transform_params->getHeader());

        ColumnNumbers input_key_positions;
        ColumnNumbers intermediate_key_positions;
        ColumnsWithTypeAndName key_header;
        input_key_positions.reserve(transform_params->params.keys.size());
        intermediate_key_positions.reserve(transform_params->params.keys.size());
        key_header.reserve(transform_params->params.keys.size());
        for (const auto & key : transform_params->params.keys)
        {
            input_key_positions.push_back(input_header->getPositionByName(key));
            intermediate_key_positions.push_back(intermediate_header->getPositionByName(key));
            key_header.push_back(input_header->getByName(key));
        }

        auto hot_key_state = std::make_shared<HotKeyState>(std::move(key_header));

        /// 1. Scatter each input stream's rows by key. As the rows stream past, the warmup detector counts
        ///    keys and promotes hot ones into `hot_key_state`; a row whose key is hot goes to the hot shard,
        ///    and every other row goes to the cold shard chosen from its key hash. A hot key may also be
        ///    present in its cold shard, but that's not a problem because we will take care of the hot-key
        ///    residue later.
        pipeline.transform(
            [&](OutputPortRawPtrs ports)
            {
                Processors scatters;
                for (auto * port : ports)
                {
                    auto scatter = std::make_shared<BufferedScatterTransform>(
                        input_header, ports_per_stream, makeInputHotColdSelector(hot_key_state, input_key_positions, num_cold_shards));
                    connect(*port, scatter->getInputs().front());
                    scatters.push_back(scatter);
                }
                return scatters;
            });

        pipeline.transform(
            [&](OutputPortRawPtrs ports)
            {
                chassert(ports.size() == num_streams * ports_per_stream);
                Processors processors;
                OutputPortRawPtrs merger_inputs;
                merger_inputs.reserve(num_cold_shards + num_streams);

                for (size_t shard = 0; shard < num_cold_shards; ++shard)
                {
                    /// 2. Aggregate each cold shard without finalizing, so that we can later steal the hot-key
                    ///    residue (the intermediate aggregate state).
                    auto cold_aggregator
                        = std::make_shared<AggregatingTransform>(input_header, intermediate_transform_params, dataflow_cache_updater);

                    /// Connect the cold aggregator to the corresponding shard port from every stream.
                    if (num_streams > 1)
                    {
                        auto shard_gather = std::make_shared<ResizeProcessor>(input_header, num_streams, 1);
                        auto input_it = shard_gather->getInputs().begin();
                        for (size_t stream = 0; stream < num_streams; ++stream, ++input_it)
                            connect(*ports[stream * ports_per_stream + shard], *input_it);
                        connect(shard_gather->getOutputs().front(), cold_aggregator->getInputs().front());
                        processors.push_back(shard_gather);
                    }
                    else
                    {
                        /// With a single input stream there is nothing to gather, so the shard's only port
                        /// feeds the aggregator directly.
                        connect(*ports[shard], cold_aggregator->getInputs().front());
                    }

                    processors.push_back(cold_aggregator);

                    /// 3. Separate the hot and cold keys into different ports from the cold shard. The hot keys present
                    ///    in the cold shard are the residue: rows that were sent to the cold shard before that key was
                    ///    detected during scatter.
                    auto residue_divert = std::make_shared<BufferedScatterTransform>(
                        intermediate_header, 2, makeDivertSelector(hot_key_state, intermediate_key_positions));
                    connect(cold_aggregator->getOutputs().front(), residue_divert->getInputs().front());
                    processors.push_back(residue_divert);

                    /// Connect the hot-key residue port to the merger inputs.
                    auto port_it = residue_divert->getOutputs().begin();
                    auto & residue_port = *port_it;
                    ++port_it;
                    auto & cold_port = *port_it;

                    merger_inputs.push_back(&residue_port);

                    /// 4. At this point, the cold keys are separated from the hot keys, so we can finalize the cold keys
                    ///    independently in the cold path. The hot keys (residue) will be finalized together with the hot
                    ///    intermediate states in the merge step later.
                    auto cold_finalizer = std::make_shared<FinalizeAggregatedTransform>(intermediate_header, transform_params);
                    connect(cold_port, cold_finalizer->getInputs().front());
                    processors.push_back(cold_finalizer);
                }

                /// 5. Aggregate the hot keys that were sent to the hot shard during scatter. The same hot keys are
                ///    also present as residue in the cold shards; that residue was diverted to the merger
                ///    inputs in step 3, so we will finalize them together in the merge step later.
                for (size_t stream = 0; stream < num_streams; ++stream)
                {
                    auto hot_aggregator
                        = std::make_shared<AggregatingTransform>(input_header, intermediate_transform_params, dataflow_cache_updater);
                    connect(*ports[stream * ports_per_stream + num_cold_shards], hot_aggregator->getInputs().front());
                    processors.push_back(hot_aggregator);
                    merger_inputs.push_back(&hot_aggregator->getOutputs().front());
                }

                /// 6. Gather all of those ports into a single stream. Its input is the residue ports from the cold
                ///    shards and the hot ports, and its output is one stream of intermediate states.
                auto merger_gather = std::make_shared<ResizeProcessor>(intermediate_header, merger_inputs.size(), 1);
                auto merger_input_it = merger_gather->getInputs().begin();
                for (auto * port : merger_inputs)
                {
                    connect(*port, *merger_input_it);
                    ++merger_input_it;
                }
                processors.push_back(merger_gather);

                /// 7. Final merge of the hot keys.
                auto merge_params = transform_params->params;
                merge_params.only_merge = true;
                auto merger = std::make_shared<MergingAggregatedTransform>(
                    intermediate_header, std::move(merge_params), final, GroupingSetsParamsList{});
                connect(merger_gather->getOutputs().front(), merger->getInputs().front());
                processors.push_back(merger);

                return processors;
            });

        aggregating = collector.detachProcessors(0);
        return;
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// But not if we execute aggregation over partitioned data in which case data streams shouldn't be mixed.
        if (!storage_has_evenly_distributed_read && !skip_merging)
            pipeline.resize(pipeline.getNumStreams(), true, settings.min_outstreams_per_resize_after_split);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

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

        pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads, false, settings.min_outstreams_per_resize_after_split);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<AggregatingTransform>(header, transform_params, dataflow_cache_updater); });

        pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);

        aggregating = collector.detachProcessors(0);
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
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    if (!sort_description_for_merging.empty())
        map.add("Order", dumpSortDescription(sort_description_for_merging));
    map.add("Skip merging", skip_merging);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
        IQueryPlanStep::describePipeline(aggregating, settings);
    else
    {
        /// Processors are printed in reverse order.
        IQueryPlanStep::describePipeline(finalizing, settings);
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

    auto aggregating_projection = std::make_unique<AggregatingProjectionStep>(
        SharedHeaders{input_headers.front(), input_header},
        params,
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


void AggregatingStep::serializeSettings(QueryPlanSerializationSettings & settings) const
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
}

void AggregatingStep::serialize(Serialization & ctx) const
{
    if (!sort_description_for_merging.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep optimized for in-order is not supported.");

    if (explicit_sorting_required_for_aggregation_in_order)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep explicit_sorting_required_for_aggregation_in_order is not supported.");

    /// If you wonder why something is serialized using settings, and other is serialized using flags, considerations are following:
    /// * flags are something that may change data format returning from the step
    /// * settings are something which already was in settings[QueryPlanSerializationSetting::h] and, usually, is passed to Aggregator unchanged
    /// Flags `final` and `group_by_use_nulls` change types, and `overflow_row` appends additional block to results.
    /// Settings like `max_rows_to_group_by` or `empty_result_for_aggregation_by_empty_set` affect the result,
    /// but does not change data format.
    /// Overall, the rule is not strict.

    UInt8 flags = 0;
    if (final && !ctx.skip_final_flag)
        flags |= 1;
    if (params.overflow_row)
        flags |= 2;
    if (group_by_use_nulls)
        flags |= 4;
    if (!grouping_sets_params.empty())
        flags |= 8;
    /// Ideally, key should be calculated from QueryPlan on the follower.
    /// So, let's have a flag to disable sending/reading pre-calculated value.
    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        flags |= 16;

    writeIntBinary(flags, ctx.out);

    if (explicit_sorting_required_for_aggregation_in_order)
        serializeSortDescription(group_by_sort_description, ctx.out);

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

    if (params.stats_collecting_params.isCollectionAndUseEnabled() && !ctx.skip_cache_key)
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
    deserializeAggregateDescriptions(aggregates, ctx.in);

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
        ctx.settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte]};

    SortDescription sort_description_for_merging;

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
        SortDescription{},
        ctx.settings[QueryPlanSerializationSetting::aggregation_sort_result_by_bucket_number],
        ctx.settings[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging],
        false,
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
