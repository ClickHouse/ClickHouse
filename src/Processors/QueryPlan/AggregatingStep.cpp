#include <cassert>
#include <cstddef>
#include <memory>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

        column.type = makeNullableSafe(column.type);
        column.column = makeNullableSafe(column.column);
    }
}

Block generateOutputHeader(const Block & input_header, const Names & keys, bool use_nulls)
{
    auto header = appendGroupingSetColumn(input_header);
    if (use_nulls)
        convertToNullable(header, keys);
    return header;
}


Block AggregatingStep::appendGroupingColumn(Block block, const Names & keys, bool has_grouping, bool use_nulls)
{
    if (!has_grouping)
        return block;

    return generateOutputHeader(block, keys, use_nulls);
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
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
    bool explicit_sorting_required_for_aggregation_in_order_)
    : ITransformingStep(
        input_stream_,
        appendGroupingColumn(params_.getHeader(input_stream_.header, final_), params_.keys, !grouping_sets_params_.empty(), group_by_use_nulls_),
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
{
    if (memoryBoundMergingWillBeUsed())
    {
        output_stream->sort_description = group_by_sort_description;
        output_stream->sort_scope = DataStream::SortScope::Global;
        output_stream->has_single_port = true;
    }
}

void AggregatingStep::applyOrder(SortDescription sort_description_for_merging_, SortDescription group_by_sort_description_)
{
    sort_description_for_merging = std::move(sort_description_for_merging_);
    group_by_sort_description = std::move(group_by_sort_description_);

    if (memoryBoundMergingWillBeUsed())
    {
        output_stream->sort_description = group_by_sort_description;
        output_stream->sort_scope = DataStream::SortScope::Global;
        output_stream->has_single_port = true;
    }

    explicit_sorting_required_for_aggregation_in_order = false;
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
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

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    const auto src_header = pipeline.getHeader();
    auto transform_params = std::make_shared<AggregatingTransformParams>(src_header, std::move(params), final);

    if (!grouping_sets_params.empty())
    {
        const size_t grouping_sets_size = grouping_sets_params.size();

        const size_t streams = pipeline.getNumStreams();

        auto input_header = pipeline.getHeader();
        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            Processors copiers;
            copiers.reserve(ports.size());

            for (auto * port : ports)
            {
                auto copier = std::make_shared<CopyTransform>(input_header, grouping_sets_size);
                connect(*port, copier->getInputPort());
                copiers.push_back(copier);
            }

            return copiers;
        });

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            assert(streams * grouping_sets_size == ports.size());
            Processors processors;
            for (size_t i = 0; i < grouping_sets_size; ++i)
            {
                Aggregator::Params params_for_set
                {
                    grouping_sets_params[i].used_keys,
                    transform_params->params.aggregates,
                    transform_params->params.overflow_row,
                    transform_params->params.max_rows_to_group_by,
                    transform_params->params.group_by_overflow_mode,
                    transform_params->params.group_by_two_level_threshold,
                    transform_params->params.group_by_two_level_threshold_bytes,
                    transform_params->params.max_bytes_before_external_group_by,
                    transform_params->params.empty_result_for_aggregation_by_empty_set,
                    transform_params->params.tmp_data_scope,
                    transform_params->params.max_threads,
                    transform_params->params.min_free_disk_space,
                    transform_params->params.compile_aggregate_expressions,
                    transform_params->params.min_count_to_compile_aggregate_expression,
                    transform_params->params.max_block_size,
                    transform_params->params.enable_prefetch,
                    /* only_merge */ false,
                    transform_params->params.stats_collecting_params};
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
                            merge_threads,
                            temporary_data_merge_threads,
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
                    auto aggregation_for_set = std::make_shared<AggregatingTransform>(input_header, transform_params_for_set);
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
                    auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getHeader(), streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += grouping_sets_size, ++input_it)
                        connect(*ports[output_it], *input_it);
                    new_ports.push_back(&resize->getOutputs().front());
                    processors.push_back(resize);
                }

                ports.swap(new_ports);
            }

            assert(ports.size() == grouping_sets_size);
            auto output_header = transform_params->getHeader();
            if (group_by_use_nulls)
                convertToNullable(output_header, params.keys);

            for (size_t set_counter = 0; set_counter < grouping_sets_size; ++set_counter)
            {
                const auto & header = ports[set_counter]->getHeader();

                /// Here we create a DAG which fills missing keys and adds `__grouping_set` column
                auto dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());
                ActionsDAG::NodeRawConstPtrs outputs;
                outputs.reserve(output_header.columns() + 1);

                auto grouping_col = ColumnConst::create(ColumnUInt64::create(1, set_counter), 0);
                const auto * grouping_node = &dag->addColumn(
                    {ColumnPtr(std::move(grouping_col)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

                grouping_node = &dag->materializeNode(*grouping_node);
                outputs.push_back(grouping_node);

                const auto & missing_columns = grouping_sets_params[set_counter].missing_keys;
                const auto & used_keys = grouping_sets_params[set_counter].used_keys;

                auto to_nullable_function = FunctionFactory::instance().get("toNullable", nullptr);
                for (size_t i = 0; i < output_header.columns(); ++i)
                {
                    auto & col = output_header.getByPosition(i);
                    const auto missing_it = std::find_if(
                        missing_columns.begin(), missing_columns.end(), [&](const auto & missing_col) { return missing_col == col.name; });
                    const auto used_it = std::find_if(
                        used_keys.begin(), used_keys.end(), [&](const auto & used_col) { return used_col == col.name; });
                    if (missing_it != missing_columns.end())
                    {
                        auto column_with_default = col.column->cloneEmpty();
                        col.type->insertDefaultInto(*column_with_default);
                        column_with_default->finalize();

                        auto column = ColumnConst::create(std::move(column_with_default), 0);
                        const auto * node = &dag->addColumn({ColumnPtr(std::move(column)), col.type, col.name});
                        node = &dag->materializeNode(*node);
                        outputs.push_back(node);
                    }
                    else
                    {
                        const auto * column_node = dag->getOutputs()[header.getPositionByName(col.name)];
                        if (used_it != used_keys.end() && group_by_use_nulls && column_node->result_type->canBeInsideNullable())
                            outputs.push_back(&dag->addFunction(to_nullable_function, { column_node }, col.name));
                        else
                            outputs.push_back(column_node);
                    }
                }

                dag->getOutputs().swap(outputs);
                auto expression = std::make_shared<ExpressionActions>(dag, settings.getActionsSettings());
                auto transform = std::make_shared<ExpressionTransform>(header, expression);

                connect(*ports[set_counter], transform->getInputPort());
                processors.emplace_back(std::move(transform));
            }

            return processors;
        });

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
            pipeline.addSimpleTransform([&](const Block & header)
            {
                /// We want to merge aggregated data in batches of size
                /// not greater than 'aggregation_in_order_max_block_bytes'.
                /// So, we reduce 'max_bytes' value for aggregation in 'merge_threads' times.
                return std::make_shared<AggregatingInOrderTransform>(
                    header, transform_params,
                    sort_description_for_merging, group_by_sort_description,
                    max_block_size, aggregation_in_order_max_block_bytes / merge_threads,
                    many_data, counter++);
            });

            if (skip_merging)
            {
                pipeline.addSimpleTransform([&](const Block & header)
                                            { return std::make_shared<FinalizeAggregatedTransform>(header, transform_params); });
                pipeline.resize(params.max_threads);
                aggregating_in_order = collector.detachProcessors(0);
                return;
            }

            aggregating_in_order = collector.detachProcessors(0);

            auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                transform_params,
                group_by_sort_description,
                max_block_size,
                aggregation_in_order_max_block_bytes);

            pipeline.addTransform(std::move(transform));

            /// Do merge of aggregated data in parallel.
            pipeline.resize(merge_threads);

            const auto & required_sort_description = memoryBoundMergingWillBeUsed() ? group_by_sort_description : SortDescription{};
            pipeline.addSimpleTransform(
                [&](const Block &)
                { return std::make_shared<MergingAggregatedBucketTransform>(transform_params, required_sort_description); });

            if (memoryBoundMergingWillBeUsed())
            {
                pipeline.addTransform(
                    std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipeline.getHeader(), pipeline.getNumStreams()));
            }

            aggregating_sorted = collector.detachProcessors(1);
        }
        else
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AggregatingInOrderTransform>(
                    header, transform_params,
                    sort_description_for_merging, group_by_sort_description,
                    max_block_size, aggregation_in_order_max_block_bytes);
            });

            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FinalizeAggregatedTransform>(header, transform_params);
            });

            aggregating_in_order = collector.detachProcessors(0);
        }

        finalizing = collector.detachProcessors(2);
        return;
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// But not if we execute aggregation over partitioned data in which case data streams shouldn't be mixed.
        if (!storage_has_evenly_distributed_read && !skip_merging)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform(
            [&](const Block & header)
            {
                return std::make_shared<AggregatingTransform>(
                    header,
                    transform_params,
                    many_data,
                    counter++,
                    merge_threads,
                    temporary_data_merge_threads,
                    should_produce_results_in_order_of_bucket_number,
                    skip_merging);
            });

        pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : params.max_threads, true /* force */);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AggregatingTransform>(header, transform_params); });

        pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : params.max_threads, false /* force */);

        aggregating = collector.detachProcessors(0);
    }
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
    String prefix(settings.offset, settings.indent_char);
    if (!sort_description_for_merging.empty())
    {
        settings.out << prefix << "Order: " << dumpSortDescription(sort_description_for_merging) << '\n';
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

void AggregatingStep::requestOnlyMergeForAggregateProjection(const DataStream & input_stream)
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    auto output_header = getOutputStream().header;
    input_streams.front() = input_stream;
    params.only_merge = true;
    updateOutputStream();
    assertBlocksHaveEqualStructure(output_header, getOutputStream().header, "AggregatingStep");
}

std::unique_ptr<AggregatingProjectionStep> AggregatingStep::convertToAggregatingProjection(const DataStream & input_stream) const
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    auto aggregating_projection = std::make_unique<AggregatingProjectionStep>(
        DataStreams{input_streams.front(), input_stream},
        params,
        final,
        merge_threads,
        temporary_data_merge_threads
    );

    assertBlocksHaveEqualStructure(getOutputStream().header, aggregating_projection->getOutputStream().header, "AggregatingStep");
    return aggregating_projection;
}

void AggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        appendGroupingColumn(params.getHeader(input_streams.front().header, final), params.keys, !grouping_sets_params.empty(), group_by_use_nulls),
        getDataStreamTraits());
}

bool AggregatingStep::memoryBoundMergingWillBeUsed() const
{
    return DB::memoryBoundMergingWillBeUsed(
        should_produce_results_in_order_of_bucket_number, memory_bound_merging_of_aggregation_results_enabled, sort_description_for_merging);
}

AggregatingProjectionStep::AggregatingProjectionStep(
    DataStreams input_streams_,
    Aggregator::Params params_,
    bool final_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_)
    : params(std::move(params_))
    , final(final_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
    input_streams = std::move(input_streams_);

    if (input_streams.size() != 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "AggregatingProjectionStep is expected to have two input streams, got {}",
            input_streams.size());

    auto normal_parts_header = params.getHeader(input_streams.front().header, final);
    params.only_merge = true;
    auto projection_parts_header = params.getHeader(input_streams.back().header, final);
    params.only_merge = false;

    assertBlocksHaveEqualStructure(normal_parts_header, projection_parts_header, "AggregatingProjectionStep");
    output_stream.emplace();
    output_stream->header = std::move(normal_parts_header);
}

QueryPipelineBuilderPtr AggregatingProjectionStep::updatePipeline(
    QueryPipelineBuilders pipelines,
    const BuildQueryPipelineSettings &)
{
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

        pipeline.resize(pipeline.getNumStreams(), true, true);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(
                header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });
    };

    build_aggregate_pipeline(*normal_parts_pipeline, false);
    build_aggregate_pipeline(*projection_parts_pipeline, true);

    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    for (auto & cur_pipeline : pipelines)
        assertBlocksHaveEqualStructure(cur_pipeline->getHeader(), getOutputStream().header, "AggregatingProjectionStep");

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), 0, &processors);
    pipeline->resize(1);
    return pipeline;
}

}
