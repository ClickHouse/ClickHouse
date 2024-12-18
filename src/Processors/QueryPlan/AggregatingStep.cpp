#include <cassert>
#include <cstddef>
#include <memory>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
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
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
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
    bool explicit_sorting_required_for_aggregation_in_order_)
    : ITransformingStep(
        input_header_,
        appendGroupingColumn(params_.getHeader(input_header_, final_), params_.keys, !grouping_sets_params_.empty(), group_by_use_nulls_),
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

    auto grouping_col = ColumnConst::create(ColumnUInt64::create(1, group), 0);
    const auto * grouping_node = &dag.addColumn(
        {ColumnPtr(std::move(grouping_col)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

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

            auto column = ColumnConst::create(std::move(column_with_default), 0);
            const auto * node = &dag.addColumn({ColumnPtr(std::move(column)), col.type, col.name});
            node = &dag.materializeNode(*node);
            outputs.push_back(node);
        }
        else
        {
            const auto * column_node = dag.getOutputs()[in_header.getPositionByName(col.name)];
            if (used_it != used_keys.end() && group_by_use_nulls && column_node->result_type->canBeInsideNullable())
                outputs.push_back(&dag.addFunction(to_nullable_function, { column_node }, col.name));
            else
                outputs.push_back(column_node);
        }
    }

    dag.getOutputs().swap(outputs);
    return dag;
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    size_t new_merge_threads = merge_threads;
    size_t new_temporary_data_merge_threads = temporary_data_merge_threads;
    updateThreadsValues(new_merge_threads, new_temporary_data_merge_threads, params, settings);

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

        if (grouping_sets_size > 1)
        {
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
        }

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            assert(streams * grouping_sets_size == ports.size());
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

                auto dag = makeCreatingMissingKeysForGroupingSetDAG(header, output_header, grouping_sets_params, set_counter, group_by_use_nulls);
                auto expression = std::make_shared<ExpressionActions>(std::move(dag), settings.getActionsSettings());
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
                    max_block_size, aggregation_in_order_max_block_bytes / new_merge_threads,
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
            pipeline.resize(new_merge_threads);

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
                    new_merge_threads,
                    new_temporary_data_merge_threads,
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
    // settings.out << prefix << "Memory bound merging: " << memory_bound_merging_of_aggregation_results_enabled << '\n';
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

void AggregatingStep::requestOnlyMergeForAggregateProjection(const Header & input_header)
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    auto output_header = getOutputHeader();
    input_headers.front() = input_header;
    params.only_merge = true;
    updateOutputHeader();
    assertBlocksHaveEqualStructure(output_header, getOutputHeader(), "AggregatingStep");
}

std::unique_ptr<AggregatingProjectionStep> AggregatingStep::convertToAggregatingProjection(const Header & input_header) const
{
    if (!canUseProjection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot aggregate from projection");

    auto aggregating_projection = std::make_unique<AggregatingProjectionStep>(
        Headers{input_headers.front(), input_header},
        params,
        final,
        merge_threads,
        temporary_data_merge_threads
    );

    assertBlocksHaveEqualStructure(getOutputHeader(), aggregating_projection->getOutputHeader(), "AggregatingStep");
    return aggregating_projection;
}

void AggregatingStep::updateOutputHeader()
{
    output_header = appendGroupingColumn(params.getHeader(input_headers.front(), final), params.keys, !grouping_sets_params.empty(), group_by_use_nulls);
}

bool AggregatingStep::memoryBoundMergingWillBeUsed() const
{
    return DB::memoryBoundMergingWillBeUsed(
        should_produce_results_in_order_of_bucket_number, memory_bound_merging_of_aggregation_results_enabled, sort_description_for_merging);
}

AggregatingProjectionStep::AggregatingProjectionStep(
    Headers input_headers_,
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

    auto normal_parts_header = params.getHeader(input_headers.front(), final);
    params.only_merge = true;
    auto projection_parts_header = params.getHeader(input_headers.back(), final);
    params.only_merge = false;

    assertBlocksHaveEqualStructure(normal_parts_header, projection_parts_header, "AggregatingProjectionStep");
    output_header = std::move(normal_parts_header);
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

        pipeline.resize(pipeline.getNumStreams(), true, true);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(
                header, transform_params, many_data, counter++, new_merge_threads, new_temporary_data_merge_threads);
        });
    };

    build_aggregate_pipeline(*normal_parts_pipeline, false);
    build_aggregate_pipeline(*projection_parts_pipeline, true);

    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    for (auto & cur_pipeline : pipelines)
        assertBlocksHaveEqualStructure(cur_pipeline->getHeader(), getOutputHeader(), "AggregatingProjectionStep");

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), 0, &processors);
    pipeline->resize(1);
    return pipeline;
}


void AggregatingStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings.max_block_size = max_block_size;
    settings.aggregation_in_order_max_block_bytes = aggregation_in_order_max_block_bytes;

    settings.aggregation_in_order_memory_bound_merging = should_produce_results_in_order_of_bucket_number;
    settings.aggregation_sort_result_by_bucket_number = memory_bound_merging_of_aggregation_results_enabled;

    settings.max_rows_to_group_by = params.max_rows_to_group_by;
    settings.group_by_overflow_mode = params.group_by_overflow_mode;

    settings.group_by_two_level_threshold = params.group_by_two_level_threshold;
    settings.group_by_two_level_threshold_bytes = params.group_by_two_level_threshold_bytes;

    settings.max_bytes_before_external_group_by = params.max_bytes_before_external_group_by;
    settings.empty_result_for_aggregation_by_empty_set = params.empty_result_for_aggregation_by_empty_set;

    settings.min_free_disk_space_for_temporary_data = params.min_free_disk_space;

    settings.compile_aggregate_expressions = params.compile_aggregate_expressions;
    settings.min_count_to_compile_aggregate_expression = params.min_count_to_compile_aggregate_expression;

    settings.enable_software_prefetch_in_aggregation = params.enable_prefetch;
    settings.optimize_group_by_constant_keys = params.optimize_group_by_constant_keys;
    settings.min_hit_rate_to_use_consecutive_keys_optimization = params.min_hit_rate_to_use_consecutive_keys_optimization;

    settings.collect_hash_table_stats_during_aggregation = params.stats_collecting_params.isCollectionAndUseEnabled();
    settings.max_entries_for_hash_table_stats = params.stats_collecting_params.max_entries_for_hash_table_stats;
    settings.max_size_to_preallocate_for_aggregation = params.stats_collecting_params.max_size_to_preallocate;
}

void AggregatingStep::serialize(Serialization & ctx) const
{
    if (!sort_description_for_merging.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep optimized for in-order is not supported.");

    if (!grouping_sets_params.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep with grouping sets is not supported.");

    if (explicit_sorting_required_for_aggregation_in_order)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep explicit_sorting_required_for_aggregation_in_order is not supported.");

    /// If you wonder why something is serialized using settings, and other is serialized using flags, considerations are following:
    /// * flags are something that may change data format returning from the step
    /// * settings are something which already was in Settings.h and, usually, is passed to Aggregator unchanged
    /// Flags `final` and `group_by_use_nulls` change types, and `overflow_row` appends additional block to results.
    /// Settings like `max_rows_to_group_by` or `empty_result_for_aggregation_by_empty_set` affect the result,
    /// but does not change data format.
    /// Overall, the rule is not strict.

    UInt8 flags = 0;
    if (final)
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

    serializeAggregateDescriptions(params.aggregates, ctx.out);

    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        writeIntBinary(params.stats_collecting_params.key, ctx.out);
}

std::unique_ptr<IQueryPlanStep> AggregatingStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "AggregatingStep must have one input stream");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool final = bool(flags & 1);
    bool overflow_row = bool(flags & 2);
    bool group_by_use_nulls = bool(flags & 4);
    bool has_grouping_sets = bool(flags & 8);
    bool has_stats_key = bool(flags & 16);

    if (has_grouping_sets)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of AggregatingStep with grouping sets is not supported.");

    UInt64 num_keys;
    readVarUInt(num_keys, ctx.in);
    Names keys(num_keys);
    for (auto & key : keys)
        readStringBinary(key, ctx.in);

    AggregateDescriptions aggregates;
    deserializeAggregateDescriptions(aggregates, ctx.in);

    UInt64 stats_key = 0;
    if (has_stats_key)
        readIntBinary(stats_key, ctx.in);

    StatsCollectingParams stats_collecting_params(
        stats_key,
        ctx.settings.collect_hash_table_stats_during_aggregation,
        ctx.settings.max_entries_for_hash_table_stats,
        ctx.settings.max_size_to_preallocate_for_aggregation);

    Aggregator::Params params
    {
        keys,
        aggregates,
        overflow_row,
        ctx.settings.max_rows_to_group_by,
        ctx.settings.group_by_overflow_mode,
        ctx.settings.group_by_two_level_threshold,
        ctx.settings.group_by_two_level_threshold_bytes,
        ctx.settings.max_bytes_before_external_group_by,
        ctx.settings.empty_result_for_aggregation_by_empty_set,
        Context::getGlobalContextInstance()->getTempDataOnDisk(),
        0, //settings.max_threads,
        ctx.settings.min_free_disk_space_for_temporary_data,
        ctx.settings.compile_aggregate_expressions,
        ctx.settings.min_count_to_compile_aggregate_expression,
        ctx.settings.max_block_size,
        ctx.settings.enable_software_prefetch_in_aggregation,
        /* only_merge */ false,
        ctx.settings.optimize_group_by_constant_keys,
        ctx.settings.min_hit_rate_to_use_consecutive_keys_optimization,
        stats_collecting_params
    };

    SortDescription sort_description_for_merging;
    GroupingSetsParamsList grouping_sets_params;

    auto aggregating_step = std::make_unique<AggregatingStep>(
        ctx.input_headers.front(),
        std::move(params),
        std::move(grouping_sets_params),
        final,
        ctx.settings.max_block_size,
        ctx.settings.aggregation_in_order_max_block_bytes,
        0, //merge_threads,
        0, //temporary_data_merge_threads,
        false, // storage_has_evenly_distributed_read, TODO: later
        group_by_use_nulls,
        std::move(sort_description_for_merging),
        SortDescription{},
        ctx.settings.aggregation_in_order_memory_bound_merging,
        ctx.settings.aggregation_sort_result_by_bucket_number,
        false);

    return aggregating_step;
}

void registerAggregatingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Aggregating", AggregatingStep::deserialize);
}


}
