#include <Processors/QueryPlan/Streaming/AggregatingStep.h>

#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>
// #include <Processors/QueryPlan/AggregatorStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <memory>

namespace DB
{
namespace Streaming
{
namespace
{
ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

inline void convertToNullable(Block & header, const Names & keys)
{
    for (const auto & key : keys)
    {
        auto & column = header.getByName(key);

        column.type = makeNullableSafe(column.type);
        column.column = makeNullableSafe(column.column);
    }
}

Block appendGroupingSetColumn(Block header)
{
    Block res;
    res.insert({std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    for (auto & col : header)
        res.insert(std::move(col));

    return res;
}

Block generateOutputHeader(const Block & input_header, const Names & keys, bool use_nulls)
{
    auto header = appendGroupingSetColumn(input_header);
    if (use_nulls)
        convertToNullable(header, keys);
    return header;
}
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
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool group_by_use_nulls_,
    bool emit_version_)
    : ITransformingStep(input_stream_, appendGroupingColumn(params_.getHeader(input_stream_.header, final_), params_.keys, !grouping_sets_params_.empty(), group_by_use_nulls_), getTraits(), false)
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , group_by_use_nulls(group_by_use_nulls_)
    , emit_version(emit_version_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */


    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// For streaming aggregating, we don't need use StrictResizeProcessor.
        /// There is no case that some upstream closed, since AggregatingTransform required `watermark` of upstream to trigger finalize.
        // if (!storage_has_evenly_distributed_read)
        //     pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            // if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
            //     || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
            // {
            //     assert(transform_params->params.window_params);
            //     switch (transform_params->params.window_params->type)
            //     {
            //         case WindowType::TUMBLE:
            //             return std::make_shared<TumbleAggregatingTransform>(
            //                 header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            //         case WindowType::HOP:
            //             return std::make_shared<HopAggregatingTransform>(
            //                 header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            //         case WindowType::SESSION:
            //             throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel processing session window is not supported");
            //         default:
            //             throw Exception(
            //                 ErrorCodes::NOT_IMPLEMENTED,
            //                 "No support window type: {}",
            //                 magic_enum::enum_name(transform_params->params.window_params->type));
            //     }
            // }
            // else if (transform_params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
            //     return std::make_shared<UserDefinedEmitStrategyAggregatingTransform>(
            //         header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            // else
            auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), final, emit_version);
            return std::make_shared<GlobalAggregatingTransform>(
                header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            // if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
            //     || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
            // {
            //     assert(transform_params->params.window_params);
            //     switch (transform_params->params.window_params->type)
            //     {
            //         case WindowType::TUMBLE:
            //             return std::make_shared<TumbleAggregatingTransform>(header, transform_params);
            //         case WindowType::HOP:
            //             return std::make_shared<HopAggregatingTransform>(header, transform_params);
            //         case WindowType::SESSION:
            //             return std::make_shared<SessionAggregatingTransform>(header, transform_params);
            //         default:
            //             throw Exception(
            //                 ErrorCodes::NOT_IMPLEMENTED,
            //                 "No support window type: {}",
            //                 magic_enum::enum_name(transform_params->params.window_params->type));
            //     }
            // }
            // else if (transform_params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
            //     return std::make_shared<UserDefinedEmitStrategyAggregatingTransform>(header, transform_params);
            // else
            auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), final, emit_version);
            return std::make_shared<GlobalAggregatingTransform>(header, transform_params);
        });
    }

    aggregating = collector.detachProcessors(0);
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

void AggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        appendGroupingColumn(params.getHeader(input_streams.front().header, final), params.keys, !grouping_sets_params.empty(), group_by_use_nulls),
        getDataStreamTraits());
}

}
}
