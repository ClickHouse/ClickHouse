#include <Processors/QueryPlan/SortingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

SortingStep::SortingStep(
    const DataStream & input_stream,
    const SortDescription & description_,
    size_t max_block_size_,
    UInt64 limit_,
    SizeLimits size_limits_,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_before_external_sort_,
    VolumePtr tmp_volume_,
    size_t min_free_disk_space_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , type(Type::Full)
    , result_description(description_)
    , max_block_size(max_block_size_)
    , limit(limit_)
    , size_limits(size_limits_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_volume(tmp_volume_)
    , min_free_disk_space(min_free_disk_space_)
{
    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

SortingStep::SortingStep(
    const DataStream & input_stream_,
    SortDescription prefix_description_,
    SortDescription result_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , type(Type::FinishSorting)
    , prefix_description(std::move(prefix_description_))
    , result_description(std::move(result_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// TODO: check input_stream is sorted by prefix_description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

SortingStep::SortingStep(
    const DataStream & input_stream,
    SortDescription sort_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , type(Type::MergingSorted)
    , result_description(std::move(sort_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// TODO: check input_stream is partially sorted (each port) by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

void SortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void SortingStep::convertToFinishSorting(SortDescription prefix_description_)
{
    type = Type::FinishSorting;
    prefix_description = std::move(prefix_description_);
}

void SortingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (type == Type::FinishSorting)
    {
        bool need_finish_sorting = (prefix_description.size() < result_description.size());
        if (pipeline.getNumStreams() > 1)
        {
            UInt64 limit_for_merging = (need_finish_sorting ? 0 : limit);
            auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    prefix_description,
                    max_block_size,
                    limit_for_merging);

            pipeline.addTransform(std::move(transform));
        }

        if (need_finish_sorting)
        {
            pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<PartialSortingTransform>(header, result_description, limit);
            });

            /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
            pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
            {
                return std::make_shared<FinishSortingTransform>(
                    header, prefix_description, result_description, max_block_size, limit);
            });
        }
    }
    else if (type == Type::Full)
    {
        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, result_description, limit);
        });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
        limits.size_limits = size_limits;

        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
            return transform;
        });

        pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return nullptr;

            return std::make_shared<MergeSortingTransform>(
                    header, result_description, max_block_size, limit,
                    max_bytes_before_remerge / pipeline.getNumStreams(),
                    remerge_lowered_memory_bytes_ratio,
                    max_bytes_before_external_sort,
                    tmp_volume,
                    min_free_disk_space);
        });

        /// If there are several streams, then we merge them into one
        if (pipeline.getNumStreams() > 1)
        {

            auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    result_description,
                    max_block_size, limit);

            pipeline.addTransform(std::move(transform));
        }
    }
    else if (type == Type::MergingSorted)
    {        /// If there are several streams, then we merge them into one
        if (pipeline.getNumStreams() > 1)
        {

            auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    result_description,
                    max_block_size, limit);

            pipeline.addTransform(std::move(transform));
        }
    }
}

void SortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    if (!prefix_description.empty())
    {
        settings.out << prefix << "Prefix sort description: ";
        dumpSortDescription(prefix_description, input_streams.front().header, settings.out);
        settings.out << '\n';

        settings.out << prefix << "Result sort description: ";
        dumpSortDescription(result_description, input_streams.front().header, settings.out);
        settings.out << '\n';
    }
    else
    {
        settings.out << prefix << "Sort description: ";
        dumpSortDescription(result_description, input_streams.front().header, settings.out);
        settings.out << '\n';
    }

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void SortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!prefix_description.empty())
    {
        map.add("Prefix Sort Description", explainSortDescription(prefix_description, input_streams.front().header));
        map.add("Result Sort Description", explainSortDescription(result_description, input_streams.front().header));
    }
    else
        map.add("Sort Description", explainSortDescription(result_description, input_streams.front().header));

    if (limit)
        map.add("Limit", limit);
}

}
