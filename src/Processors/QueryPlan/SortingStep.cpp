#include <stdexcept>
#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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
    , type(Type::Auto)
    , result_description(description_)
    , max_block_size(max_block_size_)
    , limit(limit_)
    , size_limits(size_limits_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_)
    , tmp_volume(tmp_volume_)
    , min_free_disk_space(min_free_disk_space_)
{
    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

SortingStep::SortingStep(
    const DataStream & input_stream_,
    const SortDescription & prefix_description_,
    const SortDescription & result_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , type(Type::FinishSorting)
    , prefix_description(prefix_description_)
    , result_description(result_description_)
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// TODO: check input_stream is sorted by prefix_description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

SortingStep::SortingStep(
    const DataStream & input_stream,
    const SortDescription & sort_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , type(Type::MergingSorted)
    , result_description(sort_description_)
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// TODO: check input_stream is partially sorted (each port) by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

void SortingStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
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

void SortingStep::finishSorting(
    QueryPipelineBuilder & pipeline, const SortDescription & input_sort_desc, const SortDescription & result_sort_desc, const UInt64 limit_)
{
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, result_sort_desc, limit_);
        });

    bool increase_sort_description_compile_attempts = true;

    /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
    pipeline.addSimpleTransform(
        [&, increase_sort_description_compile_attempts](const Block & header) mutable -> ProcessorPtr
        {
            /** For multiple FinishSortingTransform we need to count identical comparators only once per QueryPlan
                  * To property support min_count_to_compile_sort_description.
                  */
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            return std::make_shared<FinishSortingTransform>(
                header, input_sort_desc, result_sort_desc, max_block_size, limit_, increase_sort_description_compile_attempts_current);
        });
}

void SortingStep::mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, const UInt64 limit_)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            result_sort_desc,
            max_block_size,
            SortingQueueStrategy::Batch,
            limit_);

        pipeline.addTransform(std::move(transform));
    }
}

void SortingStep::mergeSorting(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_)
{
    bool increase_sort_description_compile_attempts = true;

    pipeline.addSimpleTransform(
        [&, increase_sort_description_compile_attempts](
            const Block & header, QueryPipelineBuilder::StreamType stream_type) mutable -> ProcessorPtr
        {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return nullptr;

            // For multiple FinishSortingTransform we need to count identical comparators only once per QueryPlan.
            // To property support min_count_to_compile_sort_description.
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            return std::make_shared<MergeSortingTransform>(
                header,
                result_sort_desc,
                max_block_size,
                limit_,
                increase_sort_description_compile_attempts_current,
                max_bytes_before_remerge / pipeline.getNumStreams(),
                remerge_lowered_memory_bytes_ratio,
                max_bytes_before_external_sort,
                tmp_volume,
                min_free_disk_space);
        });
}

void SortingStep::fullSort(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, const UInt64 limit_, const bool skip_partial_sort)
{
    if (!skip_partial_sort)
    {
        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<PartialSortingTransform>(header, result_sort_desc, limit_);
            });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
        limits.size_limits = size_limits;

        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitsCheckingTransform>(header, limits);
            });
    }

    mergeSorting(pipeline, result_sort_desc, limit_);

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(), pipeline.getNumStreams(), result_sort_desc, max_block_size, SortingQueueStrategy::Batch, limit_);

        pipeline.addTransform(std::move(transform));
    }
}

static Poco::Logger * getLogger()
{
    static Poco::Logger & logger = Poco::Logger::get("SortingStep");
    return &logger;
}

void SortingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto input_sort_mode = input_streams.front().sort_mode;
    const SortDescription & input_sort_desc = input_streams.front().sort_description;

    // dump sorting info
    LOG_DEBUG(getLogger(), "Sort type : {}", type);
    LOG_DEBUG(getLogger(), "Sort mode : {}", input_sort_mode);
    LOG_DEBUG(getLogger(), "Input ({}): {}", input_sort_desc.size(), dumpSortDescription(input_sort_desc));
    LOG_DEBUG(getLogger(), "Prefix({}): {}", prefix_description.size(), dumpSortDescription(prefix_description));
    LOG_DEBUG(getLogger(), "Result({}): {}", result_description.size(), dumpSortDescription(result_description));

    if (input_sort_mode == DataStream::SortMode::Stream && input_sort_desc.hasPrefix(result_description))
        return;

    /// merge sorted
    if (input_sort_mode == DataStream::SortMode::Port && input_sort_desc.hasPrefix(result_description))
    {
        LOG_DEBUG(getLogger(), "MergingSorted, SortMode::Port");
        mergingSorted(pipeline, result_description, limit);
        return;
    }

    if (type == Type::MergingSorted)
    {
        mergingSorted(pipeline, result_description, limit);
        return;
    }

    if (type == Type::FinishSorting)
    {
        bool need_finish_sorting = (prefix_description.size() < result_description.size());
        mergingSorted(pipeline, prefix_description, (need_finish_sorting ? 0 : limit));
        if (need_finish_sorting)
        {
            finishSorting(pipeline, prefix_description, result_description, limit);
        }
        return;
    }

    if (input_sort_mode == DataStream::SortMode::Chunk)
    {
        if (input_sort_desc.hasPrefix(result_description))
        {
            LOG_DEBUG(getLogger(), "Almost FullSort");
            fullSort(pipeline, result_description, limit, true);
            return;
        }
        if (result_description.hasPrefix(input_sort_desc))
        {
            LOG_DEBUG(getLogger(), "FinishSorting, SortMode::Chunk");
            mergeSorting(pipeline, input_sort_desc, 0);
            mergingSorted(pipeline, input_sort_desc, 0);
            finishSorting(pipeline, input_sort_desc, result_description, limit);
            return;
        }
    }

    LOG_DEBUG(getLogger(), "FullSort");
    fullSort(pipeline, result_description, limit);
}

void SortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    if (!prefix_description.empty())
    {
        settings.out << prefix << "Prefix sort description: ";
        dumpSortDescription(prefix_description, settings.out);
        settings.out << '\n';

        settings.out << prefix << "Result sort description: ";
        dumpSortDescription(result_description, settings.out);
        settings.out << '\n';
    }
    else
    {
        settings.out << prefix << "Sort description: ";
        dumpSortDescription(result_description, settings.out);
        settings.out << '\n';
    }

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void SortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!prefix_description.empty())
    {
        map.add("Prefix Sort Description", explainSortDescription(prefix_description));
        map.add("Result Sort Description", explainSortDescription(result_description));
    }
    else
        map.add("Sort Description", explainSortDescription(result_description));

    if (limit)
        map.add("Limit", limit);
}

}
