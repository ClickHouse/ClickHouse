#include <IO/Operators.h>
/// #include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/Streaming/SortingStep.h>
/// #include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }};
}

namespace Streaming
{
SortingStep::SortingStep(
    const DataStream & input_stream,
    SortDescription description_,
    size_t max_block_size_,
    UInt64 limit_,
    SizeLimits size_limits_,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_before_external_sort_,
    VolumePtr tmp_volume_,
    size_t min_free_disk_space_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , result_description(std::move(description_))
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
    output_stream->sort_scope = DataStream::SortScope::Global;
}

void SortingStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

void SortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
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

            auto tmp_disk_scope = std::make_shared<TemporaryDataOnDiskScope>(tmp_volume, limit);
            auto tmp_data_on_disk = std::make_unique<TemporaryDataOnDisk>(tmp_disk_scope);

            return std::make_shared<MergeSortingTransform>(
                header,
                result_sort_desc,
                max_block_size,
                limit_,
                increase_sort_description_compile_attempts_current,
                max_bytes_before_remerge / pipeline.getNumStreams(),
                remerge_lowered_memory_bytes_ratio,
                max_bytes_before_external_sort,
                std::move(tmp_data_on_disk),
                min_free_disk_space);
        });
}

void SortingStep::fullSort(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, const UInt64 limit_)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<PartialSortingTransform>(header, result_sort_desc, limit_);
    });

    /// FIXME, multiple stream partial sort and then merge sorted


/// proton: porting. TODO: remove this part?
#if 0
    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
    limits.size_limits = size_limits;

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<LimitsCheckingTransform>(header, limits);
    });

    mergeSorting(pipeline, result_sort_desc, limit_);

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(), pipeline.getNumStreams(), result_sort_desc, max_block_size, SortingQueueStrategy::Batch, limit_);

        pipeline.addTransform(std::move(transform));
    }
#endif
}

void SortingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
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
}
