#include <Processors/QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

MergeSortingStep::MergeSortingStep(
    const DataStream & input_stream,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    size_t max_bytes_before_remerge_,
    size_t max_bytes_before_external_sort_,
    VolumePtr tmp_volume_,
    size_t min_free_disk_space_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_volume(tmp_volume_)
    , min_free_disk_space(min_free_disk_space_)
{
    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = description;
    output_stream->sort_mode = input_stream.has_single_port ? DataStream::SortMode::Stream
                                                            : DataStream::SortMode::Port;
}

void MergeSortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = limit == 0;
    }
}

void MergeSortingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        return std::make_shared<MergeSortingTransform>(
                header, description, max_merged_block_size, limit,
                max_bytes_before_remerge / pipeline.getNumStreams(),
                max_bytes_before_external_sort,
                tmp_volume,
                min_free_disk_space);
    });
}

void MergeSortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(description, input_streams.front().header, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

}
