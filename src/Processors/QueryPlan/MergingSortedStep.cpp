#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Merges/MergingSortedTransform.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}

MergingSortedStep::MergingSortedStep(
    const DataStream & input_stream,
    SortDescription sort_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream, input_stream.header, getTraits())
    , sort_description(std::move(sort_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// Streams are merged together, only global distinct keys remain distinct.
    /// Note: we can not clear it if know that there will be only one stream in pipeline. Should we add info about it?
    output_stream->local_distinct_columns.clear();
}

void MergingSortedStep::transformPipeline(QueryPipeline & pipeline)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {

        auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                sort_description,
                max_block_size, limit);

        pipeline.addPipe({ std::move(transform) });

        pipeline.enableQuotaForCurrentStreams();
    }
}

Strings MergingSortedStep::describeActions() const
{
    Strings res = {"Sort description: " + dumpSortDescription(sort_description, input_streams.front().header)};

    if (limit)
        res.emplace_back("Limit " + std::to_string(limit));

    return res;
}

}
