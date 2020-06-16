#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Merges/MergingSortedTransform.h>

namespace DB
{

MergingSortedStep::MergingSortedStep(
    const DataStream & input_stream,
    SortDescription sort_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream, input_stream)
    , sort_description(std::move(sort_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
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

}
