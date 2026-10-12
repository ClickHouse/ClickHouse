#include <Processors/Transforms/EmptySetShortCircuitTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

EmptySetShortCircuitTransform::EmptySetShortCircuitTransform(SharedHeader header_, std::vector<FutureSetPtr> sets_)
    : ISimpleTransform(header_, header_, false)
    , sets(std::move(sets_))
{
}

IProcessor::Status EmptySetShortCircuitTransform::prepare()
{
    if (!checked && output.isNeeded())
    {
        checked = true;
        if (hasBuiltEmptySet(sets))
        {
            input.close();
            output.finish();
            return Status::Finished;
        }
    }
    return ISimpleTransform::prepare();
}

void addEmptySetShortCircuit(QueryPipelineBuilder & pipeline, const std::vector<FutureSetPtr> & sets)
{
    if (sets.empty())
        return;

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        return std::make_shared<EmptySetShortCircuitTransform>(header, sets);
    });
}

}
