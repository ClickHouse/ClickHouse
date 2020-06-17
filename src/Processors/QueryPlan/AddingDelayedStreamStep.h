#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class AddingDelayedStreamStep : public ITransformingStep
{
public:
    AddingDelayedStreamStep(
            const DataStream & input_stream_,
            ProcessorPtr source_);

    String getName() const override { return "AddingDelayedStream"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ProcessorPtr source;
};

}
