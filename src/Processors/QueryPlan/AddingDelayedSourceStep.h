#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

/// Adds another source to pipeline. Data from this source will be read after data from all other sources.
/// NOTE: tis step is needed because of non-joined data from JOIN. Remove this step after adding JoinStep.
class AddingDelayedSourceStep : public ITransformingStep
{
public:
    AddingDelayedSourceStep(
            const DataStream & input_stream_,
            ProcessorPtr source_);

    String getName() const override { return "AddingDelayedSource"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ProcessorPtr source;
};

}
