#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SubscriptionQueue.h>

namespace DB
{

class StreamingAdapterStep : public ITransformingStep
{
public:
    explicit StreamingAdapterStep(const DataStream & input_stream, Block sample, SubscriberPtr sub);

    String getName() const override { return "StreamingAdapter"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override
    {
        output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    }

    Block storage_sample;
    SubscriberPtr subscriber;
};

}
