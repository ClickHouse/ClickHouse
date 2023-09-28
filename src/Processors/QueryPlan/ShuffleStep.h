#pragma once

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ShuffleStep : public ITransformingStep
{
public:
    ShuffleStep(const DataStream & input_stream_, size_t shuffle_optimize_buckets, size_t shuffle_optimize_max);

    String getName() const override { return "Shuffle"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override
    {
        output_stream = createOutputStream(input_streams.front(),
            input_streams.front().header,
            getDataStreamTraits()
        );
    }

    size_t shuffle_optimize_buckets;
    size_t shuffle_optimize_max;
};

}
