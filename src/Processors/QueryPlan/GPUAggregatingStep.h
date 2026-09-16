#pragma once

#include "config.h"

#if USE_GPU

#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class GPUAggregatingStep : public ITransformingStep
{
public:
    GPUAggregatingStep(const SharedHeader & input_header_, Aggregator::Params params_, size_t batch_bytes_);

    String getName() const override { return "GPUAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

    static bool canRunOnDevice(const Block & input_header, const Aggregator::Params & params);

    const Aggregator::Params & getParams() const { return params; }

private:
    void updateOutputHeader() override;

    Aggregator::Params params;

    size_t batch_bytes;
};

}

#endif
