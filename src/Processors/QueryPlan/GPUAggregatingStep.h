#pragma once

#include <GPU/GPUTypes.h>
#include "config.h"

#if USE_GPU

#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <optional>
#include <vector>

namespace DB
{

std::optional<std::vector<GPU::GPUAggregationKind>> gpuAggregationsOf(const Aggregator::Params & params);

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

    /// Tells the step that its input is one row per group already - the read of compressed
    /// blocks grouped every part on the device - so that it passes the rows on in its own header
    /// instead of grouping them again.
    void setInputGrouped() { input_grouped = true; }

private:
    void updateOutputHeader() override;

    Aggregator::Params params;

    size_t batch_bytes;
    bool input_grouped = false;
};

}

#endif
