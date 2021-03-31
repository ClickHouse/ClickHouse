#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Reverse rows in chunk.
class ReverseRowsStep : public ITransformingStep
{
public:
    explicit ReverseRowsStep(const DataStream & input_stream_);

    String getName() const override { return "ReverseRows"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
};

}
