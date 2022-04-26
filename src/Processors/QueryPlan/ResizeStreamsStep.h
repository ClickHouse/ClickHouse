#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class ResizeStreamsStep : public ITransformingStep
{
public:
    explicit ResizeStreamsStep(const DataStream & input_stream_, size_t pipeline_streams_);
    String getName() const override { return "ResizeStreamsStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
private:
    size_t pipeline_streams;
};
}
