#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
namespace DB
{

class ExtremesStep : public ITransformingStep
{
public:
    ExtremesStep(const DataStream & input_stream_);

    String getName() const override { return "Extremes"; }

    void transformPipeline(QueryPipeline & pipeline) override;
};

}
