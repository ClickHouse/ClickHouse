#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class OffsetsStep : public ITransformingStep
{
public:
    OffsetsStep(const DataStream & input_stream_, size_t offset_);

    String getName() const override { return "Offsets"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    size_t offset;
};

}
