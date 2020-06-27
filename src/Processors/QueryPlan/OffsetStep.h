#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class OffsetStep : public ITransformingStep
{
public:
    OffsetStep(const DataStream & input_stream_, size_t offset_);

    String getName() const override { return "Offset"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    size_t offset;
};

}
