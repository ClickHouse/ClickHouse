#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Convert one block structure to another. See ConvertingTransform.
class MaterializingStep : public ITransformingStep
{
public:
    explicit MaterializingStep(const DataStream & input_stream_);

    String getName() const override { return "Materializing"; }

    void transformPipeline(QueryPipeline & pipeline) override;
};

}
