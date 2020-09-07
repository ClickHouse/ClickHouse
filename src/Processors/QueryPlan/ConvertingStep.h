#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ConvertingStep : public ITransformingStep
{
public:
    ConvertingStep(const DataStream & input_stream_, Block result_header_);

    String getName() const override { return "Converting"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    Block result_header;
};

}
