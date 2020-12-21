#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Convert one block structure to another. See ConvertingTransform.
class ConvertingStep : public ITransformingStep
{
public:
    ConvertingStep(const DataStream & input_stream_, Block result_header_, bool ignore_constant_values_ = false);

    String getName() const override { return "Converting"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    Block result_header;
    /// Do not check that constants are same. Use value from result_header.
    bool ignore_constant_values;
};

}
