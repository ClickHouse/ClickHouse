#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
namespace DB
{

/// Discard the totals and extremes streams of a subplan whose result feeds another pipeline.
/// A JOIN propagates totals from either input, so a subplan's totals would otherwise become the
/// enclosing query's totals. No-op when the pipeline has neither stream.
class DropTotalsAndExtremesStep : public ITransformingStep
{
public:
    explicit DropTotalsAndExtremesStep(const SharedHeader & input_header_);

    String getName() const override { return "DropTotalsAndExtremes"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }
};

}
