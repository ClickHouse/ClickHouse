#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// The steps between `DetachExtremesStep` and `AttachExtremesStep` do not process the extremes:
/// the extremes are taken out of the pipeline before them and put back after them unchanged.
/// Used for the steps of `additional_result_filter`, which, like `LIMIT`, does not apply to the extremes.
/// Since only the pipeline is affected, the plan between the steps is optimized as without the extremes.
class DetachExtremesStep : public ITransformingStep
{
public:
    explicit DetachExtremesStep(const SharedHeader & input_header_);

    String getName() const override { return "DetachExtremes"; }

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

class AttachExtremesStep : public ITransformingStep
{
public:
    explicit AttachExtremesStep(const SharedHeader & input_header_);

    String getName() const override { return "AttachExtremes"; }

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
