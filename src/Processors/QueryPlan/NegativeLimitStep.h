#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes Negative LIMIT. See NegativeLimitTransform.
class NegativeLimitStep : public ITransformingStep
{
public:
    NegativeLimitStep(
        const SharedHeader & input_header_,
        UInt64 limit_, UInt64 offset_);

    String getName() const override { return "NegativeLimit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }

    UInt64 getLimitForSorting() const
    {
        return 0;
    }

    bool withTies() const { return false; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    UInt64 limit;
    UInt64 offset;
};

}
