#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <base/BFloat16.h>
#include <base/types.h>

namespace DB
{

/// Executes LIMIT 0.1 - 0.9, See FractionalLimitTransform.
class FractionalLimitStep : public ITransformingStep
{
public:
    FractionalLimitStep(
        const SharedHeader & input_header_,
        BFloat16 limit_fraction_, 
        BFloat16 offset_fraction_,
        UInt64 offset = 0,
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "FractionalLimit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    BFloat16 getLimitFraction() const { return limit_fraction; }

    bool withTies() const { return with_ties; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    BFloat16 limit_fraction; 
    BFloat16 offset_fraction;

    UInt64 offset;

    bool with_ties;
    const SortDescription description;
};

}
