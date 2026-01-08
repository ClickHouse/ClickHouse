#pragma once
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <base/types.h>

namespace DB
{

/// Executes Fractional LIMIT, See FractionalLimitTransform.
class FractionalLimitStep : public ITransformingStep
{
public:
    FractionalLimitStep(
        const SharedHeader & input_header_,
        Float64 limit_fraction_,
        Float64 offset_fraction_,
        UInt64 offset = 0,
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "FractionalLimit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    Float64 getLimitFraction() const { return limit_fraction; }

    bool withTies() const { return with_ties; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    Float64 limit_fraction;
    Float64 offset_fraction;

    UInt64 offset;

    bool with_ties;
    const SortDescription description;
};

}
