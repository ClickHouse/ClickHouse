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

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

    /// A fractional limit is resolved against the whole query result (the transform must see every row
    /// to know what the fraction is), so `apply_prelimit` never pushes it to the shard and it is
    /// evaluated on the initiator. Reporting support keeps `considerEnablingParallelReplicas` from
    /// rejecting the whole plan; `transformPipeline` still attaches a collector so that a boundary here
    /// would be measured rather than cached as `output_bytes = 0`.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    Float64 limit_fraction;
    Float64 offset_fraction;

    UInt64 offset;

    bool with_ties;
    const SortDescription description;
};

}
