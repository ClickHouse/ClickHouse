#pragma once

#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// Pipeline step for `GROUP BY ... WITH CLUSTER`; runs `ClusterMergingTransform`.
class ClusterMergingStep : public ITransformingStep
{
public:
    ClusterMergingStep(
        SharedHeader input_header_,
        AggregatingTransformParamsPtr params_,
        Names cluster_key_names_,
        Float64 cluster_distance_,
        size_t dimensions_);

    String getName() const override { return "ClusterMerging"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    AggregatingTransformParamsPtr params;
    Names cluster_key_names;
    Float64 cluster_distance;
    size_t dimensions;
};

}
