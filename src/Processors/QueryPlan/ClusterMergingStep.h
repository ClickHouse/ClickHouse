#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Adds a ClusterMergingTransform to the pipeline that merges adjacent groups
/// within a specified distance for a GROUP BY WITH CLUSTER key.
class ClusterMergingStep : public ITransformingStep
{
public:
    ClusterMergingStep(
        SharedHeader input_header_,
        AggregatingTransformParamsPtr params_,
        String cluster_key_name_,
        Float64 cluster_distance_);

    String getName() const override { return "ClusterMerging"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    AggregatingTransformParamsPtr params;
    String cluster_key_name;
    Float64 cluster_distance;
};

}
