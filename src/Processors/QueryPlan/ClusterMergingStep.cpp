#include <Processors/QueryPlan/ClusterMergingStep.h>
#include <Processors/Transforms/ClusterMergingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/Port.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ClusterMergingStep::ClusterMergingStep(
    SharedHeader input_header_,
    AggregatingTransformParamsPtr params_,
    String cluster_key_name_,
    Float64 cluster_distance_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(params_->getHeader()), getTraits())
    , params(std::move(params_))
    , cluster_key_name(std::move(cluster_key_name_))
    , cluster_distance(cluster_distance_)
{
}

void ClusterMergingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ClusterMergingTransform>(header, params, cluster_key_name, cluster_distance);
    });
}

void ClusterMergingStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Cluster key: " << cluster_key_name << '\n';
    settings.out << prefix << "Cluster distance: " << cluster_distance << '\n';
}

void ClusterMergingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Cluster key", cluster_key_name);
    map.add("Cluster distance", cluster_distance);
}

void ClusterMergingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params->getHeader());
}

}
