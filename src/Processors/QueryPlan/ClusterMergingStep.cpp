#include <Processors/QueryPlan/ClusterMergingStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
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
    Names cluster_key_names_,
    Float64 cluster_distance_,
    size_t dimensions_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(params_->getHeader()), getTraits())
    , params(std::move(params_))
    , cluster_key_names(std::move(cluster_key_names_))
    , cluster_distance(cluster_distance_)
    , dimensions(dimensions_)
{
}

void ClusterMergingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ClusterMergingTransform>(header, params, cluster_key_names, cluster_distance, dimensions);
    });
}

static String formatKeyNames(const Names & names)
{
    String result;
    for (size_t i = 0; i < names.size(); ++i)
    {
        if (i)
            result += ", ";
        result += names[i];
    }
    return result;
}

void ClusterMergingStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Cluster key: " << formatKeyNames(cluster_key_names) << '\n';
    settings.out << prefix << "Cluster distance: " << cluster_distance << '\n';
    settings.out << prefix << "Cluster dimensions: " << dimensions << '\n';
}

void ClusterMergingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Cluster key", formatKeyNames(cluster_key_names));
    map.add("Cluster distance", cluster_distance);
    map.add("Cluster dimensions", dimensions);
}

void ClusterMergingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params->getHeader());
}

}
