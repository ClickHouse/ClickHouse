#include <Processors/QueryPlan/TopologicalSortStep.h>
#include <Processors/Transforms/TopologicalSortTransform.h>
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
            .preserves_number_of_rows = true,
        }
    };
}

TopologicalSortStep::TopologicalSortStep(
    const SharedHeader & input_header,
    const String & key_column_name_,
    const String & deps_column_name_)
    : ITransformingStep(input_header, input_header, getTraits())
    , key_column_name(key_column_name_)
    , deps_column_name(deps_column_name_)
{
}

void TopologicalSortStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<TopologicalSortTransform>(header, key_column_name, deps_column_name);
    });
}

void TopologicalSortStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Key: " << key_column_name
                 << " DEPENDS ON " << deps_column_name << '\n';
}

void TopologicalSortStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Key", key_column_name);
    map.add("DependsOn", deps_column_name);
}

}
