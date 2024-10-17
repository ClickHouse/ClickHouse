#include <Common/JSONBuilder.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/ShuffleProcessor.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true
        },
        {
            .preserves_number_of_rows = true
        }
    };
}

ShuffleStep::ShuffleStep(const Header & input_header_, size_t shuffle_optimize_buckets_, size_t shuffle_optimize_max_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , shuffle_optimize_buckets(shuffle_optimize_buckets_)
    , shuffle_optimize_max(shuffle_optimize_max_)
{}

void ShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    auto shuffle = std::make_shared<ShuffleProcessor>(pipeline.getHeader(), shuffle_optimize_buckets, shuffle_optimize_max);
    pipeline.addTransform(std::move(shuffle));
}

void ShuffleStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "SHUFFLE BY ID, Buckets " << shuffle_optimize_buckets << ", Max Key " << shuffle_optimize_max;
    settings.out << '\n';
}

void ShuffleStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Name", "SHUFFLE BY default");
    map.add("Shuffle Optimization Buckets", shuffle_optimize_buckets);
    map.add("Shuffle Optimization Max Key", shuffle_optimize_max);
}

}
