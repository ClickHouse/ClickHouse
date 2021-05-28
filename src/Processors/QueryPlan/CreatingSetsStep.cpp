#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

CreatingSetsStep::CreatingSetsStep(
    const DataStream & input_stream_,
    SubqueriesForSets subqueries_for_sets_,
    SizeLimits network_transfer_limits_,
    const Context & context_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , subqueries_for_sets(std::move(subqueries_for_sets_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , context(context_)
{
}

void CreatingSetsStep::transformPipeline(QueryPipeline & pipeline)
{
    auto creating_sets = std::make_shared<CreatingSetsTransform>(
            pipeline.getHeader(), subqueries_for_sets,
            network_transfer_limits,
            context);

    pipeline.addCreatingSetsTransform(std::move(creating_sets));
}

void CreatingSetsStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & set : subqueries_for_sets)
    {
        settings.out << prefix;
        if (set.second.set)
            settings.out << "Set: ";
        else if (set.second.join)
            settings.out << "Join: ";

        settings.out << set.first << '\n';
    }
}

}
