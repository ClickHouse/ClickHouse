#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

LimitStep::LimitStep(
    const Header & input_header_,
    size_t limit_, size_t offset_,
    bool always_read_till_end_,
    bool with_ties_,
    SortDescription description_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{
}

void LimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<LimitTransform>(
        pipeline.getHeader(), limit, offset, pipeline.getNumStreams(), always_read_till_end, with_ties, description);

    pipeline.addTransform(std::move(transform));
}

void LimitStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Limit " << limit << '\n';
    settings.out << prefix << "Offset " << offset << '\n';

    if (with_ties || always_read_till_end)
    {
        settings.out << prefix;

        if (with_ties)
            settings.out << "WITH TIES";

        if (always_read_till_end)
        {
            if (!with_ties)
                settings.out << ", ";

            settings.out << "Reads all data";
        }

        settings.out << '\n';
    }
}

void LimitStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Limit", limit);
    map.add("Offset", offset);
    map.add("With Ties", with_ties);
    map.add("Reads All Data", always_read_till_end);
}

void LimitStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (always_read_till_end)
        flags |= 1;
    if (with_ties)
        flags |= 2;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(limit, ctx.out);
    writeVarUInt(offset, ctx.out);

    if (with_ties)
        serializeSortDescription(description, ctx.out);
}

std::unique_ptr<IQueryPlanStep> LimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool always_read_till_end = bool(flags & 1);
    bool with_ties = bool(flags & 2);

    UInt64 limit;
    UInt64 offset;

    readVarUInt(limit, ctx.in);
    readVarUInt(offset, ctx.in);

    SortDescription description;
    if (with_ties)
        deserializeSortDescription(description, ctx.in);

    return std::make_unique<LimitStep>(ctx.input_headers.front(), limit, offset, always_read_till_end, with_ties, std::move(description));
}

void registerLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Limit", LimitStep::deserialize);
}

}
