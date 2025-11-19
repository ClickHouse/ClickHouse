#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Processors/FractionalLimitTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

FractionalLimitStep::FractionalLimitStep(
    const SharedHeader & input_header_,
    Float64 limit_fraction_,
    Float64 offset_fraction_,
    UInt64 offset_,
    bool with_ties_,
    SortDescription description_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , limit_fraction(limit_fraction_)
    , offset_fraction(offset_fraction_)
    , offset(offset_)
    , with_ties(with_ties_)
    , description(std::move(description_))
{
}

void FractionalLimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<FractionalLimitTransform>(
        pipeline.getSharedHeader(), limit_fraction, offset_fraction, offset, pipeline.getNumStreams(), with_ties, description);

    pipeline.addTransform(std::move(transform));
}

void FractionalLimitStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Fractional Limit " << limit_fraction << '\n';
    settings.out << prefix << "Fractional Offset " << offset_fraction << '\n';

    if (with_ties)
        settings.out << prefix << "WITH TIES" << '\n';
}

void FractionalLimitStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Fractional Limit", limit_fraction);
    map.add("Fractional Offset", offset_fraction);
    map.add("With Ties", with_ties);
}

void FractionalLimitStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (with_ties)
        flags |= 1;

    writeIntBinary(flags, ctx.out);

    writeFloatBinary(limit_fraction, ctx.out);
    writeFloatBinary(offset_fraction, ctx.out);
    writeVarUInt(offset, ctx.out);

    if (with_ties)
        serializeSortDescription(description, ctx.out);
}

std::unique_ptr<IQueryPlanStep> FractionalLimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags;
    readIntBinary(flags, ctx.in);
    bool with_ties = bool(flags & 1);

    Float64 limit_fraction;
    Float64 offset_fraction;
    UInt64 offset;

    readFloatBinary(limit_fraction, ctx.in);
    readFloatBinary(offset_fraction, ctx.in);
    readVarUInt(offset, ctx.in);

    SortDescription description;
    if (with_ties)
        deserializeSortDescription(description, ctx.in);

    return std::make_unique<FractionalLimitStep>(
        ctx.input_headers.front(), limit_fraction, offset_fraction, offset, with_ties, std::move(description));
}

void registerFractionalLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("FractionalLimit", FractionalLimitStep::deserialize);
}

}
