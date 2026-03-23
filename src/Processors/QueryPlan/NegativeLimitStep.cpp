#include <IO/Operators.h>
#include <Processors/NegativeLimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}

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

NegativeLimitStep::NegativeLimitStep(
    const SharedHeader & input_header_, UInt64 limit_, UInt64 offset_,
    bool with_ties_, SortDescription description_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , limit(limit_)
    , offset(offset_)
    , with_ties(with_ties_)
    , description(std::move(description_))
{
}

void NegativeLimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<NegativeLimitTransform>(
        pipeline.getSharedHeader(), limit, offset, pipeline.getNumStreams(), with_ties, description);

    pipeline.addTransform(std::move(transform));
}

void NegativeLimitStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Negative Limit " << limit << '\n';
    settings.out << prefix << "Negative Offset " << offset << '\n';

    if (with_ties)
        settings.out << prefix << "WITH TIES" << '\n';
}

void NegativeLimitStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Negative Limit", limit);
    map.add("Negative Offset", offset);
    map.add("With Ties", with_ties);
}

void NegativeLimitStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (with_ties)
        flags |= 1;
    writeIntBinary(flags, ctx.out);

    writeVarUInt(limit, ctx.out);
    writeVarUInt(offset, ctx.out);

    if (with_ties)
        serializeSortDescription(description, ctx.out);
}

QueryPlanStepPtr NegativeLimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool with_ties_v = bool(flags & 1);

    if (flags & ~UInt8(1))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "NegativeLimitStep: unsupported flags={} in this version", static_cast<size_t>(flags));

    UInt64 limit_v = 0;
    UInt64 offset_v = 0;

    readVarUInt(limit_v, ctx.in);
    readVarUInt(offset_v, ctx.in);

    SortDescription sort_description;
    if (with_ties_v)
        deserializeSortDescription(sort_description, ctx.in);

    return std::make_unique<NegativeLimitStep>(ctx.input_headers.front(), limit_v, offset_v, with_ties_v, std::move(sort_description));
}

void registerNegativeLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("NegativeLimit", NegativeLimitStep::deserialize);
}

}
