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

NegativeLimitStep::NegativeLimitStep(const SharedHeader & input_header_, UInt64 limit_, UInt64 offset_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , limit(limit_)
    , offset(offset_)
{
}

void NegativeLimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<NegativeLimitTransform>(pipeline.getSharedHeader(), limit, offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void NegativeLimitStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Negative Limit " << limit << '\n';
    settings.out << prefix << "Negative Offset " << offset << '\n';
}

void NegativeLimitStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Negative Limit", limit);
    map.add("Negative Offset", offset);
}

void NegativeLimitStep::serialize(Serialization & ctx) const
{
    /// Keep a flags byte for future use; currently always 0 as we don't support extensions such as WITH TIES
    UInt8 flags = 0;
    writeIntBinary(flags, ctx.out);

    writeVarUInt(limit, ctx.out);
    writeVarUInt(offset, ctx.out);
}

std::unique_ptr<IQueryPlanStep> NegativeLimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags;
    readIntBinary(flags, ctx.in); // reserved, ignored for now

    if (flags != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "NegativeLimitStep: unsupported flags={} in this version", static_cast<size_t>(flags));

    UInt64 limit_v = 0;
    UInt64 offset_v = 0;

    readVarUInt(limit_v, ctx.in);
    readVarUInt(offset_v, ctx.in);

    return std::make_unique<NegativeLimitStep>(ctx.input_headers.front(), limit_v, offset_v);
}

void registerNegativeLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("NegativeLimit", NegativeLimitStep::deserialize);
}

}
