#include <IO/Operators.h>
#include <Processors/NegativeLimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
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

/// Wire format and cross-version compatibility.
///
/// The serialized layout is `flags` (one byte), then `limit` and `offset` (varuint), then -
/// only when bit 0 of `flags` is set - a trailing `SortDescription` for `WITH TIES`.
/// The leading `flags` byte has been part of the format since negative `LIMIT` was introduced
/// (it was always written, reserved for exactly this kind of extension), so the byte stream is
/// self-describing: a reader that understands the `flags` byte always knows whether the trailing
/// `SortDescription` follows, and there is never an ambiguity that could desynchronize the stream.
///
/// This makes the change forward-compatible without bumping `DBMS_QUERY_PLAN_SERIALIZATION_VERSION`.
/// A server that predates `WITH TIES` reads the `flags` byte, sees a non-zero value, and rejects it
/// with `CORRUPTED_DATA` *before* reading `limit`/`offset` (see the guard below, which on the old
/// server reads `if (flags != 0)`). So a newer initiator that sends a `WITH TIES` negative-`LIMIT`
/// plan to such a server fails closed - the distributed query errors cleanly instead of
/// misinterpreting the trailing bytes. This is intentionally finer-grained than a version bump:
/// bumping the plan version would make the old server reject *every* serialized plan from the newer
/// initiator (including plain negative `LIMIT`), whereas the `flags` byte rejects only the plans
/// that genuinely use the unsupported feature.
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
    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool with_ties_v = bool(flags & 1);

    /// Reject any flag bit we do not understand before reading the rest of the payload, so that an
    /// unknown extension fails closed rather than desynchronizing the stream. See the comment above.
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

void registerNegativeLimitStep(QueryPlanStepRegistry & registry);
void registerNegativeLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("NegativeLimit", NegativeLimitStep::deserialize);
}

}
