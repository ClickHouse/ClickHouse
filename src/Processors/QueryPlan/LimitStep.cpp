#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Core/Defines.h>
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
    const SharedHeader & input_header_,
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
    /// WITH TIES compares adjacent rows under `description`, so it needs a single ordered
    /// stream. The input may arrive as several already-sorted streams (e.g. an in-order read
    /// over multiple parts) with no merge above, so merge them here first.
    if (with_ties && pipeline.getNumStreams() > 1)
    {
        auto merge = std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            description,
            DEFAULT_BLOCK_SIZE,
            /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt,
            SortingQueueStrategy::Batch);
        pipeline.addTransform(std::move(merge));
    }

    auto transform = std::make_shared<LimitTransform>(
        pipeline.getSharedHeader(),
        limit,
        offset,
        pipeline.getNumStreams(),
        always_read_till_end,
        with_ties,
        description,
        dataflow_cache_updater);
    if (is_shard_limit)
        transform->markAsShardLimit();
    pipeline.addTransform(std::move(transform));
}

void LimitStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
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

namespace
{

constexpr auto LIMIT_MANIFEST = StepManifest<LimitStep, LimitWire>("Limit")
    .nameIntroducedIn(1)
    .baseFormat(
        field("limit", WireFieldClass::Logical, &LimitWire::limit),
        field("offset", WireFieldClass::Logical, &LimitWire::offset),
        field("always_read_till_end", WireFieldClass::Logical, &LimitWire::always_read_till_end),
        field("with_ties", WireFieldClass::Logical, &LimitWire::with_ties),
        field("description", WireFieldClass::Logical, &LimitWire::description),
        field("is_shard_limit", WireFieldClass::Logical, &LimitWire::is_shard_limit));

}

LimitWire LimitStep::toWire() const
{
    return LimitWire{limit, offset, always_read_till_end, with_ties, description, is_shard_limit};
}

QueryPlanStepPtr LimitStep::fromWire(LimitWire wire, Deserialization & ctx)
{
    auto step = std::make_unique<LimitStep>(
        ctx.input_headers.front(), wire.limit, wire.offset, wire.always_read_till_end, wire.with_ties, std::move(wire.description));
    if (wire.is_shard_limit)
        step->markAsShardLimit();
    return step;
}

void LimitStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(LIMIT_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr LimitStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(LIMIT_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void LimitStep::serializeLegacy(Serialization & ctx) const
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

QueryPlanStepPtr LimitStep::deserializeLegacy(Deserialization & ctx)
{
    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool always_read_till_end = bool(flags & 1);
    bool with_ties = bool(flags & 2);

    UInt64 limit = 0;
    UInt64 offset = 0;

    readVarUInt(limit, ctx.in);
    readVarUInt(offset, ctx.in);

    SortDescription description;
    if (with_ties)
        deserializeSortDescription(description, ctx.in);

    return std::make_unique<LimitStep>(ctx.input_headers.front(), limit, offset, always_read_till_end, with_ties, std::move(description));
}

QueryPlanStepPtr LimitStep::clone() const
{
    return std::make_unique<LimitStep>(*this);
}

void registerLimitStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry)
{
    registerManifest<LIMIT_MANIFEST>(registry, LimitStep::deserialize);
}

}
