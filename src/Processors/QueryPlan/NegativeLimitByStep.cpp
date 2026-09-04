#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/Transforms/NegativeLimitByTransform.h>
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
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

NegativeLimitByStep::NegativeLimitByStep(
    const SharedHeader & input_header_,
    size_t group_length_, size_t group_offset_, Names columns_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , group_length(group_length_)
    , group_offset(group_offset_)
    , columns(std::move(columns_))
{
}


void NegativeLimitByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        if (!sorted_columns_descr.empty())
            return std::make_shared<NegativeLimitBySortedStreamTransform>(header, group_length, group_offset, sorted_columns_descr);

        return std::make_shared<NegativeLimitByTransform>(header, group_length, group_offset, columns);
    });
}

void NegativeLimitByStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    settings.out << prefix << "Columns: ";

    if (columns.empty())
        settings.out << "none\n";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column, settings.pretty_names) : column);
        }
        settings.out << '\n';
    }

    settings.out << prefix << "Negative Length " << group_length << '\n';
    settings.out << prefix << "Negative Offset " << group_offset << '\n';
}

void NegativeLimitByStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    map.add("Negative Length", group_length);
    map.add("Negative Offset", group_offset);
}

namespace
{

constexpr auto NEGATIVE_LIMIT_BY_MANIFEST = StepManifest<NegativeLimitByStep, NegativeLimitByWire>("NegativeLimitBy")
    .nameIntroducedIn(1)
    .baseFormat(
        field("group_length", WireFieldClass::Logical, &NegativeLimitByWire::group_length),
        field("group_offset", WireFieldClass::Logical, &NegativeLimitByWire::group_offset),
        field("columns", WireFieldClass::Logical, &NegativeLimitByWire::columns),
        field("sorted_columns_descr", WireFieldClass::Logical, &NegativeLimitByWire::sorted_columns_descr));

}

NegativeLimitByWire NegativeLimitByStep::toWire() const
{
    return NegativeLimitByWire{group_length, group_offset, columns, sorted_columns_descr};
}

QueryPlanStepPtr NegativeLimitByStep::fromWire(NegativeLimitByWire wire, Deserialization & ctx)
{
    auto step = std::make_unique<NegativeLimitByStep>(ctx.input_headers.front(), wire.group_length, wire.group_offset, std::move(wire.columns));
    if (!wire.sorted_columns_descr.empty())
        step->applyOrder(wire.sorted_columns_descr);
    return step;
}

void NegativeLimitByStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(NEGATIVE_LIMIT_BY_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr NegativeLimitByStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(NEGATIVE_LIMIT_BY_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void NegativeLimitByStep::serializeLegacy(Serialization & ctx) const
{
    writeVarUInt(group_length, ctx.out);
    writeVarUInt(group_offset, ctx.out);

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr NegativeLimitByStep::deserializeLegacy(Deserialization & ctx)
{
    UInt64 group_length = 0;
    UInt64 group_offset = 0;

    readVarUInt(group_length, ctx.in);
    readVarUInt(group_offset, ctx.in);

    UInt64 num_columns = 0;
    readVarUInt(num_columns, ctx.in);
    Names columns(num_columns);
    for (auto & column : columns)
        readStringBinary(column, ctx.in);

    return std::make_unique<NegativeLimitByStep>(ctx.input_headers.front(), group_length, group_offset, std::move(columns));
}

void NegativeLimitByStep::applyOrder(const SortDescription & sort_description)
{
    sorted_columns_descr = sort_description;
}

void registerNegativeLimitByStep(QueryPlanStepRegistry & registry);
void registerNegativeLimitByStep(QueryPlanStepRegistry & registry)
{
    registerManifest<NEGATIVE_LIMIT_BY_MANIFEST>(registry, NegativeLimitByStep::deserialize);
}

}
