#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
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
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
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

        if (in_order)
            return std::make_shared<NegativeLimitBySortedStreamTransform>(header, group_length, group_offset, columns);

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

void NegativeLimitByStep::serialize(Serialization & ctx) const
{
    writeVarUInt(group_length, ctx.out);
    writeVarUInt(group_offset, ctx.out);

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr NegativeLimitByStep::deserialize(Deserialization & ctx)
{
    UInt64 group_length;
    UInt64 group_offset;

    readVarUInt(group_length, ctx.in);
    readVarUInt(group_offset, ctx.in);

    UInt64 num_columns;
    readVarUInt(num_columns, ctx.in);
    Names columns(num_columns);
    for (auto & column : columns)
        readStringBinary(column, ctx.in);

    return std::make_unique<NegativeLimitByStep>(ctx.input_headers.front(), group_length, group_offset, std::move(columns));
}

void NegativeLimitByStep::applyOrder(SortDescription sort_description)
{
    in_order = sort_description.hasPrefix(columns);
}

void registerNegativeLimitByStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("NegativeLimitBy", NegativeLimitByStep::deserialize);
}

}
