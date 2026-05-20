#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/Defines.h>
#include <limits>

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

LimitByStep::LimitByStep(
    const SharedHeader & input_header_,
    size_t group_length_, size_t group_offset_, Names columns_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , group_length(group_length_)
    , group_offset(group_offset_)
    , columns(std::move(columns_))
{
}


void LimitByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// Data is coming in order but in multiple streams. This can happen if the data is partitioned and we request inOrder reading.
    /// For example, if number of partitions is lower than allowed number of streams, then data is sent in multiple streams for each
    /// partition where each stream is ordered.
    if (in_order && pipeline.getNumStreams() > 1)
    {
        /// Do not apply OFFSET in pre-filter.
        const UInt64 prefilter_length = (group_offset > std::numeric_limits<UInt64>::max() - group_length)
            ? std::numeric_limits<UInt64>::max()
            : group_length + group_offset;

        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitByTransform>(header, prefilter_length, 0, true, columns);
            });

        /// For small `group_length` and `group_offset` values (which is the typical case), we have already filtered most of the data.
        /// So this step should be quite fast as well.
        auto merge = std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            sort_description,
            DEFAULT_BLOCK_SIZE,
            0,
            std::nullopt,
            SortingQueueStrategy::Batch);
        pipeline.addTransform(std::move(merge));

        /// Now we can apply the OFFSET
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitByTransform>(header, group_length, group_offset, true, columns);
            });
        return;
    }

    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<LimitByTransform>(header, group_length, group_offset, in_order, columns);
    });
}

void LimitByStep::describeActions(FormatSettings & settings) const
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

    settings.out << prefix << "Length " << group_length << '\n';
    settings.out << prefix << "Offset " << group_offset << '\n';
}

void LimitByStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    map.add("Length", group_length);
    map.add("Offset", group_offset);
}

void LimitByStep::serialize(Serialization & ctx) const
{
    writeVarUInt(group_length, ctx.out);
    writeVarUInt(group_offset, ctx.out);


    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr LimitByStep::deserialize(Deserialization & ctx)
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

    return std::make_unique<LimitByStep>(ctx.input_headers.front(), group_length, group_offset, std::move(columns));
}

void LimitByStep::applyOrder(SortDescription sort_desc)
{
    in_order = sort_desc.hasPrefix(columns);
    if (in_order)
        sort_description = std::move(sort_desc);
}

void registerLimitByStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("LimitBy", LimitByStep::deserialize);
}

}
