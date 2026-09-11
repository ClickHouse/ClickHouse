#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Columns/IColumn.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/Defines.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

/// True when every key of `description` is constant in `header`, so all rows compare equal under it.
static bool sortDescriptionIsAllConstant(const SortDescription & description, const Block & header)
{
    for (const auto & column_description : description)
    {
        const auto * column = header.findByName(column_description.column_name);
        if (!column || !column->column || !isColumnConst(*column->column))
            return false;
    }

    return true;
}

QueryPipelineBuilderPtr GatherSendStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GatherSendStep expects single input step");

    auto & pipeline = *pipelines.front();

    const String bucket = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    /// Cannot have multiple sinks writing to the same file concurrently. Merge-sort rather than plain
    /// resize(1) when order must be preserved, since `GatherReceiveStep` merge-sorts assuming each bucket's
    /// stream already arrives sorted.
    /// An all-constant description orders nothing, so any interleaving satisfies it. The merge is skipped
    /// there because it waits for every input to have data, which cannot complete while the streams share
    /// one upstream producer blocked on a stream the merge is not reading.
    if (maintain_sort_description && pipeline.getNumStreams() > 1
        && !sortDescriptionIsAllConstant(*maintain_sort_description, *pipeline.getSharedHeader()))
    {
        pipeline.addTransform(
            std::make_shared<MergingSortedTransform>(
                pipeline.getSharedHeader(),
                pipeline.getNumStreams(),
                *maintain_sort_description,
                /* merge_block_size_rows */ DEFAULT_BLOCK_SIZE,
                /* merge_block_size_bytes */ 0,
                /* max_dynamic_subcolumns */ std::nullopt,
                SortingQueueStrategy::Batch,
                /* limit */ 0,
                /* always_read_till_end */ false,
                /* rows_sources_write_buf */ nullptr,
                /* filter_column_name */ std::nullopt,
                /* blocks_are_granules_size */ false));
    }
    else
    {
        pipeline.resize(1);
    }

    pipeline.setSinks([&](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        chassert(stream_type == Pipe::StreamType::Main);
        return settings.exchange_lookup->createSink(header, ExchangeStreamId(exchange_id, bucket, "0"), /*advisory*/ false);
    });

    return std::move(pipelines.front());
}

static constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SORTED_GATHER_SEND = 6;

void GatherSendStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);

    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SORTED_GATHER_SEND)
    {
        /// An older peer doesn't know to read a trailing sort description at all, so fail closed only when
        /// it would actually be lost; with no `maintain_sort_description` the bytes below are unchanged.
        if (maintain_sort_description)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "make_distributed_plan: serializing an order-preserving GatherSendStep requires query plan "
                "serialization version >= {}; all nodes must run the same version",
                DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SORTED_GATHER_SEND);
        return;
    }

    writeVarUInt(maintain_sort_description.has_value(), ctx.out);
    if (maintain_sort_description.has_value())
        serializeSortDescription(*maintain_sort_description, ctx.out);
}

std::unique_ptr<IQueryPlanStep> GatherSendStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    std::optional<SortDescription> maintain_sort_description;
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SORTED_GATHER_SEND)
    {
        bool has_maintain_sort_description = false;
        readVarUInt(has_maintain_sort_description, ctx.in);
        if (has_maintain_sort_description)
        {
            maintain_sort_description.emplace();
            deserializeSortDescription(*maintain_sort_description, ctx.in);
        }
    }

    return std::make_unique<GatherSendStep>(ctx.input_headers.front(), exchange_id, std::move(maintain_sort_description));
}

void registerGatherSendStep(QueryPlanStepRegistry & registry);
void registerGatherSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("GatherSend", GatherSendStep::deserialize);
}

}
