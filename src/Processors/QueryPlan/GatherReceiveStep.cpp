#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/SortDescription.h>
#include <Core/Defines.h>

#include <optional>


namespace DB
{

void GatherReceiveStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    Pipes pipes;

    /// Read from all buckets
    for (size_t i = 0; i < num_buckets; ++i)
    {
        pipes.push_back(Pipe(settings.exchange_lookup->createSource(output_header, ExchangeStreamId(exchange_id, i, 0))));
    }

    pipeline.init(Pipe::unitePipes(std::move(pipes)));

    if (maintain_sort_description && pipeline.getNumStreams() > 1)
    {
        pipeline.addTransform(
            std::make_shared<MergingSortedTransform>(
                output_header,
                num_buckets,
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
}

void GatherReceiveStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(num_buckets, ctx.out);
    writeVarUInt(maintain_sort_description.has_value(), ctx.out);
    if (maintain_sort_description.has_value())
        serializeSortDescription(*maintain_sort_description, ctx.out);
}

std::unique_ptr<IQueryPlanStep> GatherReceiveStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    size_t num_buckets;
    readVarUInt(num_buckets, ctx.in);

    std::optional<SortDescription> maintain_sort_description;
    bool has_maintain_sort_description;
    readVarUInt(has_maintain_sort_description, ctx.in);
    if (has_maintain_sort_description)
    {
        maintain_sort_description.emplace();
        deserializeSortDescription(*maintain_sort_description, ctx.in);
    }

    return std::make_unique<GatherReceiveStep>(ctx.output_header, exchange_id, num_buckets, std::move(maintain_sort_description));
}

void registerGatherReceiveStep(QueryPlanStepRegistry & registry);
void registerGatherReceiveStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("GatherReceive", GatherReceiveStep::deserialize);
}

}
