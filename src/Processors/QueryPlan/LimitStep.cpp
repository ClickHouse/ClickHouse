#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Core/Defines.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <unordered_set>

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

IQueryPlanStep::UnneededInputPositions LimitStep::getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const
{
    auto unneeded = unneeded_output_positions;
    if (with_ties)
    {
        /// `LimitTransform` compares the first column of each name in `description`, so only that copy stays.
        const auto & header = *input_headers.front();
        std::unordered_set<size_t> compared_positions;
        for (const auto & column : description)
            compared_positions.insert(header.getPositionByName(column.column_name));

        std::erase_if(unneeded, [&](size_t position) { return compared_positions.contains(position); });
    }

    return {std::move(unneeded)};
}

IQueryPlanStep::RemoveUnusedColumnsResult
LimitStep::removeUnusedColumns(const std::vector<size_t> & /*unneeded_output_positions*/, const std::vector<PrunedInput> & inputs)
{
    const auto & pruned = inputs.at(0);

    RemoveUnusedColumnsResult result;
    result.dropped_output_positions = pruned.dropped_positions;
    result.step_changed = !blocksHaveEqualStructure(*input_headers.front(), *pruned.header);

    if (result.step_changed)
        updateInputHeader(pruned.header, 0);

    return result;
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

void LimitStep::serialize(Serialization & ctx) const
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
        serializeSortDescription(description, ctx.out, ctx.version);
}

QueryPlanStepPtr LimitStep::deserialize(Deserialization & ctx)
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
        deserializeSortDescription(description, ctx.in, ctx.version, ctx.max_type_complexity);

    return std::make_unique<LimitStep>(ctx.input_headers.front(), limit, offset, always_read_till_end, with_ties, std::move(description));
}

QueryPlanStepPtr LimitStep::clone() const
{
    return std::make_unique<LimitStep>(*this);
}

void registerLimitStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Limit", LimitStep::deserialize);
}

}
