#include <Processors/QueryPlan/LimitStep.h>
#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/DistributedTopKCandidateGateTransform.h>
#include <Core/Defines.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
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

    /// The distributed Top-K candidate gate sends the sort key columns to the initiator.
    if (distributed_top_k_candidate_sort_description)
    {
        const auto & header = *input_headers.front();
        std::unordered_set<size_t> sort_key_positions;
        for (const auto & column : *distributed_top_k_candidate_sort_description)
            sort_key_positions.insert(header.getPositionByName(column.column_name));

        std::erase_if(unneeded, [&](size_t position) { return sort_key_positions.contains(position); });
    }

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

void LimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
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

    if (distributed_top_k_candidate_sort_description)
    {
        if (pipeline.getNumStreams() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed TopK candidate gate requires a single input stream");

        pipeline.addTransform(std::make_shared<DistributedTopKCandidateGateTransform>(
            pipeline.getSharedHeader(),
            limit,
            *distributed_top_k_candidate_sort_description,
            settings.query_coordination_callback));
    }
}

void LimitStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Limit " << limit << '\n';
    settings.out << prefix << "Offset " << offset << '\n';
    if (distributed_top_k_candidate_sort_description)
        settings.out << prefix << "Distributed TopK candidate limit\n";

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
    map.add("Distributed TopK Candidate Limit", distributed_top_k_candidate_sort_description.has_value());
}

void LimitStep::serialize(Serialization & ctx) const
{
    if (distributed_top_k_candidate_sort_description
        && (!is_shard_limit || limit == 0 || offset != 0 || always_read_till_end || with_ties
            || distributed_top_k_candidate_sort_description->empty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid distributed Top-K candidate limit");

    /// Version 1 of the step carries the shard-limit and distributed Top-K candidate markers. The
    /// candidate marker cannot be dropped for an older peer: it would return all its local rows as
    /// final ones instead of taking part in the candidate exchange.
    if (distributed_top_k_candidate_sort_description && ctx.step_version < 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot serialize a distributed Top-K candidate limit for a peer with query plan serialization version {}",
            ctx.version);

    UInt8 flags = 0;
    if (always_read_till_end)
        flags |= 1;
    if (with_ties)
        flags |= 2;
    if (ctx.step_version >= 1 && is_shard_limit)
        flags |= 4;
    if (ctx.step_version >= 1 && distributed_top_k_candidate_sort_description)
        flags |= 8;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(limit, ctx.out);
    writeVarUInt(offset, ctx.out);

    if (with_ties)
        serializeSortDescription(description, ctx.out, ctx.version, ctx.for_cache_key, ctx.input_header);
    if (ctx.step_version >= 1 && distributed_top_k_candidate_sort_description)
        serializeSortDescription(
            *distributed_top_k_candidate_sort_description, ctx.out, ctx.version, ctx.for_cache_key, ctx.input_header);
}

QueryPlanStepPtr LimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    const UInt8 known_flags = ctx.step_version >= 1 ? 0x0F : 0x03;
    if (flags & ~known_flags)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown LimitStep flags: {}", static_cast<UInt64>(flags));

    bool always_read_till_end = bool(flags & 1);
    bool with_ties = bool(flags & 2);
    bool is_shard_limit = ctx.step_version >= 1 && bool(flags & 4);
    bool is_distributed_top_k_candidate_limit
        = ctx.step_version >= 1 && bool(flags & 8);

    UInt64 limit = 0;
    UInt64 offset = 0;

    readVarUInt(limit, ctx.in);
    readVarUInt(offset, ctx.in);

    SortDescription description;
    if (with_ties)
        deserializeSortDescription(description, ctx.in, ctx.version, ctx.max_type_complexity);

    SortDescription distributed_top_k_sort_description;
    if (is_distributed_top_k_candidate_limit)
    {
        deserializeSortDescription(distributed_top_k_sort_description, ctx.in, ctx.version, ctx.max_type_complexity);
        if (!is_shard_limit || limit == 0 || offset != 0 || always_read_till_end || with_ties
            || distributed_top_k_sort_description.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid distributed Top-K candidate limit");
    }

    auto step = std::make_unique<LimitStep>(
        ctx.input_headers.front(), limit, offset, always_read_till_end, with_ties, std::move(description));
    if (is_shard_limit)
        step->markAsShardLimit();
    if (is_distributed_top_k_candidate_limit)
        step->setDistributedTopKCandidateLimit(std::move(distributed_top_k_sort_description));
    return step;
}

QueryPlanStepPtr LimitStep::clone() const
{
    return std::make_unique<LimitStep>(*this);
}

void registerLimitStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry)
{
    const QueryPlanStepRegistry::StepVersions versions{{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT}};
    registry.registerStep("Limit", LimitStep::deserialize, versions);
}

}
