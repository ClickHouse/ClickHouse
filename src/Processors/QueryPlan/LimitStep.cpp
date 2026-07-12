#include <Processors/QueryPlan/LimitStep.h>
#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/DistributedTopKCandidateGateTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

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

void LimitStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
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

    UInt8 flags = 0;
    if (always_read_till_end)
        flags |= 1;
    if (with_ties)
        flags |= 2;
    if (ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT && is_shard_limit)
        flags |= 4;
    if (ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT && distributed_top_k_candidate_sort_description)
        flags |= 8;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(limit, ctx.out);
    writeVarUInt(offset, ctx.out);

    if (with_ties)
        serializeSortDescription(description, ctx.out);
    if (ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT && distributed_top_k_candidate_sort_description)
        serializeSortDescription(*distributed_top_k_candidate_sort_description, ctx.out);
}

QueryPlanStepPtr LimitStep::deserialize(Deserialization & ctx)
{
    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    const UInt8 known_flags = ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT ? 0x0F : 0x03;
    if (flags & ~known_flags)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown LimitStep flags: {}", static_cast<UInt64>(flags));

    bool always_read_till_end = bool(flags & 1);
    bool with_ties = bool(flags & 2);
    bool is_shard_limit = ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT && bool(flags & 4);
    bool is_distributed_top_k_candidate_limit
        = ctx.version >= DBMS_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SHARD_LIMIT && bool(flags & 8);

    UInt64 limit = 0;
    UInt64 offset = 0;

    readVarUInt(limit, ctx.in);
    readVarUInt(offset, ctx.in);

    SortDescription description;
    if (with_ties)
        deserializeSortDescription(description, ctx.in);

    SortDescription distributed_top_k_sort_description;
    if (is_distributed_top_k_candidate_limit)
    {
        deserializeSortDescription(distributed_top_k_sort_description, ctx.in);
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

void registerLimitStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Limit", LimitStep::deserialize);
}

}
