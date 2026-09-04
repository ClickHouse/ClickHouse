#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepIdentity.h>
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
        serializeSortDescription(description, ctx.out);
}

namespace
{
/// Full digest tags for `LimitStep`. Unique within the step; never reused.
enum LimitStepIdentityTag : UInt64
{
    IS_SHARD_LIMIT_TAG = 1,
};
}

void LimitStep::writeFullDigest(StepDigestWriter & writer) const
{
    /// Unguarded: no DAG and no `NonZeroUInt64` plan setting, so neither wire method can throw for
    /// any instance (`isSerializable()` is unconditionally true).
    writer.addStepWireEncoding(*this);

    /// Not on the wire (`markAsShardLimit` sets it after construction).
    /// `QueryPipeline::initRowsBeforeLimit` special-cases a shard limit, so the rows it discards
    /// still count toward the parent limit's `rows_before_limit_at_least`, a user-visible field.
    writer.addBool(IS_SHARD_LIMIT_TAG, is_shard_limit);
}

namespace
{
/// Logical digest tags for `LimitStep`. Own enum, unique within this writer; never reused.
enum LimitStepLogicalDigestTag : UInt64
{
    LOGICAL_LIMIT_TAG = 1,
    LOGICAL_OFFSET_TAG = 2,
    LOGICAL_WITH_TIES_TAG = 3,
    LOGICAL_TIES_DESCRIPTION_TAG = 4,
    LOGICAL_ALWAYS_READ_TILL_END_TAG = 5,
    LOGICAL_IS_SHARD_LIMIT_TAG = 6,
};
}

void LimitStep::writeLogicalDigest(StepDigestWriter & writer) const
{
    /// The window of rows that survives.
    writer.addVarUInt(LOGICAL_LIMIT_TAG, limit);
    writer.addVarUInt(LOGICAL_OFFSET_TAG, offset);

    /// `with_ties` extends the window past `limit` by the rows equal to the last one under
    /// `description`, so both are relation-defining.
    writer.addBool(LOGICAL_WITH_TIES_TAG, with_ties);
    writer.addSortDescription(LOGICAL_TIES_DESCRIPTION_TAG, description);

    /// Both change user-visible output beyond this step's own rows, so both are in: with
    /// `always_read_till_end` the input keeps running after the limit is reached, which is what
    /// makes `totals` see every row, and `QueryPipeline::initRowsBeforeLimit` special-cases a shard
    /// limit so its discarded rows still count into the parent limit's `rows_before_limit_at_least`.
    /// `is_shard_limit` is also the stage marker of a split limit (plan section 4.2).
    writer.addBool(LOGICAL_ALWAYS_READ_TILL_END_TAG, always_read_till_end);
    writer.addBool(LOGICAL_IS_SHARD_LIMIT_TAG, is_shard_limit);
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
    registry.registerStep("Limit", LimitStep::deserialize);
}

}
