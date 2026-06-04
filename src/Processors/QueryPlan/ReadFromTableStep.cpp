#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

ReadFromTableStep::ReadFromTableStep(
    SharedHeader header,
    String table_name_,
    TableExpressionModifiers table_expression_modifiers_,
    bool use_parallel_replicas_,
    PrewhereInfoPtr prewhere_info_)
    : ISourceStep(std::move(header))
    , table_name(std::move(table_name_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , use_parallel_replicas(use_parallel_replicas_)
    , prewhere_info(std::move(prewhere_info_))
{
}

void ReadFromTableStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implemented for ReadFromTableStep");
}

static void serializeRational(TableExpressionModifiers::Rational val, WriteBuffer & out)
{
    writeIntBinary(val.numerator, out);
    writeIntBinary(val.denominator, out);
}

static TableExpressionModifiers::Rational deserializeRational(ReadBuffer & in)
{
    TableExpressionModifiers::Rational val;
    readIntBinary(val.numerator, in);
    readIntBinary(val.denominator, in);
    return val;
}

/// Serialization flags for ReadFromTableStep.
static constexpr UInt8 FLAG_HAS_FINAL = 1;
static constexpr UInt8 FLAG_HAS_SAMPLE_SIZE = 2;
static constexpr UInt8 FLAG_HAS_SAMPLE_OFFSET = 4;
static constexpr UInt8 FLAG_PARALLEL_REPLICAS = 8;
static constexpr UInt8 FLAG_HAS_PREWHERE = 16;

void ReadFromTableStep::serialize(Serialization & ctx) const
{
    writeStringBinary(table_name, ctx.out);

    UInt8 flags = 0;
    if (table_expression_modifiers.hasFinal())
        flags |= FLAG_HAS_FINAL;
    if (table_expression_modifiers.hasSampleSizeRatio())
        flags |= FLAG_HAS_SAMPLE_SIZE;
    if (table_expression_modifiers.hasSampleOffsetRatio())
        flags |= FLAG_HAS_SAMPLE_OFFSET;
    if (use_parallel_replicas)
        flags |= FLAG_PARALLEL_REPLICAS;
    if (prewhere_info && ctx.version >= QUERY_PLAN_CACHE_SERIALIZATION_VERSION)
        flags |= FLAG_HAS_PREWHERE;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (ctx.version == 0 && use_parallel_replicas)
        writeIntBinary(use_parallel_replicas, ctx.out);

    if (prewhere_info && ctx.version >= QUERY_PLAN_CACHE_SERIALIZATION_VERSION)
        prewhere_info->serialize(ctx);
}

QueryPlanStepPtr ReadFromTableStep::deserialize(Deserialization & ctx)
{
    String table_name;
    readStringBinary(table_name, ctx.in);

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool has_final = (flags & FLAG_HAS_FINAL) != 0;

    std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
    std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;

    if (flags & FLAG_HAS_SAMPLE_SIZE)
        sample_size_ratio = deserializeRational(ctx.in);

    if (flags & FLAG_HAS_SAMPLE_OFFSET)
        sample_offset_ratio = deserializeRational(ctx.in);

    bool use_parallel_replicas = (flags & FLAG_PARALLEL_REPLICAS) != 0;
    if (ctx.version == 0 && use_parallel_replicas)
    {
        UInt8 serialized_use_parallel_replicas = 0;
        readIntBinary(serialized_use_parallel_replicas, ctx.in);
        use_parallel_replicas = serialized_use_parallel_replicas != 0;
    }

    PrewhereInfoPtr prewhere_info;
    if (flags & FLAG_HAS_PREWHERE)
    {
        if (ctx.version < QUERY_PLAN_CACHE_SERIALIZATION_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected PREWHERE payload flag in ReadFromTableStep serialization version {}", ctx.version);
        prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));
    }

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableStep>(ctx.output_header, table_name, table_expression_modifiers, use_parallel_replicas, prewhere_info);
}

QueryPlanStepPtr ReadFromTableStep::clone() const
{
    return std::make_unique<ReadFromTableStep>(getOutputHeader(), table_name, table_expression_modifiers, use_parallel_replicas, prewhere_info);
}

void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTable", &ReadFromTableStep::deserialize);
}

}
