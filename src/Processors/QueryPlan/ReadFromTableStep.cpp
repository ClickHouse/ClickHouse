#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

ReadFromTableStep::ReadFromTableStep(
    SharedHeader header,
    String table_name_,
    TableExpressionModifiers table_expression_modifiers_,
    String merge_child_table_set_key_,
    bool use_parallel_replicas_)
    : ISourceStep(std::move(header))
    , table_name(std::move(table_name_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , merge_child_table_set_key(std::move(merge_child_table_set_key_))
    , use_parallel_replicas(use_parallel_replicas_)
{
}

void ReadFromTableStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implementad for ReadFromTableStep");
}

void ReadFromTableStep::serialize(Serialization & ctx) const
{
    if (!merge_child_table_set_key.empty()
        && ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_MERGE_CHILD_TABLE_SET_KEY)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a `Merge` table read requires query plan serialization version >= {}; all nodes must run the same version",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_MERGE_CHILD_TABLE_SET_KEY);

    writeStringBinary(table_name, ctx.out);

    UInt8 flags = 0;
    if (table_expression_modifiers.hasFinal())
        flags |= 1;
    if (table_expression_modifiers.hasSampleSizeRatio())
        flags |= 2;
    if (table_expression_modifiers.hasSampleOffsetRatio())
        flags |= 4;
    if (use_parallel_replicas)
        flags |= 8;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (use_parallel_replicas)
        writeIntBinary(use_parallel_replicas, ctx.out);

    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_MERGE_CHILD_TABLE_SET_KEY)
        writeStringBinary(merge_child_table_set_key, ctx.out);
}

QueryPlanStepPtr ReadFromTableStep::deserialize(Deserialization & ctx)
{
    String table_name;
    readStringBinary(table_name, ctx.in);

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool has_final = false;
    std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
    std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;

    if (flags & 1)
        has_final = true;

    if (flags & 2)
        sample_size_ratio = deserializeRational(ctx.in);

    if (flags & 4)
        sample_offset_ratio = deserializeRational(ctx.in);

    char use_parallel_replicas = 0;
    if (flags & 8)
        readIntBinary(use_parallel_replicas, ctx.in);

    String merge_child_table_set_key;
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_MERGE_CHILD_TABLE_SET_KEY)
        readStringBinary(merge_child_table_set_key, ctx.in);

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableStep>(
        ctx.output_header, table_name, table_expression_modifiers, std::move(merge_child_table_set_key), use_parallel_replicas);
}

QueryPlanStepPtr ReadFromTableStep::clone() const
{
    return std::make_unique<ReadFromTableStep>(
        getOutputHeader(), table_name, table_expression_modifiers, merge_child_table_set_key, use_parallel_replicas);
}

void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTable", &ReadFromTableStep::deserialize);
}

}
