#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ReadFromTableStep::ReadFromTableStep(
    SharedHeader header,
    String table_name_,
    TableExpressionModifiers table_expression_modifiers_,
    bool use_parallel_replicas_)
    : ISourceStep(std::move(header))
    , table_name(std::move(table_name_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , use_parallel_replicas(use_parallel_replicas_)
{
}

void ReadFromTableStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implementad for ReadFromTableStep");
}

void ReadFromTableStep::serialize(Serialization & ctx) const
{
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
    const bool write_column_identifiers = ctx.step_version >= 1 && !column_identifiers.empty();
    if (write_column_identifiers)
        flags |= 16;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (use_parallel_replicas)
        writeIntBinary(use_parallel_replicas, ctx.out);

    if (write_column_identifiers)
        serializeColumnIdentifiers(column_identifiers, ctx.out);
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

    NameToNameVector column_identifiers;
    if (flags & 16)
        column_identifiers = deserializeColumnIdentifiers(ctx.in);

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    auto step = std::make_unique<ReadFromTableStep>(ctx.output_header, table_name, table_expression_modifiers, use_parallel_replicas);
    step->setColumnIdentifiers(std::move(column_identifiers));
    return step;
}

QueryPlanStepPtr ReadFromTableStep::clone() const
{
    auto step = std::make_unique<ReadFromTableStep>(getOutputHeader(), table_name, table_expression_modifiers, use_parallel_replicas);
    step->setColumnIdentifiers(column_identifiers);
    return step;
}

void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry)
{
    /// Version 1 carries the column identifiers of the read.
    registry.registerStep(
        "ReadFromTable",
        &ReadFromTableStep::deserialize,
        {{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_INDEX_HINT_ACTIONS}});
}

}
