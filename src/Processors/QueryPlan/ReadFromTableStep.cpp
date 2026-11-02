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
}

ReadFromTableStep::ReadFromTableStep(
    SharedHeader header,
    String table_name_,
    TableExpressionModifiers table_expression_modifiers_,
    bool use_parallel_replicas_,
    RowPolicyPlacement row_policy_placement_,
    std::optional<Names> selected_columns_,
    std::optional<size_t> columns_to_read_,
    std::optional<String> additional_filter_)
    : ISourceStep(std::move(header))
    , table_name(std::move(table_name_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , use_parallel_replicas(use_parallel_replicas_)
    , row_policy_placement(row_policy_placement_)
    , selected_columns(std::move(selected_columns_))
    , columns_to_read(columns_to_read_)
    , additional_filter(std::move(additional_filter_))
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
    if (ctx.step_version >= 1 && row_policy_placement == RowPolicyPlacement::FilterStep)
        flags |= 16;
    if (ctx.step_version >= 1 && selected_columns)
        flags |= 32;
    if (ctx.step_version >= 1 && additional_filter)
        flags |= 64;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (use_parallel_replicas)
        writeIntBinary(use_parallel_replicas, ctx.out);

    if (flags & 32)
    {
        writeVarUInt(selected_columns->size(), ctx.out);
        for (const auto & name : *selected_columns)
            writeStringBinary(name, ctx.out);
        writeVarUInt(columns_to_read.value_or(selected_columns->size()), ctx.out);
    }

    if (flags & 64)
        writeStringBinary(*additional_filter, ctx.out);
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

    auto row_policy_placement = RowPolicyPlacement::Unknown;
    std::optional<Names> selected_columns;
    std::optional<size_t> columns_to_read;
    std::optional<String> additional_filter;
    if (ctx.step_version >= 1)
    {
        row_policy_placement = (flags & 16) ? RowPolicyPlacement::FilterStep : RowPolicyPlacement::NotInPlan;
        if (flags & 32)
        {
            size_t size = 0;
            readVarUInt(size, ctx.in);
            auto & names = selected_columns.emplace();
            for (size_t i = 0; i < size; ++i)
                readStringBinary(names.emplace_back(), ctx.in);
            size_t count = 0;
            readVarUInt(count, ctx.in);
            columns_to_read = count;
        }
        if (flags & 64)
            readStringBinary(additional_filter.emplace(), ctx.in);
    }

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableStep>(
        ctx.output_header, table_name, table_expression_modifiers, use_parallel_replicas, row_policy_placement,
        std::move(selected_columns), columns_to_read, std::move(additional_filter));
}

QueryPlanStepPtr ReadFromTableStep::clone() const
{
    return std::make_unique<ReadFromTableStep>(
        getOutputHeader(), table_name, table_expression_modifiers, use_parallel_replicas, row_policy_placement, selected_columns,
        columns_to_read, additional_filter);
}

void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep(
        "ReadFromTable",
        &ReadFromTableStep::deserialize,
        {{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROW_POLICY_PLACEMENT}});
}

}
