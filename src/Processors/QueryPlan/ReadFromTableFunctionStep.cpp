#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
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

ReadFromTableFunctionStep::ReadFromTableFunctionStep(
    SharedHeader header,
    std::string serialized_ast_,
    TableExpressionModifiers table_expression_modifiers_,
    bool use_parallel_replicas_)
    : ISourceStep(std::move(header))
    , serialized_ast(std::move(serialized_ast_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , use_parallel_replicas(use_parallel_replicas_)
{
}

void ReadFromTableFunctionStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implementad for ReadFromTableFunctionStep");
}

enum class TableFunctionSerializationKind : UInt8
{
    AST = 0,
};

void ReadFromTableFunctionStep::serialize(Serialization & ctx) const
{
    /// A peer below this version does not know the parallel-replicas flag bit: it would ignore the
    /// bit, leave the trailing byte unread and misparse the rest of the plan stream. Fail closed
    /// rather than write bytes an older peer cannot understand.
    if (use_parallel_replicas && ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_TABLE_FUNCTION_PARALLEL_REPLICAS)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a parallel-replicas read from a table function requires query plan serialization "
            "version >= {}; all nodes must run the same version",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_TABLE_FUNCTION_PARALLEL_REPLICAS);

    writeIntBinary(TableFunctionSerializationKind::AST, ctx.out);

    writeStringBinary(serialized_ast, ctx.out);

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
}

QueryPlanStepPtr ReadFromTableFunctionStep::deserialize(Deserialization & ctx)
{
    UInt8 kind = 0;
    readIntBinary(kind, ctx.in);

    if (kind != UInt8(TableFunctionSerializationKind::AST))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization kind {} is not implemented for ReadFromTableFunctionStep", int(kind));

    String serialized_ast;
    readStringBinary(serialized_ast, ctx.in);

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
    {
        /// Mirrors the guard in `serialize`: a peer below this version never legitimately writes the
        /// parallel-replicas flag bit, so a set bit in an older stream is a sign of stream corruption.
        if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_TABLE_FUNCTION_PARALLEL_REPLICAS)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Deserializing a parallel-replicas read from a table function requires query plan serialization "
                "version >= {}; all nodes must run the same version",
                DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_TABLE_FUNCTION_PARALLEL_REPLICAS);

        readIntBinary(use_parallel_replicas, ctx.in);
    }

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableFunctionStep>(
        ctx.output_header, std::move(serialized_ast), table_expression_modifiers, use_parallel_replicas);
}

void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry);
void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTableFunction", &ReadFromTableFunctionStep::deserialize);
}

}
