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
}

ReadFromTableFunctionStep::ReadFromTableFunctionStep(
    Block header,
    std::string serialized_ast_,
    TableExpressionModifiers table_expression_modifiers_)
    : ISourceStep(std::move(header))
    , serialized_ast(std::move(serialized_ast_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
{
}

void ReadFromTableFunctionStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implementad for ReadFromTableFunctionStep");
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

enum class TableFunctionSerializationKind : UInt8
{
    AST = 0,
};

void ReadFromTableFunctionStep::serialize(Serialization & ctx) const
{
    writeIntBinary(TableFunctionSerializationKind::AST, ctx.out);

    writeStringBinary(serialized_ast, ctx.out);

    UInt8 flags = 0;
    if (table_expression_modifiers.hasFinal())
        flags |= 1;
    if (table_expression_modifiers.hasSampleSizeRatio())
        flags |= 2;
    if (table_expression_modifiers.hasSampleOffsetRatio())
        flags |= 4;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);
}

std::unique_ptr<IQueryPlanStep> ReadFromTableFunctionStep::deserialize(Deserialization & ctx)
{
    UInt8 kind;
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

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableFunctionStep>(*ctx.output_header, std::move(serialized_ast), table_expression_modifiers);
}

void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTableFunction", &ReadFromTableFunctionStep::deserialize);
}

}
