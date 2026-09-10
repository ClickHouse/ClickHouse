#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/StepManifest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ReadFromTableFunctionStep::ReadFromTableFunctionStep(
    SharedHeader header,
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

enum class TableFunctionSerializationKind : UInt8
{
    AST = 0,
};

namespace
{

constexpr auto READ_FROM_TABLE_FUNCTION_MANIFEST = StepManifest<ReadFromTableFunctionStep, ReadFromTableFunctionWire>("ReadFromTableFunction")
    .nameIntroducedIn(1)
    .baseFormat(
        field("serialized_ast", WireFieldClass::Logical, &ReadFromTableFunctionWire::serialized_ast),
        field("final", WireFieldClass::Logical, &ReadFromTableFunctionWire::final),
        field("sample_size_ratio", WireFieldClass::Logical, &ReadFromTableFunctionWire::sample_size_ratio),
        field("sample_offset_ratio", WireFieldClass::Logical, &ReadFromTableFunctionWire::sample_offset_ratio));

}

ReadFromTableFunctionWire ReadFromTableFunctionStep::toWire() const
{
    return ReadFromTableFunctionWire{
        serialized_ast,
        table_expression_modifiers.hasFinal(),
        table_expression_modifiers.getSampleSizeRatio(),
        table_expression_modifiers.getSampleOffsetRatio()};
}

QueryPlanStepPtr ReadFromTableFunctionStep::fromWire(ReadFromTableFunctionWire wire, Deserialization & ctx)
{
    TableExpressionModifiers modifiers(wire.final, wire.sample_size_ratio, wire.sample_offset_ratio);
    return std::make_unique<ReadFromTableFunctionStep>(ctx.output_header, std::move(wire.serialized_ast), modifiers);
}

void ReadFromTableFunctionStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(READ_FROM_TABLE_FUNCTION_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr ReadFromTableFunctionStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(READ_FROM_TABLE_FUNCTION_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void ReadFromTableFunctionStep::serializeLegacy(Serialization & ctx) const
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

QueryPlanStepPtr ReadFromTableFunctionStep::deserializeLegacy(Deserialization & ctx)
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

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableFunctionStep>(ctx.output_header, std::move(serialized_ast), table_expression_modifiers);
}

void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry);
void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry)
{
    registerManifest<READ_FROM_TABLE_FUNCTION_MANIFEST>(registry, ReadFromTableFunctionStep::deserialize);
}

}
