#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Core/Streaming/CursorTree_fwd.h>

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

static void serializeStreamSettings(const TableExpressionModifiers::StreamSettings & stream_settings, WriteBuffer & out)
{
    UInt8 has_cursor = stream_settings.cursor_tree ? 1 : 0;
    writeIntBinary(has_cursor, out);

    if (!has_cursor)
        return;

    const auto cursor_map = cursorTreeToMap(stream_settings.cursor_tree);
    writeVarUInt(cursor_map.size(), out);
    for (const auto & entry : cursor_map)
    {
        const auto & tuple = entry.safeGet<Tuple>();
        writeStringBinary(tuple.at(0).safeGet<String>(), out);
        writeIntBinary(tuple.at(1).safeGet<Int64>(), out);
    }
}

static TableExpressionModifiers::StreamSettings deserializeStreamSettings(ReadBuffer & in)
{
    TableExpressionModifiers::StreamSettings stream_settings;

    UInt8 has_cursor = 0;
    readIntBinary(has_cursor, in);

    if (!has_cursor)
        return stream_settings;

    UInt64 size = 0;
    readVarUInt(size, in);

    Map cursor_map;
    cursor_map.reserve(size);
    for (UInt64 i = 0; i < size; ++i)
    {
        String path;
        Int64 value = 0;
        readStringBinary(path, in);
        readIntBinary(value, in);
        cursor_map.push_back(Tuple{path, value});
    }
    stream_settings.cursor_tree = buildCursorTree(cursor_map);
    return stream_settings;
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
    if (table_expression_modifiers.hasStream())
        flags |= 16;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (table_expression_modifiers.hasStream())
        serializeStreamSettings(*table_expression_modifiers.getStreamSettings(), ctx.out);
}

QueryPlanStepPtr ReadFromTableFunctionStep::deserialize(Deserialization & ctx)
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
    std::optional<TableExpressionModifiers::StreamSettings> stream_settings;

    if (flags & 1)
        has_final = true;

    if (flags & 2)
        sample_size_ratio = deserializeRational(ctx.in);

    if (flags & 4)
        sample_offset_ratio = deserializeRational(ctx.in);

    if (flags & 16)
        stream_settings = deserializeStreamSettings(ctx.in);

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio, std::move(stream_settings));
    return std::make_unique<ReadFromTableFunctionStep>(ctx.output_header, std::move(serialized_ast), table_expression_modifiers);
}

void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTableFunction", &ReadFromTableFunctionStep::deserialize);
}

}
