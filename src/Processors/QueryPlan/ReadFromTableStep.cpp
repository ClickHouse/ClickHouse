#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <vector>

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
    PrewhereInfoPtr prewhere_info_,
    FilterDAGInfoPtr row_level_filter_,
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column_)
    : ISourceStep(std::move(header))
    , table_name(std::move(table_name_))
    , table_expression_modifiers(std::move(table_expression_modifiers_))
    , use_parallel_replicas(use_parallel_replicas_)
    , prewhere_info(std::move(prewhere_info_))
    , row_level_filter(std::move(row_level_filter_))
    , node_name_to_input_node_column(std::move(node_name_to_input_node_column_))
{
}

void ReadFromTableStep::initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "initializePipeline is not implemented for ReadFromTableStep");
}

/// `serializeRational` / `deserializeRational` are the shared helpers declared in
/// `Analyzer/TableExpressionModifiers.h` (transitively included via `ReadFromTableStep.h`).

/// Serialization flags for ReadFromTableStep.
static constexpr UInt8 FLAG_HAS_FINAL = 1 << 0;
static constexpr UInt8 FLAG_HAS_SAMPLE_SIZE = 1 << 1;
static constexpr UInt8 FLAG_HAS_SAMPLE_OFFSET = 1 << 2;
static constexpr UInt8 FLAG_PARALLEL_REPLICAS = 1 << 3;
static constexpr UInt8 FLAG_HAS_PREWHERE = 1 << 4;
static constexpr UInt8 FLAG_HAS_ROW_LEVEL_FILTER = 1 << 5;
static constexpr UInt8 FLAG_HAS_NODE_NAME_TO_INPUT_COLUMN = 1 << 6;
static constexpr UInt64 READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION = 1;
static constexpr UInt64 READ_FROM_TABLE_NODE_NAME_MAPPING_SERIALIZATION_VERSION = 2;

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
    if (prewhere_info && ctx.version >= READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
        flags |= FLAG_HAS_PREWHERE;
    if (row_level_filter && ctx.version >= READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
        flags |= FLAG_HAS_ROW_LEVEL_FILTER;
    if (!node_name_to_input_node_column.empty() && ctx.version >= READ_FROM_TABLE_NODE_NAME_MAPPING_SERIALIZATION_VERSION)
        flags |= FLAG_HAS_NODE_NAME_TO_INPUT_COLUMN;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers.hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers.getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers.hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers.getSampleOffsetRatio(), ctx.out);

    if (ctx.version == 0 && use_parallel_replicas)
        writeIntBinary(use_parallel_replicas, ctx.out);

    if (prewhere_info && ctx.version >= READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
        prewhere_info->serialize(ctx);

    if (row_level_filter && ctx.version >= READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
        row_level_filter->serialize(ctx);

    if (!node_name_to_input_node_column.empty() && ctx.version >= READ_FROM_TABLE_NODE_NAME_MAPPING_SERIALIZATION_VERSION)
    {
        writeVarUInt(node_name_to_input_node_column.size(), ctx.out);
        std::vector<const std::pair<const std::string, ColumnWithTypeAndName> *> sorted_columns;
        sorted_columns.reserve(node_name_to_input_node_column.size());
        for (const auto & column : node_name_to_input_node_column)
            sorted_columns.push_back(&column);

        std::ranges::sort(sorted_columns, {}, [](const auto * column) { return column->first; });
        for (const auto * column_with_node_name : sorted_columns)
        {
            const auto & [node_name, column] = *column_with_node_name;
            writeStringBinary(node_name, ctx.out);
            writeStringBinary(column.name, ctx.out);
            encodeDataType(column.type, ctx.out);
        }
    }
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
        if (ctx.version < READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected PREWHERE payload flag in ReadFromTableStep serialization version {}", ctx.version);
        prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));
    }

    FilterDAGInfoPtr row_level_filter;
    if (flags & FLAG_HAS_ROW_LEVEL_FILTER)
    {
        if (ctx.version < READ_FROM_TABLE_FILTERS_SERIALIZATION_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected row-level filter payload flag in ReadFromTableStep serialization version {}", ctx.version);
        row_level_filter = std::make_shared<FilterDAGInfo>(FilterDAGInfo::deserialize(ctx));
    }

    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (flags & FLAG_HAS_NODE_NAME_TO_INPUT_COLUMN)
    {
        if (ctx.version < READ_FROM_TABLE_NODE_NAME_MAPPING_SERIALIZATION_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected node-name mapping payload flag in ReadFromTableStep serialization version {}", ctx.version);

        UInt64 size = 0;
        readVarUInt(size, ctx.in);
        node_name_to_input_node_column.reserve(size);
        for (UInt64 i = 0; i < size; ++i)
        {
            String node_name;
            String column_name;
            readStringBinary(node_name, ctx.in);
            readStringBinary(column_name, ctx.in);
            auto type = decodeDataType(ctx.in, ctx.max_type_complexity);
            node_name_to_input_node_column.emplace(std::move(node_name), ColumnWithTypeAndName(nullptr, type, column_name));
        }
    }

    TableExpressionModifiers table_expression_modifiers(has_final, sample_size_ratio, sample_offset_ratio);
    return std::make_unique<ReadFromTableStep>(
        ctx.output_header,
        table_name,
        table_expression_modifiers,
        use_parallel_replicas,
        prewhere_info,
        row_level_filter,
        std::move(node_name_to_input_node_column));
}

QueryPlanStepPtr ReadFromTableStep::clone() const
{
    return std::make_unique<ReadFromTableStep>(
        getOutputHeader(),
        table_name,
        table_expression_modifiers,
        use_parallel_replicas,
        prewhere_info,
        row_level_filter,
        node_name_to_input_node_column);
}

void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromTable", &ReadFromTableStep::deserialize);
}

}
