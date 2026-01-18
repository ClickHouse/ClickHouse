#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ArrayJoinTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ArrayJoinAction.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsBool enable_lazy_columns_replication;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ArrayJoinStep::ArrayJoinStep(const SharedHeader & input_header_, ArrayJoin array_join_, bool is_unaligned_, size_t max_block_size_, bool enable_lazy_columns_replication_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(ArrayJoinTransform::transformHeader(*input_header_, array_join_.columns)),
        getTraits())
    , array_join(std::move(array_join_))
    , is_unaligned(is_unaligned_)
    , max_block_size(max_block_size_)
    , enable_lazy_columns_replication(enable_lazy_columns_replication_)
{
}

void ArrayJoinStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(ArrayJoinTransform::transformHeader(*input_headers.front(), array_join.columns));
}

void ArrayJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto array_join_actions = std::make_shared<ArrayJoinAction>(array_join.columns, array_join.is_left, is_unaligned, max_block_size, enable_lazy_columns_replication);
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, array_join_actions, on_totals);
    });
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    settings.out << prefix << (array_join.is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join.columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;


        settings.out << column;
    }
    settings.out << '\n';
}

void ArrayJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Left", array_join.is_left);

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : array_join.columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

void ArrayJoinStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
}

void ArrayJoinStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (array_join.is_left)
        flags |= 1;
    if (is_unaligned)
        flags |= 2;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(array_join.columns.size(), ctx.out);
    for (const auto & column : array_join.columns)
        writeStringBinary(column, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ArrayJoinStep::deserialize(Deserialization & ctx)
{
    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool is_left = bool(flags & 1);
    bool is_unaligned = bool(flags & 2);

    UInt64 num_columns;
    readVarUInt(num_columns, ctx.in);

    ArrayJoin array_join;
    array_join.is_left = is_left;
    array_join.columns.resize(num_columns);

    for (auto & column : array_join.columns)
        readStringBinary(column, ctx.in);

    return std::make_unique<ArrayJoinStep>(
        ctx.input_headers.front(),
        std::move(array_join),
        is_unaligned,
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::enable_lazy_columns_replication]);
}

void registerArrayJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ArrayJoin", ArrayJoinStep::deserialize);
}

}
