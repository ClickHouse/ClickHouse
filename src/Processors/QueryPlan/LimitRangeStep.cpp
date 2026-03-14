#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitRangeTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

LimitRangeStep::LimitRangeStep(
    const SharedHeader & input_header_,
    std::optional<std::pair<ActionsDAG, String>> start_condition_,
    std::optional<std::pair<ActionsDAG, String>> end_condition_,
    std::optional<UInt64> limit_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , start_condition(std::move(start_condition_))
    , end_condition(std::move(end_condition_))
    , limit(limit_)
{
}

void LimitRangeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    ExpressionActionsPtr start_expression;
    String start_column_name;
    if (start_condition)
    {
        start_expression = std::make_shared<ExpressionActions>(
            start_condition->first.clone(), settings.getActionsSettings());
        start_column_name = start_condition->second;
    }

    ExpressionActionsPtr end_expression;
    String end_column_name;
    if (end_condition)
    {
        end_expression = std::make_shared<ExpressionActions>(
            end_condition->first.clone(), settings.getActionsSettings());
        end_column_name = end_condition->second;
    }

    pipeline.addSimpleTransform(
        [start_expression, start_column_name, end_expression, end_column_name, limit_value = limit]
        (const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<LimitRangeTransform>(
            header,
            start_expression,
            start_column_name,
            end_expression,
            end_column_name,
            limit_value);
    });
}

void LimitRangeStep::describeActions(FormatSettings & format_settings) const
{
    const String & prefix = format_settings.detail_prefix;
    format_settings.out << prefix << "LimitRange limit ";
    if (limit)
        format_settings.out << *limit;
    else
        format_settings.out << "none";
    if (start_condition)
        format_settings.out << " AFTER";
    if (end_condition)
        format_settings.out << " UNTIL";
    format_settings.out << '\n';
}

void LimitRangeStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (limit)
        map.add("Limit", *limit);
    else
        map.add("Limit", "none");
    map.add("Has After", start_condition.has_value());
    map.add("Has Until", end_condition.has_value());
}

void LimitRangeStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (start_condition)
        flags |= 1;
    if (end_condition)
        flags |= 2;
    if (limit)
        flags |= 4;

    writeIntBinary(flags, ctx.out);

    if (limit)
        writeVarUInt(*limit, ctx.out);

    auto write_condition = [&](const std::optional<std::pair<ActionsDAG, String>> & condition)
    {
        if (!condition)
            return;

        writeStringBinary(condition->second, ctx.out);
        condition->first.serialize(ctx.out, ctx.registry);
    };

    write_condition(start_condition);
    write_condition(end_condition);
}

QueryPlanStepPtr LimitRangeStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "LimitRangeStep must have one input stream");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    std::optional<UInt64> limit_value;
    if (flags & 4)
    {
        UInt64 limit;
        readVarUInt(limit, ctx.in);
        limit_value = limit;
    }

    auto read_condition = [&](bool present) -> std::optional<std::pair<ActionsDAG, String>>
    {
        if (!present)
            return std::nullopt;

        String column_name;
        readStringBinary(column_name, ctx.in);
        auto dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
        return std::make_pair(std::move(dag), std::move(column_name));
    };

    return std::make_unique<LimitRangeStep>(
        ctx.input_headers.front(),
        read_condition(flags & 1),
        read_condition(flags & 2),
        limit_value);
}

void registerLimitRangeStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("LimitRange", LimitRangeStep::deserialize);
}

}
