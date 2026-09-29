#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitRangeTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/ProtocolDefines.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            /// The transform keeps the row order of its single stream, but `transformPipeline` first
            /// resizes several input streams into one without merging them, so a per-stream order is
            /// lost. A global order survives the step; `applyOrder` in the plan optimizer keeps it.
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

LimitRangeStep::LimitRangeStep(
    const SharedHeader & input_header_,
    ActionsDAG conditions_,
    std::optional<String> start_column_name_,
    std::optional<String> end_column_name_,
    bool start_all_,
    std::optional<UInt64> limit_,
    bool always_read_till_end_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , conditions(std::move(conditions_))
    , start_column_name(std::move(start_column_name_))
    , end_column_name(std::move(end_column_name_))
    , start_all(start_all_)
    , limit(limit_)
    , always_read_till_end(always_read_till_end_)
{
}

LimitRangeStep::LimitRangeStep(const LimitRangeStep & other)
    : ITransformingStep(other)
    , conditions(other.conditions.clone())
    , start_column_name(other.start_column_name)
    , end_column_name(other.end_column_name)
    , start_all(other.start_all)
    , limit(other.limit)
    , always_read_till_end(other.always_read_till_end)
{
}

void LimitRangeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<LimitRangeTransform>(
            header,
            conditions,
            start_column_name,
            end_column_name,
            settings.getActionsSettings(),
            start_all,
            limit,
            always_read_till_end);
    });
}

void LimitRangeStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    if (limit)
        settings.out << prefix << "Limit " << *limit << '\n';

    auto describe_column = [&](const std::optional<String> & column_name, const char * title, const char * suffix)
    {
        if (!column_name)
            return;

        settings.out << prefix << title
                     << (settings.pretty ? QueryPlanFormat::formatColumnPretty(*column_name, settings.pretty_names) : *column_name)
                     << suffix << '\n';
    };

    describe_column(start_column_name, "After column: ", start_all ? " (all)" : "");
    describe_column(end_column_name, "Until column: ", "");

    if (!settings.compact && (start_column_name || end_column_name))
        ExpressionActions(conditions.clone()).describeActions(settings.out, prefix);

    if (always_read_till_end)
        settings.out << prefix << "Reads all data: 1\n";
}

void LimitRangeStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (limit)
        map.add("Limit", *limit);

    if (start_column_name)
    {
        map.add("After Column", *start_column_name);
        map.add("After All", start_all);
    }

    if (end_column_name)
        map.add("Until Column", *end_column_name);

    if (start_column_name || end_column_name)
        map.add("Expression", ExpressionActions(conditions.clone()).toTree());

    map.add("Reads All Data", always_read_till_end);
}

void LimitRangeStep::serialize(Serialization & ctx) const
{
    /// A peer below this version does not know the `LimitRange` step and would fail on its name, so fail
    /// closed here rather than write bytes it cannot understand.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_RANGE_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a LimitRangeStep requires query plan serialization version >= {}; all nodes must run the same version",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_RANGE_STEP);

    UInt8 flags = 0;
    if (start_column_name)
        flags |= 1;
    if (end_column_name)
        flags |= 2;
    if (limit)
        flags |= 4;
    if (start_all)
        flags |= 8;
    if (always_read_till_end)
        flags |= 16;

    writeIntBinary(flags, ctx.out);

    if (limit)
        writeVarUInt(*limit, ctx.out);

    if (start_column_name)
        writeStringBinary(*start_column_name, ctx.out);
    if (end_column_name)
        writeStringBinary(*end_column_name, ctx.out);

    conditions.serialize(ctx.out, ctx.registry);
}

QueryPlanStepPtr LimitRangeStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a peer that old cannot have written this step.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_RANGE_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Deserializing a LimitRangeStep requires query plan serialization version >= {}; all nodes must run the same version",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_RANGE_STEP);

    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "LimitRangeStep must have one input stream");

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    /// Reject flag bits this version does not know before reading the rest of the payload, so that an
    /// unknown extension fails closed instead of desynchronizing the stream.
    if (flags & ~UInt8(31))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "LimitRangeStep: unsupported flags={} in this version", static_cast<size_t>(flags));

    if ((flags & 8) && !(flags & 1))
        throw Exception(ErrorCodes::INCORRECT_DATA, "LimitRangeStep: ALL requires a start condition");

    std::optional<UInt64> limit_value;
    if (flags & 4)
    {
        UInt64 limit = 0;
        readVarUInt(limit, ctx.in);
        limit_value = limit;
    }

    auto read_column_name = [&](bool present) -> std::optional<String>
    {
        if (!present)
            return std::nullopt;

        String column_name;
        readStringBinary(column_name, ctx.in);
        return column_name;
    };

    /// Read in wire order (start before end). Function-argument evaluation order is unspecified,
    /// so the reads must happen in explicit statements, not inline in the constructor call.
    auto start_column_name = read_column_name(flags & 1);
    auto end_column_name = read_column_name(flags & 2);

    auto conditions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);

    /// The plan may come from a client (`process_query_plan_packet`), so the conditions are checked the
    /// way the planner checks them for a query: boolean result columns, and no `ARRAY JOIN`, which would
    /// misalign the condition rows with the rows of the chunk.
    for (const auto & column_name : {start_column_name, end_column_name})
    {
        if (!column_name)
            continue;

        const auto * output = conditions.tryFindInOutputs(*column_name);
        if (!output)
            throw Exception(ErrorCodes::INCORRECT_DATA, "LimitRangeStep: condition column {} is not an output of its expression", *column_name);
        if (!output->result_type->canBeUsedInBooleanContext())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "LimitRangeStep: condition column {} must be boolean, got {}", *column_name, output->result_type->getName());
    }

    if (conditions.hasArrayJoin())
        throw Exception(ErrorCodes::INCORRECT_DATA, "LimitRangeStep: condition expression must not contain ARRAY JOIN");

    return std::make_unique<LimitRangeStep>(
        ctx.input_headers.front(),
        std::move(conditions),
        std::move(start_column_name),
        std::move(end_column_name),
        flags & 8,
        limit_value,
        bool(flags & 16));
}

QueryPlanStepPtr LimitRangeStep::clone() const
{
    return std::make_unique<LimitRangeStep>(*this);
}

void registerLimitRangeStep(QueryPlanStepRegistry & registry);
void registerLimitRangeStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("LimitRange", LimitRangeStep::deserialize);
}

}
