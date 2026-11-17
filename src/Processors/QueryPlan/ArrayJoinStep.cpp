#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ArrayJoinTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
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

void ArrayJoinStep::setElementFilter(ActionsDAG filter_dag, String filter_column_name, bool remove_filter_column)
{
    element_filter = std::move(filter_dag);
    element_filter_column_name = std::move(filter_column_name);
    remove_element_filter_column = remove_filter_column;
}

void ArrayJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto array_join_actions = std::make_shared<ArrayJoinAction>(array_join.columns, array_join.is_left, is_unaligned, max_block_size, enable_lazy_columns_replication);
    if (element_filter)
    {
        array_join_actions->element_filter = std::make_shared<ExpressionActions>(element_filter->clone(), settings.getActionsSettings());
        array_join_actions->element_filter_column_name = element_filter_column_name;
    }
    /// A standalone FilterStep is a pass-through on the totals stream, so the fused filter must be too -
    /// run the totals stream through an action without the element filter.
    auto totals_actions = element_filter
        ? std::make_shared<ArrayJoinAction>(array_join.columns, array_join.is_left, is_unaligned, max_block_size, enable_lazy_columns_replication)
        : array_join_actions;
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, on_totals ? totals_actions : array_join_actions, on_totals);
    });
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    bool first = true;

    settings.out << prefix << (array_join.is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join.columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;


        settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column, settings.pretty_names) : column);
    }
    settings.out << '\n';

    if (element_filter)
    {
        settings.out << prefix << "Element filter column: " << element_filter_column_name;
        if (remove_element_filter_column)
            settings.out << " (removed)";
        settings.out << '\n';
        if (!settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(element_filter->clone());
            expression->describeActions(settings.out, prefix);
        }
    }
}

void ArrayJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Left", array_join.is_left);

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : array_join.columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));

    if (element_filter)
    {
        map.add("Element Filter Column", element_filter_column_name);
        map.add("Removes Element Filter", remove_element_filter_column);
        auto expression = std::make_shared<ExpressionActions>(element_filter->clone());
        map.add("Element Filter Expression", expression->toTree());
    }
}

void ArrayJoinStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
}

void ArrayJoinStep::serialize(Serialization & ctx) const
{
    /// The filter is only ever serialized locally (e.g. calculateHashTableCacheKeys); fusion bails for
    /// distributed/serialized plans, so an older worker never receives it and no version bump is needed
    const bool serialize_filter = element_filter.has_value();

    UInt8 flags = 0;
    if (array_join.is_left)
        flags |= 1;
    if (is_unaligned)
        flags |= 2;
    /// Carried here rather than through serializeSettings: a step's settings object only ever holds
    /// the names that same step writes, and readers that predate this bit ignore it and keep doing
    /// eager replication, which is the correct fallback for a performance-only flag.
    if (enable_lazy_columns_replication)
        flags |= 4;
    if (serialize_filter)
        flags |= 8;
    if (serialize_filter && remove_element_filter_column)
        flags |= 16;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(array_join.columns.size(), ctx.out);
    for (const auto & column : array_join.columns)
        writeStringBinary(column, ctx.out);

    if (serialize_filter)
    {
        writeStringBinary(element_filter_column_name, ctx.out);
        element_filter->serialize(ctx.out, ctx.registry);
    }
}

QueryPlanStepPtr ArrayJoinStep::clone() const
{
    return std::make_unique<ArrayJoinStep>(*this);
}

QueryPlanStepPtr ArrayJoinStep::deserialize(Deserialization & ctx)
{
    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool is_left = bool(flags & 1);
    bool is_unaligned = bool(flags & 2);
    bool enable_lazy_columns_replication = bool(flags & 4);
    bool has_element_filter = bool(flags & 8);
    bool remove_element_filter_column = bool(flags & 16);

    UInt64 num_columns = 0;
    readVarUInt(num_columns, ctx.in);

    ArrayJoin array_join;
    array_join.is_left = is_left;
    array_join.columns.resize(num_columns);

    for (auto & column : array_join.columns)
        readStringBinary(column, ctx.in);

    auto step = std::make_unique<ArrayJoinStep>(
        ctx.input_headers.front(),
        std::move(array_join),
        is_unaligned,
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        enable_lazy_columns_replication);

    if (has_element_filter)
    {
        String filter_column_name;
        readStringBinary(filter_column_name, ctx.in);
        ActionsDAG filter_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);
        step->setElementFilter(std::move(filter_dag), std::move(filter_column_name), remove_element_filter_column);
    }

    return step;
}

void registerArrayJoinStep(QueryPlanStepRegistry & registry);
void registerArrayJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ArrayJoin", ArrayJoinStep::deserialize);
}

}
