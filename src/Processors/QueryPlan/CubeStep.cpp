#include <Processors/QueryPlan/CubeStep.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool enable_packed_string_keys_in_aggregation;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_CUBE_STEP = 9;

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

CubeStep::CubeStep(const SharedHeader & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(generateOutputHeader(params_.getHeader(*input_header_, final_), params_.keys, use_nulls_)), getTraits())
    , keys_size(params_.keys_size)
    , params(std::move(params_))
    , final(final_)
    , use_nulls(use_nulls_)
{
}

ProcessorPtr addGroupingSetForTotals(SharedHeader header, const Names & keys, bool use_nulls, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number);

ProcessorPtr addGroupingSetForTotals(SharedHeader header, const Names & keys, bool use_nulls, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number)
{
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    auto & outputs = dag.getOutputs();

    if (use_nulls)
    {
        auto to_nullable = FunctionFactory::instance().get("toNullable", nullptr);
        for (const auto & key : keys)
        {
            const auto * node = dag.getOutputs()[header->getPositionByName(key)];
            if (removeLowCardinality(node->result_type)->canBeInsideNullable())
            {
                dag.addOrReplaceInOutputs(dag.addFunction(to_nullable, { node }, node->result_name));
            }
        }
    }

    ColumnConst::Ptr grouping_col = ColumnConst::create(ColumnUInt64::create(1, grouping_set_number), 0);
    const auto * grouping_node = &dag.addColumn(
        std::move(grouping_col), std::make_shared<DataTypeUInt64>(), "__grouping_set");

    grouping_node = &dag.materializeNode(*grouping_node);
    outputs.insert(outputs.begin(), grouping_node);

    auto expression = std::make_shared<ExpressionActions>(std::move(dag), settings.getActionsSettings());
    return std::make_shared<ExpressionTransform>(header, expression);
}

void CubeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](SharedHeader header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, params.keys, use_nulls, settings, (UInt64(1) << keys_size) - 1);

        auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), final);
        return std::make_shared<CubeTransform>(header, std::move(transform_params), use_nulls);
    });
}

const Aggregator::Params & CubeStep::getParams() const
{
    return params;
}

QueryPlanStepPtr CubeStep::clone() const
{
    return std::make_unique<CubeStep>(*this);
}

void CubeStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(generateOutputHeader(params.getHeader(*input_headers.front(), final), params.keys, use_nulls));
}

void CubeStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    /// Only the parameters the transform's block merge reads; the reader rebuilds merge-only
    /// `Aggregator::Params` from them, like `MergingAggregatedStep` does.
    settings[QueryPlanSerializationSetting::max_block_size] = params.max_block_size;
    settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization] = params.min_hit_rate_to_use_consecutive_keys_optimization;
    settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte] = params.serialize_string_with_zero_byte;
    /// Every version that can read a `Cube` step also knows this setting name (see the gate in
    /// `serialize`), so unlike `AggregatingStep` no old-peer narrowing applies.
    settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation] = params.enable_packed_string_keys;
}

void CubeStep::serialize(Serialization & ctx) const
{
    /// A "Cube" step is only registered under `QueryPlanStepRegistry` since query-plan serialization
    /// version 8; an older peer does not know the step name and would throw on it. Throw here rather
    /// than send bytes the other side cannot read.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_CUBE_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a CubeStep requires query plan serialization version >= {}; "
            "the receiving server is too old for it", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_CUBE_STEP);

    UInt8 flags = 0;
    if (final)
        flags |= 1;
    if (params.overflow_row)
        flags |= 2;
    if (use_nulls)
        flags |= 4;
    writeIntBinary(flags, ctx.out);

    writeVarUInt(params.keys.size(), ctx.out);
    for (const auto & key : params.keys)
        writeStringBinary(key, ctx.out);

    /// The planner builds the cube aggregates without argument names (the transform only merges
    /// states, so the argument columns do not exist in its input), which the generic
    /// `serializeAggregateDescriptions` rejects.
    serializeAggregateDescriptionsWithoutArguments(params.aggregates, ctx.out);
}

QueryPlanStepPtr CubeStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a "Cube" step never legitimately arrives from a stream
    /// written below this version, since a peer that old cannot have written one.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_CUBE_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Deserializing a CubeStep requires query plan serialization version >= {}, "
            "but the plan was written with version {}",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_CUBE_STEP, ctx.version);

    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CubeStep must have one input stream");

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);
    const bool final = bool(flags & 1);
    const bool overflow_row = bool(flags & 2);
    const bool use_nulls = bool(flags & 4);

    UInt64 num_keys = 0;
    readVarUInt(num_keys, ctx.in);
    Names keys(num_keys);
    for (auto & key : keys)
        readStringBinary(key, ctx.in);

    AggregateDescriptions aggregates;
    deserializeAggregateDescriptionsWithoutArguments(aggregates, ctx.in, ctx.max_type_complexity);

    const auto & query_settings = ctx.context->getSettingsRef();

    Aggregator::Params params(
        keys,
        aggregates,
        overflow_row,
        query_settings[Setting::max_threads],
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization],
        ctx.settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte],
        ctx.settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation]);

    /// The transform's `group_by_use_nulls` path builds an `Aggregator` over a finalized header;
    /// with `only_merge` set, the aggregate state types would be read from that header's finalized
    /// columns and the non-final output would be built over the wrong column type. The writer's
    /// planner-built params carry `only_merge = false` as well.
    params.only_merge = false;

    return std::make_unique<CubeStep>(ctx.input_headers.front(), std::move(params), final, use_nulls);
}

void registerCubeStep(QueryPlanStepRegistry & registry);
void registerCubeStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Cube", CubeStep::deserialize);
}

}
