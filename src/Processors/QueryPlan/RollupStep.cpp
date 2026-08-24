#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/RollupTransform.h>
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

constexpr UInt64 DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP = 9;

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

RollupStep::RollupStep(const SharedHeader & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(generateOutputHeader(params_.getHeader(*input_header_, final_), params_.keys, use_nulls_)), getTraits())
    , params(std::move(params_))
    , keys_size(params.keys_size)
    , final(final_)
    , use_nulls(use_nulls_)
{
}

ProcessorPtr addGroupingSetForTotals(SharedHeader header, const Names & keys, bool use_nulls, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number);

void RollupStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, params.keys, use_nulls, settings, keys_size);

        auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), true);
        return std::make_shared<RollupTransform>(header, std::move(transform_params), use_nulls);
    });
}

void RollupStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(generateOutputHeader(params.getHeader(*input_headers.front(), final), params.keys, use_nulls));
}

QueryPlanStepPtr RollupStep::clone() const
{
    return std::make_unique<RollupStep>(*this);
}

void RollupStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    /// Only the parameters the transform's block merge reads; the reader rebuilds merge-only
    /// `Aggregator::Params` from them, like `MergingAggregatedStep` does.
    settings[QueryPlanSerializationSetting::max_block_size] = params.max_block_size;
    settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization] = params.min_hit_rate_to_use_consecutive_keys_optimization;
    settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte] = params.serialize_string_with_zero_byte;
    /// Every version that can read a `Rollup` step also knows this setting name (see the gate in
    /// `serialize`), so unlike `AggregatingStep` no old-peer narrowing applies.
    settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation] = params.enable_packed_string_keys;
}

void RollupStep::serialize(Serialization & ctx) const
{
    /// A "Rollup" step is only registered under `QueryPlanStepRegistry` since query-plan serialization
    /// version 8; an older peer does not know the step name and would throw on it. Throw here rather
    /// than send bytes the other side cannot read.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a RollupStep requires query plan serialization version >= {}; "
            "the receiving server is too old for it", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP);

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

    /// The planner builds the rollup aggregates without argument names (the transform only merges
    /// states, so the argument columns do not exist in its input), which the generic
    /// `serializeAggregateDescriptions` rejects.
    serializeAggregateDescriptionsWithoutArguments(params.aggregates, ctx.out);
}

QueryPlanStepPtr RollupStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a "Rollup" step never legitimately arrives from a stream
    /// written below this version, since a peer that old cannot have written one.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Deserializing a RollupStep requires query plan serialization version >= {}, "
            "but the plan was written with version {}",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP, ctx.version);

    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "RollupStep must have one input stream");

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

    return std::make_unique<RollupStep>(ctx.input_headers.front(), std::move(params), final, use_nulls);
}

void registerRollupStep(QueryPlanStepRegistry & registry);
void registerRollupStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Rollup", RollupStep::deserialize);
}

}
