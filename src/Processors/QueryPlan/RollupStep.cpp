#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/NullsAction.h>
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
    extern const QueryPlanSerializationSettingsBool compile_aggregate_expressions;
    extern const QueryPlanSerializationSettingsBool empty_result_for_aggregation_by_empty_set;
    extern const QueryPlanSerializationSettingsBool enable_packed_string_keys_in_aggregation;
    extern const QueryPlanSerializationSettingsBool enable_parallel_single_level_merge;
    extern const QueryPlanSerializationSettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const QueryPlanSerializationSettingsBool enable_software_prefetch_in_aggregation;
    extern const QueryPlanSerializationSettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold_bytes;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_group_by;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_to_group_by;
    extern const QueryPlanSerializationSettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const QueryPlanSerializationSettingsBool optimize_group_by_constant_keys;
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
}

namespace ErrorCodes
{
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
    settings[QueryPlanSerializationSetting::max_block_size] = params.max_block_size;

    settings[QueryPlanSerializationSetting::max_rows_to_group_by] = params.max_rows_to_group_by;
    settings[QueryPlanSerializationSetting::group_by_overflow_mode] = params.group_by_overflow_mode;

    settings[QueryPlanSerializationSetting::group_by_two_level_threshold] = params.group_by_two_level_threshold;
    settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes] = params.group_by_two_level_threshold_bytes;

    settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by] = params.max_bytes_before_external_group_by;
    settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set] = params.empty_result_for_aggregation_by_empty_set;

    settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = params.min_free_disk_space;

    settings[QueryPlanSerializationSetting::compile_aggregate_expressions] = params.compile_aggregate_expressions;
    settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression] = params.min_count_to_compile_aggregate_expression;

    settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation] = params.enable_prefetch;
    settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys] = params.optimize_group_by_constant_keys;
    settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization] = params.min_hit_rate_to_use_consecutive_keys_optimization;

    settings[QueryPlanSerializationSetting::enable_producing_buckets_out_of_order_in_aggregation] = params.enable_producing_buckets_out_of_order_in_aggregation;
    settings[QueryPlanSerializationSetting::enable_parallel_single_level_merge] = params.enable_parallel_single_level_merge;

    settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte] = params.serialize_string_with_zero_byte;

    /// Every version that can read a `Rollup` step also knows this setting name (see the gate in
    /// `serialize`), so unlike `AggregatingStep` no old-peer narrowing applies.
    settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation] = params.enable_packed_string_keys;
}

void RollupStep::serialize(Serialization & ctx) const
{
    /// A "Rollup" step is only registered under `QueryPlanStepRegistry` since query-plan serialization
    /// version 7; an older worker does not know the step name and would throw on it. Throw here rather
    /// than send bytes the other side cannot read.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: serializing a RollupStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP);

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
    /// `serializeAggregateDescriptions` rejects. Write the argument types from the resolved function
    /// instead, so the reader can resolve the same function and keep the argument names empty.
    writeVarUInt(params.aggregates.size(), ctx.out);
    for (const auto & aggregate : params.aggregates)
    {
        writeStringBinary(aggregate.column_name, ctx.out);
        writeStringBinary(aggregate.function->getName(), ctx.out);

        const auto & argument_types = aggregate.function->getArgumentTypes();
        writeVarUInt(argument_types.size(), ctx.out);
        for (const auto & argument_type : argument_types)
            encodeDataType(argument_type, ctx.out);

        writeVarUInt(aggregate.parameters.size(), ctx.out);
        for (const auto & param : aggregate.parameters)
            writeFieldBinary(param, ctx.out);
    }
}

QueryPlanStepPtr RollupStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a "Rollup" step never legitimately arrives from a stream
    /// written below this version, since a peer that old cannot have written one.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: deserializing a RollupStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ROLLUP_STEP);

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

    UInt64 num_aggregates = 0;
    readVarUInt(num_aggregates, ctx.in);
    AggregateDescriptions aggregates(num_aggregates);
    for (auto & aggregate : aggregates)
    {
        readStringBinary(aggregate.column_name, ctx.in);

        String function_name;
        readStringBinary(function_name, ctx.in);

        UInt64 num_args = 0;
        readVarUInt(num_args, ctx.in);
        DataTypes argument_types;
        argument_types.reserve(num_args);
        for (size_t arg_num = 0; arg_num < num_args; ++arg_num)
            argument_types.emplace_back(decodeDataType(ctx.in, ctx.max_type_complexity));

        UInt64 num_params = 0;
        readVarUInt(num_params, ctx.in);
        aggregate.parameters.resize(num_params);
        for (auto & param : aggregate.parameters)
            param = readFieldBinary(ctx.in);

        /// `argument_names` stays empty, mirroring the writer's state (see `serialize`).
        AggregateFunctionProperties properties;
        aggregate.function = AggregateFunctionFactory::instance().get(
            function_name, NullsAction::EMPTY, argument_types, aggregate.parameters, properties);
    }

    /// Rebuild the same full (not merge-only) `Aggregator::Params` shape the planner gives the writer's
    /// `RollupStep`: the transform constructs its own `Aggregator` instances from them.
    Aggregator::Params params{
        keys,
        aggregates,
        overflow_row,
        ctx.settings[QueryPlanSerializationSetting::max_rows_to_group_by],
        ctx.settings[QueryPlanSerializationSetting::group_by_overflow_mode],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes],
        ctx.settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by],
        ctx.settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set],
        Context::getGlobalContextInstance()->getTempDataOnDisk(),
        0, /// max_threads: 0 lets the pipeline decide
        ctx.settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data],
        ctx.settings[QueryPlanSerializationSetting::compile_aggregate_expressions],
        ctx.settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression],
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation],
        /* only_merge */ false,
        ctx.settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys],
        ctx.settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization],
        StatsCollectingParams{},
        ctx.settings[QueryPlanSerializationSetting::enable_producing_buckets_out_of_order_in_aggregation],
        ctx.settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte],
        ctx.settings[QueryPlanSerializationSetting::enable_parallel_single_level_merge],
        ctx.settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation]};

    return std::make_unique<RollupStep>(ctx.input_headers.front(), std::move(params), final, use_nulls);
}

void registerRollupStep(QueryPlanStepRegistry & registry);
void registerRollupStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Rollup", RollupStep::deserialize);
}

}
