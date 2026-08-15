#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatorParamsSerialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>

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

void serializeAggregatorParamsToSettings(const Aggregator::Params & params, QueryPlanSerializationSettings & settings)
{
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

    /// Written even when `false`: a peer predating this setting name serializes String keys the
    /// way `false` does, so omitting the `false` value would silently leave such a peer on the
    /// other layout.
    settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte] = params.serialize_string_with_zero_byte;
}

Aggregator::Params deserializeAggregatorParams(
    Names keys,
    AggregateDescriptions aggregates,
    bool overflow_row,
    StatsCollectingParams stats_collecting_params,
    const IQueryPlanStep::Deserialization & ctx)
{
    return Aggregator::Params{
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
        std::move(stats_collecting_params),
        ctx.settings[QueryPlanSerializationSetting::enable_producing_buckets_out_of_order_in_aggregation],
        ctx.settings[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte],
        ctx.settings[QueryPlanSerializationSetting::enable_parallel_single_level_merge],
        ctx.settings[QueryPlanSerializationSetting::enable_packed_string_keys_in_aggregation]};
}

}
