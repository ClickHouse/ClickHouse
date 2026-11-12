#include <Planner/PlannerAggregation.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/getEffectiveMaxThreads.h>
#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 adaptive_aggregator_freeze_threshold;
    extern const SettingsUInt64 adaptive_aggregator_freeze_threshold_bytes;
    extern const SettingsBool adaptive_aggregator_disable_thaw;
    extern const SettingsBool collect_hash_table_stats_during_aggregation;
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_adaptive_aggregator;
    extern const SettingsBool enable_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_parallel_single_level_merge;
    extern const SettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsDouble max_bytes_ratio_before_external_group_by;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const SettingsUInt64 min_bytes_before_external_group_by;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const SettingsBool optimize_group_by_constant_keys;
    extern const SettingsBool serialize_string_in_memory_with_zero_byte;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsString temporary_files_codec;
    extern const SettingsBool use_aggregation_memory_tracker;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_entries_for_hash_table_stats;
}

AggregateDescriptions extractAggregateDescriptions(const QueryTreeNodes & aggregate_function_nodes, const PlannerContext & planner_context)
{
    QueryTreeNodeToName node_to_name;
    return extractAggregateDescriptions(aggregate_function_nodes, planner_context, node_to_name);
}

AggregateDescriptions extractAggregateDescriptions(
    const QueryTreeNodes & aggregate_function_nodes,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name)
{
    NameSet unique_aggregate_action_node_names;
    AggregateDescriptions aggregate_descriptions;

    for (const auto & aggregate_function_node : aggregate_function_nodes)
    {
        const auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();
        String node_name = calculateActionNodeName(aggregate_function_node, planner_context, node_to_name);
        auto [_, inserted] = unique_aggregate_action_node_names.emplace(node_name);
        if (!inserted)
            continue;

        AggregateDescription aggregate_description;
        aggregate_description.function = aggregate_function_node_typed.getAggregateFunction();

        const auto & parameters_nodes = aggregate_function_node_typed.getParameters().getNodes();
        aggregate_description.parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            /// Function parameters constness validated during analysis stage
            aggregate_description.parameters.push_back(parameter_node->as<ConstantNode &>().getValue());
        }

        const auto & arguments_nodes = aggregate_function_node_typed.getArguments().getNodes();
        aggregate_description.argument_names.reserve(arguments_nodes.size());

        for (const auto & argument_node : arguments_nodes)
        {
            String argument_node_name = calculateActionNodeName(argument_node, planner_context, node_to_name);
            aggregate_description.argument_names.emplace_back(std::move(argument_node_name));
        }

        aggregate_description.column_name = std::move(node_name);
        aggregate_descriptions.push_back(std::move(aggregate_description));
    }

    return aggregate_descriptions;
}

Aggregator::Params getAggregatorParams(const ContextPtr & query_context, const Names & keys, const AggregateDescriptions & aggregates, bool overflow_row)
{
    const auto & settings = query_context->getSettingsRef();

    /// The cache key is computed later from the query plan in setAggregationHashTableCacheKeys
    /// (key == 0 keeps preallocation disabled until the optimization pass stamps the real key).
    /// max_size_to_preallocate is still carried here because it is consumed at runtime.
    const auto stats_collecting_params = StatsCollectingParams(
        /*key_=*/ 0,
        settings[Setting::collect_hash_table_stats_during_aggregation],
        query_context->getServerSettings()[ServerSetting::max_entries_for_hash_table_stats],
        settings[Setting::max_size_to_preallocate_for_aggregation]);

    auto tmp_data_scope = query_context->getTempDataOnDisk();
    if (tmp_data_scope)
        tmp_data_scope = tmp_data_scope->childScope(/* metrics */{}, settings[Setting::temporary_files_buffer_size], settings[Setting::temporary_files_codec]);
    return Aggregator::Params(
        keys,
        aggregates,
        overflow_row,
        settings[Setting::max_rows_to_group_by],
        settings[Setting::group_by_overflow_mode].value,
        settings[Setting::group_by_two_level_threshold],
        settings[Setting::group_by_two_level_threshold_bytes],
        Aggregator::Params::getMaxBytesBeforeExternalGroupBy(
            settings[Setting::max_bytes_before_external_group_by], settings[Setting::max_bytes_ratio_before_external_group_by]),
        settings[Setting::min_bytes_before_external_group_by],
        settings[Setting::empty_result_for_aggregation_by_empty_set],
        tmp_data_scope,
        getEffectiveMaxThreads(settings),
        settings[Setting::min_free_disk_space_for_temporary_data],
        settings[Setting::compile_aggregate_expressions],
        settings[Setting::min_count_to_compile_aggregate_expression],
        settings[Setting::max_block_size],
        settings[Setting::enable_software_prefetch_in_aggregation],
        /* only_merge */ false,
        settings[Setting::optimize_group_by_constant_keys],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params,
        settings[Setting::enable_producing_buckets_out_of_order_in_aggregation],
        settings[Setting::serialize_string_in_memory_with_zero_byte],
        settings[Setting::enable_parallel_single_level_merge],
        settings[Setting::enable_packed_string_keys_in_aggregation],
        settings[Setting::enable_adaptive_aggregator],
        settings[Setting::adaptive_aggregator_freeze_threshold],
        settings[Setting::adaptive_aggregator_freeze_threshold_bytes],
        settings[Setting::adaptive_aggregator_disable_thaw],
        settings[Setting::use_aggregation_memory_tracker]);
}

}
