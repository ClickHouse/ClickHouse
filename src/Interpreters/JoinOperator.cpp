#include <Interpreters/JoinOperator.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

#include <fmt/ranges.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_rows_in_join;
    extern const SettingsUInt64 max_bytes_in_join;
    extern const SettingsOverflowMode join_overflow_mode;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsUInt64 cross_join_min_rows_to_compress;
    extern const SettingsUInt64 cross_join_min_bytes_to_compress;
    extern const SettingsUInt64 partial_merge_join_left_table_buffer_bytes;
    extern const SettingsUInt64 partial_merge_join_rows_in_right_blocks;
    extern const SettingsUInt64 join_on_disk_max_files_to_merge;

    extern const SettingsNonZeroUInt64 grace_hash_join_initial_buckets;
    extern const SettingsNonZeroUInt64 grace_hash_join_max_buckets;

    extern const SettingsUInt64 max_rows_in_set_to_optimize_join;

    extern const SettingsBool collect_hash_table_stats_during_joins;
    extern const SettingsUInt64 max_size_to_preallocate_for_joins;
    extern const SettingsUInt64 parallel_hash_join_threshold;

    extern const SettingsUInt64 max_joined_block_size_rows;
    extern const SettingsString temporary_files_codec;
    extern const SettingsUInt64 join_output_by_rowlist_perkey_rows_threshold;
    extern const SettingsUInt64 join_to_sort_minimum_perkey_rows;
    extern const SettingsUInt64 join_to_sort_maximum_table_rows;
    extern const SettingsBool allow_experimental_join_right_table_sorting;
    extern const SettingsUInt64 min_joined_block_size_bytes;
    extern const SettingsMaxThreads max_threads;

    extern const SettingsUInt64 default_max_bytes_in_join;
}

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsJoinAlgorithm join_algorithm;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_join;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_join;
    extern const QueryPlanSerializationSettingsOverflowMode join_overflow_mode;
    extern const QueryPlanSerializationSettingsBool join_any_take_last_row;
    extern const QueryPlanSerializationSettingsUInt64 cross_join_min_rows_to_compress;
    extern const QueryPlanSerializationSettingsUInt64 cross_join_min_bytes_to_compress;
    extern const QueryPlanSerializationSettingsUInt64 partial_merge_join_left_table_buffer_bytes;
    extern const QueryPlanSerializationSettingsUInt64 partial_merge_join_rows_in_right_blocks;
    extern const QueryPlanSerializationSettingsUInt64 join_on_disk_max_files_to_merge;

    extern const QueryPlanSerializationSettingsNonZeroUInt64 grace_hash_join_initial_buckets;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 grace_hash_join_max_buckets;

    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_set_to_optimize_join;

    extern const QueryPlanSerializationSettingsBool collect_hash_table_stats_during_joins;
    extern const QueryPlanSerializationSettingsUInt64 max_size_to_preallocate_for_joins;
    extern const QueryPlanSerializationSettingsUInt64 parallel_hash_join_threshold;

    extern const QueryPlanSerializationSettingsUInt64 max_joined_block_size_rows;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsUInt64 join_output_by_rowlist_perkey_rows_threshold;
    extern const QueryPlanSerializationSettingsUInt64 join_to_sort_minimum_perkey_rows;
    extern const QueryPlanSerializationSettingsUInt64 join_to_sort_maximum_table_rows;
    extern const QueryPlanSerializationSettingsBool allow_experimental_join_right_table_sorting;
    extern const QueryPlanSerializationSettingsUInt64 min_joined_block_size_bytes;

    extern const QueryPlanSerializationSettingsUInt64 default_max_bytes_in_join;
}

JoinSettings::JoinSettings(const Settings & query_settings)
{
    join_algorithms = query_settings[Setting::join_algorithm];

    max_block_size = query_settings[Setting::max_block_size];

    max_rows_in_join = query_settings[Setting::max_rows_in_join];
    max_bytes_in_join = query_settings[Setting::max_bytes_in_join];
    default_max_bytes_in_join = query_settings[Setting::default_max_bytes_in_join];

    max_joined_block_size_rows = query_settings[Setting::max_joined_block_size_rows];
    min_joined_block_size_bytes = query_settings[Setting::min_joined_block_size_bytes];

    join_overflow_mode = query_settings[Setting::join_overflow_mode];
    join_any_take_last_row = query_settings[Setting::join_any_take_last_row];

    cross_join_min_rows_to_compress = query_settings[Setting::cross_join_min_rows_to_compress];
    cross_join_min_bytes_to_compress = query_settings[Setting::cross_join_min_bytes_to_compress];

    partial_merge_join_left_table_buffer_bytes = query_settings[Setting::partial_merge_join_left_table_buffer_bytes];
    partial_merge_join_rows_in_right_blocks = query_settings[Setting::partial_merge_join_rows_in_right_blocks];
    join_on_disk_max_files_to_merge = query_settings[Setting::join_on_disk_max_files_to_merge];

    grace_hash_join_initial_buckets = query_settings[Setting::grace_hash_join_initial_buckets];
    grace_hash_join_max_buckets = query_settings[Setting::grace_hash_join_max_buckets];

    max_rows_in_set_to_optimize_join = query_settings[Setting::max_rows_in_set_to_optimize_join];

    collect_hash_table_stats_during_joins = query_settings[Setting::collect_hash_table_stats_during_joins];
    max_size_to_preallocate_for_joins = query_settings[Setting::max_size_to_preallocate_for_joins];
    parallel_hash_join_threshold = query_settings[Setting::parallel_hash_join_threshold];

    temporary_files_codec = query_settings[Setting::temporary_files_codec];
    join_output_by_rowlist_perkey_rows_threshold = query_settings[Setting::join_output_by_rowlist_perkey_rows_threshold];
    join_to_sort_minimum_perkey_rows = query_settings[Setting::join_to_sort_minimum_perkey_rows];
    join_to_sort_maximum_table_rows = query_settings[Setting::join_to_sort_maximum_table_rows];
    allow_experimental_join_right_table_sorting = query_settings[Setting::allow_experimental_join_right_table_sorting];
}

JoinSettings::JoinSettings(const QueryPlanSerializationSettings & settings)
{
    join_algorithms = settings[QueryPlanSerializationSetting::join_algorithm];
    max_block_size = settings[QueryPlanSerializationSetting::max_block_size];

    max_rows_in_join = settings[QueryPlanSerializationSetting::max_rows_in_join];
    max_bytes_in_join = settings[QueryPlanSerializationSetting::max_bytes_in_join];

    join_overflow_mode = settings[QueryPlanSerializationSetting::join_overflow_mode];
    join_any_take_last_row = settings[QueryPlanSerializationSetting::join_any_take_last_row];

    cross_join_min_rows_to_compress = settings[QueryPlanSerializationSetting::cross_join_min_rows_to_compress];
    cross_join_min_bytes_to_compress = settings[QueryPlanSerializationSetting::cross_join_min_bytes_to_compress];

    partial_merge_join_left_table_buffer_bytes = settings[QueryPlanSerializationSetting::partial_merge_join_left_table_buffer_bytes];
    partial_merge_join_rows_in_right_blocks = settings[QueryPlanSerializationSetting::partial_merge_join_rows_in_right_blocks];
    join_on_disk_max_files_to_merge = settings[QueryPlanSerializationSetting::join_on_disk_max_files_to_merge];

    grace_hash_join_initial_buckets = settings[QueryPlanSerializationSetting::grace_hash_join_initial_buckets];
    grace_hash_join_max_buckets = settings[QueryPlanSerializationSetting::grace_hash_join_max_buckets];

    max_rows_in_set_to_optimize_join = settings[QueryPlanSerializationSetting::max_rows_in_set_to_optimize_join];

    collect_hash_table_stats_during_joins = settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_joins];
    max_size_to_preallocate_for_joins = settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_joins];
    parallel_hash_join_threshold = settings[QueryPlanSerializationSetting::parallel_hash_join_threshold];

    max_joined_block_size_rows = settings[QueryPlanSerializationSetting::max_joined_block_size_rows];
    temporary_files_codec = settings[QueryPlanSerializationSetting::temporary_files_codec];
    join_output_by_rowlist_perkey_rows_threshold = settings[QueryPlanSerializationSetting::join_output_by_rowlist_perkey_rows_threshold];
    join_to_sort_minimum_perkey_rows = settings[QueryPlanSerializationSetting::join_to_sort_minimum_perkey_rows];
    join_to_sort_maximum_table_rows = settings[QueryPlanSerializationSetting::join_to_sort_maximum_table_rows];
    allow_experimental_join_right_table_sorting = settings[QueryPlanSerializationSetting::allow_experimental_join_right_table_sorting];
    min_joined_block_size_bytes = settings[QueryPlanSerializationSetting::min_joined_block_size_bytes];

    default_max_bytes_in_join = settings[QueryPlanSerializationSetting::default_max_bytes_in_join];
}

void JoinSettings::updatePlanSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::join_algorithm] = join_algorithms;
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;

    settings[QueryPlanSerializationSetting::max_rows_in_join] = max_rows_in_join;
    settings[QueryPlanSerializationSetting::max_bytes_in_join] = max_bytes_in_join;

    settings[QueryPlanSerializationSetting::join_overflow_mode] = join_overflow_mode;
    settings[QueryPlanSerializationSetting::join_any_take_last_row] = join_any_take_last_row;

    settings[QueryPlanSerializationSetting::cross_join_min_rows_to_compress] = cross_join_min_rows_to_compress;
    settings[QueryPlanSerializationSetting::cross_join_min_bytes_to_compress] = cross_join_min_bytes_to_compress;

    settings[QueryPlanSerializationSetting::partial_merge_join_left_table_buffer_bytes] = partial_merge_join_left_table_buffer_bytes;
    settings[QueryPlanSerializationSetting::partial_merge_join_rows_in_right_blocks] = partial_merge_join_rows_in_right_blocks;
    settings[QueryPlanSerializationSetting::join_on_disk_max_files_to_merge] = join_on_disk_max_files_to_merge;

    settings[QueryPlanSerializationSetting::grace_hash_join_initial_buckets] = grace_hash_join_initial_buckets;
    settings[QueryPlanSerializationSetting::grace_hash_join_max_buckets] = grace_hash_join_max_buckets;

    settings[QueryPlanSerializationSetting::max_rows_in_set_to_optimize_join] = max_rows_in_set_to_optimize_join;

    settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_joins] = collect_hash_table_stats_during_joins;
    settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_joins] = max_size_to_preallocate_for_joins;
    settings[QueryPlanSerializationSetting::parallel_hash_join_threshold] = parallel_hash_join_threshold;

    settings[QueryPlanSerializationSetting::max_joined_block_size_rows] = max_joined_block_size_rows;
    settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    settings[QueryPlanSerializationSetting::join_output_by_rowlist_perkey_rows_threshold] = join_output_by_rowlist_perkey_rows_threshold;
    settings[QueryPlanSerializationSetting::join_to_sort_minimum_perkey_rows] = join_to_sort_minimum_perkey_rows;
    settings[QueryPlanSerializationSetting::join_to_sort_maximum_table_rows] = join_to_sort_maximum_table_rows;
    settings[QueryPlanSerializationSetting::allow_experimental_join_right_table_sorting] = allow_experimental_join_right_table_sorting;
    settings[QueryPlanSerializationSetting::min_joined_block_size_bytes] = min_joined_block_size_bytes;

    settings[QueryPlanSerializationSetting::default_max_bytes_in_join] = default_max_bytes_in_join;
}

String toString(const JoinActionRef & node)
{
    WriteBufferFromOwnString out;

    const auto & column = node.getColumn();
    out << column.name;
    out << " :: " << column.type->getName();
    if (column. column)
        out << " CONST " << column. column->dumpStructure();
    return out.str();
}

static void serializeJoinActions(WriteBuffer & out, const std::vector<JoinActionRef> & actions, const ActionsDAG * actions_dag)
{
    auto nodes = std::ranges::to<std::vector>(actions | std::views::transform([] (const auto & action) { return action.getNode(); }));
    ActionsDAG::serializeNodeList(out, actions_dag->getNodeToIdMap(), nodes);
}

static std::vector<JoinActionRef> deserializeJoinActions(ReadBuffer & in, JoinExpressionActions & expression_actions)
{
    auto node_map = expression_actions.getActionsDAG()->getIdToNodeMap();
    auto nodes = ActionsDAG::deserializeNodeList(in, node_map);

    std::vector<JoinActionRef> actions;
    actions.reserve(nodes.size());
    for (const auto * node : nodes)
        actions.emplace_back(node, expression_actions);

    return actions;
}

void JoinOperator::serialize(WriteBuffer & out, const ActionsDAG * actions_dag) const
{
    serializeJoinActions(out, expression, actions_dag);
    serializeJoinActions(out, residual_filter, actions_dag);

    serializeJoinKind(kind, out);
    serializeJoinStrictness(strictness, out);
    serializeJoinLocality(locality, out);
}

JoinOperator JoinOperator::deserialize(ReadBuffer & in, JoinExpressionActions & expression_actions)
{
    auto actions = deserializeJoinActions(in, expression_actions);
    auto residual_filter = deserializeJoinActions(in, expression_actions);

    auto kind = deserializeJoinKind(in);
    auto strictness = deserializeJoinStrictness(in);
    auto locality = deserializeJoinLocality(in);

    JoinOperator result(kind, strictness, locality);
    result.expression = std::move(actions);
    result.residual_filter = std::move(residual_filter);

    return result;
}

}
