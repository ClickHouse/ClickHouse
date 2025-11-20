#include <vector>
#include <Interpreters/JoinOperator.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

#include <fmt/ranges.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

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

    extern const SettingsBool joined_block_split_single_row;
    extern const SettingsUInt64 max_joined_block_size_rows;
    extern const SettingsUInt64 max_joined_block_size_bytes;
    extern const SettingsString temporary_files_codec;
    extern const SettingsUInt64 temporary_files_buffer_size;
    extern const SettingsUInt64 join_output_by_rowlist_perkey_rows_threshold;
    extern const SettingsUInt64 join_to_sort_minimum_perkey_rows;
    extern const SettingsUInt64 join_to_sort_maximum_table_rows;
    extern const SettingsBool allow_experimental_join_right_table_sorting;
    extern const SettingsUInt64 min_joined_block_size_rows;
    extern const SettingsUInt64 min_joined_block_size_bytes;
    extern const SettingsMaxThreads max_threads;

    extern const SettingsUInt64 default_max_bytes_in_join;

    extern const SettingsBool allow_dynamic_type_in_join_keys;
    extern const SettingsBool use_join_disjunctions_push_down;
    extern const SettingsBool enable_lazy_columns_replication;
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

    extern const QueryPlanSerializationSettingsBool joined_block_split_single_row;
    extern const QueryPlanSerializationSettingsUInt64 max_joined_block_size_rows;
    extern const QueryPlanSerializationSettingsUInt64 max_joined_block_size_bytes;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsUInt64 temporary_files_buffer_size;
    extern const QueryPlanSerializationSettingsUInt64 join_output_by_rowlist_perkey_rows_threshold;
    extern const QueryPlanSerializationSettingsUInt64 join_to_sort_minimum_perkey_rows;
    extern const QueryPlanSerializationSettingsUInt64 join_to_sort_maximum_table_rows;
    extern const QueryPlanSerializationSettingsBool allow_experimental_join_right_table_sorting;
    extern const QueryPlanSerializationSettingsUInt64 min_joined_block_size_rows;
    extern const QueryPlanSerializationSettingsUInt64 min_joined_block_size_bytes;

    extern const QueryPlanSerializationSettingsUInt64 default_max_bytes_in_join;

    extern const QueryPlanSerializationSettingsBool allow_dynamic_type_in_join_keys;
    extern const QueryPlanSerializationSettingsBool use_join_disjunctions_push_down;
    extern const QueryPlanSerializationSettingsBool enable_lazy_columns_replication;
}

JoinSettings::JoinSettings(const Settings & query_settings)
{
    join_algorithms = query_settings[Setting::join_algorithm];

    max_block_size = query_settings[Setting::max_block_size];

    max_rows_in_join = query_settings[Setting::max_rows_in_join];
    max_bytes_in_join = query_settings[Setting::max_bytes_in_join];
    default_max_bytes_in_join = query_settings[Setting::default_max_bytes_in_join];

    joined_block_split_single_row = query_settings[Setting::joined_block_split_single_row];
    max_joined_block_size_rows = query_settings[Setting::max_joined_block_size_rows];
    max_joined_block_size_bytes = query_settings[Setting::max_joined_block_size_bytes];
    min_joined_block_size_rows = query_settings[Setting::min_joined_block_size_rows];
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
    temporary_files_buffer_size = query_settings[Setting::temporary_files_buffer_size];
    join_output_by_rowlist_perkey_rows_threshold = query_settings[Setting::join_output_by_rowlist_perkey_rows_threshold];
    join_to_sort_minimum_perkey_rows = query_settings[Setting::join_to_sort_minimum_perkey_rows];
    join_to_sort_maximum_table_rows = query_settings[Setting::join_to_sort_maximum_table_rows];
    allow_experimental_join_right_table_sorting = query_settings[Setting::allow_experimental_join_right_table_sorting];

    allow_dynamic_type_in_join_keys = query_settings[Setting::allow_dynamic_type_in_join_keys];
    use_join_disjunctions_push_down = query_settings[Setting::use_join_disjunctions_push_down];
    enable_lazy_columns_replication = query_settings[Setting::enable_lazy_columns_replication];
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

    joined_block_split_single_row = settings[QueryPlanSerializationSetting::joined_block_split_single_row];
    max_joined_block_size_rows = settings[QueryPlanSerializationSetting::max_joined_block_size_rows];
    max_joined_block_size_bytes = settings[QueryPlanSerializationSetting::max_joined_block_size_bytes];
    temporary_files_codec = settings[QueryPlanSerializationSetting::temporary_files_codec];
    temporary_files_buffer_size = settings[QueryPlanSerializationSetting::temporary_files_buffer_size];
    join_output_by_rowlist_perkey_rows_threshold = settings[QueryPlanSerializationSetting::join_output_by_rowlist_perkey_rows_threshold];
    join_to_sort_minimum_perkey_rows = settings[QueryPlanSerializationSetting::join_to_sort_minimum_perkey_rows];
    join_to_sort_maximum_table_rows = settings[QueryPlanSerializationSetting::join_to_sort_maximum_table_rows];
    allow_experimental_join_right_table_sorting = settings[QueryPlanSerializationSetting::allow_experimental_join_right_table_sorting];
    min_joined_block_size_rows = settings[QueryPlanSerializationSetting::min_joined_block_size_rows];
    min_joined_block_size_bytes = settings[QueryPlanSerializationSetting::min_joined_block_size_bytes];

    default_max_bytes_in_join = settings[QueryPlanSerializationSetting::default_max_bytes_in_join];

    allow_dynamic_type_in_join_keys = settings[QueryPlanSerializationSetting::allow_dynamic_type_in_join_keys];
    use_join_disjunctions_push_down = settings[QueryPlanSerializationSetting::use_join_disjunctions_push_down];
    enable_lazy_columns_replication = settings[QueryPlanSerializationSetting::enable_lazy_columns_replication];

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

    settings[QueryPlanSerializationSetting::joined_block_split_single_row] = joined_block_split_single_row;
    settings[QueryPlanSerializationSetting::max_joined_block_size_rows] = max_joined_block_size_rows;
    settings[QueryPlanSerializationSetting::max_joined_block_size_bytes] = max_joined_block_size_bytes;
    settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = temporary_files_buffer_size;
    settings[QueryPlanSerializationSetting::join_output_by_rowlist_perkey_rows_threshold] = join_output_by_rowlist_perkey_rows_threshold;
    settings[QueryPlanSerializationSetting::join_to_sort_minimum_perkey_rows] = join_to_sort_minimum_perkey_rows;
    settings[QueryPlanSerializationSetting::join_to_sort_maximum_table_rows] = join_to_sort_maximum_table_rows;
    settings[QueryPlanSerializationSetting::allow_experimental_join_right_table_sorting] = allow_experimental_join_right_table_sorting;
    settings[QueryPlanSerializationSetting::min_joined_block_size_rows] = min_joined_block_size_rows;
    settings[QueryPlanSerializationSetting::min_joined_block_size_bytes] = min_joined_block_size_bytes;

    settings[QueryPlanSerializationSetting::default_max_bytes_in_join] = default_max_bytes_in_join;

    settings[QueryPlanSerializationSetting::allow_dynamic_type_in_join_keys] = allow_dynamic_type_in_join_keys;
    settings[QueryPlanSerializationSetting::use_join_disjunctions_push_down] = use_join_disjunctions_push_down;
    settings[QueryPlanSerializationSetting::enable_lazy_columns_replication] = enable_lazy_columns_replication;

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

static void serializeNodeList(WriteBuffer & out, const std::unordered_map<const ActionsDAG::Node *, size_t> & node_to_id, const std::vector<JoinActionRef> & nodes)
{
    writeVarUInt(nodes.size(), out);
    for (const auto & action : nodes)
    {
        const auto * node = action.getNode();
        if (auto it = node_to_id.find(node); it != node_to_id.end())
            writeVarUInt(it->second, out);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find node '{}' in node map", node->result_name);
    }
}

void JoinOperator::serialize(WriteBuffer & out, const ActionsDAG * actions_dag) const
{
    auto node_to_id = actions_dag->getNodeToIdMap();
    serializeNodeList(out, node_to_id, expression);
    serializeNodeList(out, node_to_id, residual_filter);

    serializeJoinKind(kind, out);
    serializeJoinStrictness(strictness, out);
    serializeJoinLocality(locality, out);
}

static std::vector<JoinActionRef> deserializeNodeList(ReadBuffer & in, const ActionsDAG::NodeRawConstPtrs & id_to_node, JoinExpressionActions & expression_actions)
{
    size_t num_nodes;
    readVarUInt(num_nodes, in);

    size_t max_node_id = id_to_node.size();

    std::vector<JoinActionRef> result;
    result.reserve(num_nodes);

    for (size_t i = 0; i < num_nodes; ++i)
    {
        size_t node_id;
        readVarUInt(node_id, in);
        if (node_id >= max_node_id)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Node id {} is out of range, must be less than {}", node_id, max_node_id);

        result.emplace_back(id_to_node[node_id], expression_actions);
    }
    return result;
}

JoinOperator JoinOperator::deserialize(ReadBuffer & in, JoinExpressionActions & expression_actions)
{
    auto id_to_node = expression_actions.getActionsDAG()->getIdToNode();
    auto actions = deserializeNodeList(in, id_to_node, expression_actions);
    auto residual_filter = deserializeNodeList(in, id_to_node, expression_actions);

    auto kind = deserializeJoinKind(in);
    auto strictness = deserializeJoinStrictness(in);
    auto locality = deserializeJoinLocality(in);

    JoinOperator result(kind, strictness, locality);
    result.expression = std::move(actions);
    result.residual_filter = std::move(residual_filter);

    return result;
}

String JoinOperator::dump() const
{
    return fmt::format("JoinOperator(kind={}, strictness={}, locality={}, expression=[{}], residual_filter=[{}])",
        toString(kind), toString(strictness), toString(locality),
        fmt::join(expression | std::views::transform(&JoinActionRef::dump), ", "),
        fmt::join(residual_filter | std::views::transform(&JoinActionRef::dump), ", "));
}

}
