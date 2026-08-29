#include <vector>
#include <Interpreters/JoinOperator.h>

#include <Columns/IColumn.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

#include <fmt/ranges.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
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
    extern const SettingsBool parallel_non_joined_rows_processing;
    extern const SettingsUInt64 max_joined_block_size_rows;
    extern const SettingsUInt64 max_joined_block_size_bytes;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
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
    extern const SettingsBool enable_software_prefetch_in_join;
    extern const SettingsBool use_hash_table_stats_for_join_reordering;
    extern const SettingsUInt64 max_bytes_before_external_join;
    extern const SettingsDouble max_bytes_ratio_before_external_join;

    extern const SettingsBool enable_join_fixed_hash_table_conversion;
    extern const SettingsBool join_runtime_filter_from_fixed_hash_table;
    extern const SettingsBool enable_hash_join_row_store;
    extern const SettingsDouble min_rows_ratio_for_hash_join_row_store;
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

    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_join;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_join;

    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_set_to_optimize_join;

    extern const QueryPlanSerializationSettingsBool collect_hash_table_stats_during_joins;
    extern const QueryPlanSerializationSettingsUInt64 max_size_to_preallocate_for_joins;
    extern const QueryPlanSerializationSettingsUInt64 parallel_hash_join_threshold;

    extern const QueryPlanSerializationSettingsBool joined_block_split_single_row;
    extern const QueryPlanSerializationSettingsBool parallel_non_joined_rows_processing;
    extern const QueryPlanSerializationSettingsUInt64 max_joined_block_size_rows;
    extern const QueryPlanSerializationSettingsUInt64 max_joined_block_size_bytes;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsBool spill_codec_authorized;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
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
    extern const QueryPlanSerializationSettingsBool enable_software_prefetch_in_join;
    extern const QueryPlanSerializationSettingsBool use_hash_table_stats_for_join_reordering;

    extern const QueryPlanSerializationSettingsBool enable_join_fixed_hash_table_conversion;
    extern const QueryPlanSerializationSettingsBool join_runtime_filter_from_fixed_hash_table;
    extern const QueryPlanSerializationSettingsBool enable_hash_join_row_store;
    extern const QueryPlanSerializationSettingsDouble min_rows_ratio_for_hash_join_row_store;
}

JoinSettings::JoinSettings(const Settings & query_settings, JoinAnalyzeMode join_analyze_mode_, bool temporary_storage_available_)
    : temporary_storage_available(temporary_storage_available_)
    , join_analyze_mode(join_analyze_mode_)
{
    join_algorithms = query_settings[Setting::join_algorithm];

    max_block_size = query_settings[Setting::max_block_size];

    max_rows_in_join = query_settings[Setting::max_rows_in_join];
    max_bytes_in_join = query_settings[Setting::max_bytes_in_join];
    default_max_bytes_in_join = query_settings[Setting::default_max_bytes_in_join];

    joined_block_split_single_row = query_settings[Setting::joined_block_split_single_row];
    parallel_non_joined_rows_processing = query_settings[Setting::parallel_non_joined_rows_processing];
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

    max_bytes_before_external_join = query_settings[Setting::max_bytes_before_external_join];
    max_bytes_ratio_before_external_join = query_settings[Setting::max_bytes_ratio_before_external_join];

    max_rows_in_set_to_optimize_join = query_settings[Setting::max_rows_in_set_to_optimize_join];

    collect_hash_table_stats_during_joins = query_settings[Setting::collect_hash_table_stats_during_joins];
    max_size_to_preallocate_for_joins = query_settings[Setting::max_size_to_preallocate_for_joins];
    parallel_hash_join_threshold = query_settings[Setting::parallel_hash_join_threshold];

    temporary_files_codec = query_settings[Setting::temporary_files_codec];
    spill_codec_authorized = spillCodecAuthorizedBySession(query_settings);
    temporary_files_buffer_size = query_settings[Setting::temporary_files_buffer_size];
    join_output_by_rowlist_perkey_rows_threshold = query_settings[Setting::join_output_by_rowlist_perkey_rows_threshold];
    join_to_sort_minimum_perkey_rows = query_settings[Setting::join_to_sort_minimum_perkey_rows];
    join_to_sort_maximum_table_rows = query_settings[Setting::join_to_sort_maximum_table_rows];
    allow_experimental_join_right_table_sorting = query_settings[Setting::allow_experimental_join_right_table_sorting];

    allow_dynamic_type_in_join_keys = query_settings[Setting::allow_dynamic_type_in_join_keys];
    use_join_disjunctions_push_down = query_settings[Setting::use_join_disjunctions_push_down];
    enable_lazy_columns_replication = query_settings[Setting::enable_lazy_columns_replication];
    enable_software_prefetch_in_join = query_settings[Setting::enable_software_prefetch_in_join];

    if (temporary_files_buffer_size > 1_GiB)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large `temporary_files_buffer_size`, maximum 1 GiB");
    use_hash_table_stats_for_join_reordering = query_settings[Setting::use_hash_table_stats_for_join_reordering];

    enable_join_fixed_hash_table_conversion = query_settings[Setting::enable_join_fixed_hash_table_conversion];
    join_runtime_filter_from_fixed_hash_table = query_settings[Setting::join_runtime_filter_from_fixed_hash_table];
    enable_hash_join_row_store = query_settings[Setting::enable_hash_join_row_store];
    min_rows_ratio_for_hash_join_row_store = query_settings[Setting::min_rows_ratio_for_hash_join_row_store];
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

    max_bytes_before_external_join = settings[QueryPlanSerializationSetting::max_bytes_before_external_join];
    max_bytes_ratio_before_external_join = settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_join];

    max_rows_in_set_to_optimize_join = settings[QueryPlanSerializationSetting::max_rows_in_set_to_optimize_join];

    collect_hash_table_stats_during_joins = settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_joins];
    max_size_to_preallocate_for_joins = settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_joins];
    parallel_hash_join_threshold = settings[QueryPlanSerializationSetting::parallel_hash_join_threshold];

    joined_block_split_single_row = settings[QueryPlanSerializationSetting::joined_block_split_single_row];
    parallel_non_joined_rows_processing = settings[QueryPlanSerializationSetting::parallel_non_joined_rows_processing];
    max_joined_block_size_rows = settings[QueryPlanSerializationSetting::max_joined_block_size_rows];
    max_joined_block_size_bytes = settings[QueryPlanSerializationSetting::max_joined_block_size_bytes];
    temporary_files_codec = settings[QueryPlanSerializationSetting::temporary_files_codec];
    spill_codec_authorized = settings[QueryPlanSerializationSetting::spill_codec_authorized];
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
    enable_software_prefetch_in_join = settings[QueryPlanSerializationSetting::enable_software_prefetch_in_join];
    use_hash_table_stats_for_join_reordering = settings[QueryPlanSerializationSetting::use_hash_table_stats_for_join_reordering];

    enable_join_fixed_hash_table_conversion = settings[QueryPlanSerializationSetting::enable_join_fixed_hash_table_conversion];
    join_runtime_filter_from_fixed_hash_table = settings[QueryPlanSerializationSetting::join_runtime_filter_from_fixed_hash_table];
    enable_hash_join_row_store = settings[QueryPlanSerializationSetting::enable_hash_join_row_store];
    min_rows_ratio_for_hash_join_row_store = settings[QueryPlanSerializationSetting::min_rows_ratio_for_hash_join_row_store];
}

void JoinSettings::updatePlanSettings(QueryPlanSerializationSettings & settings, const JoinOperator & join_operator, UInt64 version) const
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

    settings[QueryPlanSerializationSetting::max_bytes_before_external_join] = max_bytes_before_external_join;
    settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_join] = max_bytes_ratio_before_external_join;

    settings[QueryPlanSerializationSetting::max_rows_in_set_to_optimize_join] = max_rows_in_set_to_optimize_join;

    settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_joins] = collect_hash_table_stats_during_joins;
    settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_joins] = max_size_to_preallocate_for_joins;
    settings[QueryPlanSerializationSetting::parallel_hash_join_threshold] = parallel_hash_join_threshold;

    settings[QueryPlanSerializationSetting::joined_block_split_single_row] = joined_block_split_single_row;
    settings[QueryPlanSerializationSetting::parallel_non_joined_rows_processing] = parallel_non_joined_rows_processing;
    settings[QueryPlanSerializationSetting::max_joined_block_size_rows] = max_joined_block_size_rows;
    settings[QueryPlanSerializationSetting::max_joined_block_size_bytes] = max_joined_block_size_bytes;
    settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    /// `spill_codec_authorized` is a plan-setting name older peers do not know, and
    /// `QueryPlanSerializationSettings::readBinary` throws on an unknown name, so it goes on the wire only
    /// when the spill behavior of this join actually depends on it: a join that can never reach temporary
    /// files (see `canSpillToTemporaryFiles`) never resolves the codec and must not carry the opt-in. See
    /// the matching comment in `AggregatingStep::serializeSettings` and
    /// `spillCodecAuthorizationMustBeSerialized`.
    /// The setting was added in serialization version 10. Older workers cannot safely execute a plan that
    /// can spill with an experimental codec because they would silently lose the opt-in.
    if (spillCodecAuthorizationMustBeSerialized(
            canSpillToTemporaryFiles(join_operator), spill_codec_authorized, temporary_files_codec))
    {
        if (version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXPERIMENTAL_SPILL_CODEC)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "An experimental temporary-files codec requires query plan serialization version >= {}",
                DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXPERIMENTAL_SPILL_CODEC);

        settings[QueryPlanSerializationSetting::spill_codec_authorized] = true;
    }
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
    settings[QueryPlanSerializationSetting::enable_software_prefetch_in_join] = enable_software_prefetch_in_join;
    settings[QueryPlanSerializationSetting::use_hash_table_stats_for_join_reordering] = use_hash_table_stats_for_join_reordering;

    settings[QueryPlanSerializationSetting::enable_join_fixed_hash_table_conversion] = enable_join_fixed_hash_table_conversion;
    settings[QueryPlanSerializationSetting::join_runtime_filter_from_fixed_hash_table] = join_runtime_filter_from_fixed_hash_table;
    settings[QueryPlanSerializationSetting::enable_hash_join_row_store] = enable_hash_join_row_store;
    settings[QueryPlanSerializationSetting::min_rows_ratio_for_hash_join_row_store] = min_rows_ratio_for_hash_join_row_store;
}

bool JoinSettings::joinAlgorithmAlwaysBuildsSomeJoin(JoinAlgorithm algorithm)
{
    /// The branches of `PlannerJoins::tryCreateJoin` for these entries end in an unconditional in-memory
    /// hash join, so `chooseJoinAlgorithm`'s first-buildable-wins loop never consults anything listed after
    /// them. The other entries build a join only when the shape admits their algorithm and otherwise let
    /// the loop move on (`direct` additionally needs a key-value storage, which is unknown here, so it
    /// conservatively does not terminate the loop).
    return algorithm == JoinAlgorithm::HASH || algorithm == JoinAlgorithm::PARALLEL_HASH
        || algorithm == JoinAlgorithm::DEFAULT || algorithm == JoinAlgorithm::AUTO
        || algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE;
}

bool JoinSettings::canSpillToTemporaryFiles(const JoinOperator & join_operator) const
{
    /// `ConstantJoin` (`CROSS`, comma and constant-predicate joins) is chosen before the algorithm list is
    /// consulted at all and streams the right table to disk as soon as the in-memory size limits would be
    /// exceeded — but only when the join shape admits a `ConstantJoin`: a join keyed by a genuine equality
    /// never reaches it, so for such a join the size limits alone cannot cause a spill.
    if (join_operator.canBecomeConstantJoin() && (max_rows_in_join != 0 || max_bytes_in_join != 0))
        return true;

    /// Both spilling implementations accept only some kind/strictness pairs, and both require the
    /// single-clause join shape (`TableJoin::oneDisjunct`), which a top-level disjunction never plans
    /// into; a join the implementation rejects falls back to an in-memory algorithm (or fails to plan)
    /// instead of spilling. `MergeJoin` additionally declines a join whose ON expression becomes a mixed
    /// join expression, because it never evaluates one.
    const bool single_clause_join = !join_operator.expressionIsTopLevelDisjunction();
    const bool spilling_hash_join_is_possible = single_clause_join && GraceHashJoin::isSupported(join_operator.kind, join_operator.strictness);
    const bool merge_join_is_possible = single_clause_join
        && !join_operator.buildsMixedJoinExpression()
        && MergeJoin::isSupported(join_operator.kind, join_operator.strictness);
    const bool merge_join_limit_is_set = max_rows_in_join != 0 || max_bytes_in_join != 0 || default_max_bytes_in_join != 0;
    const bool external_join_threshold_is_set = max_bytes_before_external_join != 0 || max_bytes_ratio_before_external_join != 0.;

    /// `chooseJoinAlgorithm` walks the algorithm list in order and the first algorithm that builds a join
    /// wins, so this walks the same list the same way: a spill-capable candidate that may be chosen makes
    /// the answer true, while an entry that always builds an in-memory join makes everything after it
    /// unreachable. Where this cannot decide exactly what the planner does, it errs towards true:
    /// under-emitting the opt-in would make a shard reject the codec at its first spill.
    for (auto algorithm : join_algorithms)
    {
        /// `prefer_partial_merge` tries `MergeJoin` (which writes the right table through
        /// `SortedBlocksWriter` once the in-memory size limits are hit) before falling back to the hash
        /// branch; `partial_merge` has no fallback; `auto` tries the hash branch first and otherwise wraps
        /// `MergeJoin` in `JoinSwitcher` (an in-memory hash join that converts to `MergeJoin` on the same
        /// limits). Only the kind/strictness pairs `MergeJoin` supports can end up there; e.g. a keyed
        /// `RIGHT ANY` join under `auto` falls back to an in-memory hash join.
        if ((algorithm == JoinAlgorithm::PARTIAL_MERGE || algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE
             || algorithm == JoinAlgorithm::AUTO)
            && merge_join_is_possible
            && merge_join_limit_is_set)
            return true;

        /// An external-join threshold converts the hash branch into a spilling hash join
        /// (`PlannerJoins::tryCreateJoin` and `ExpressionAnalyzer::createJoin` consult
        /// `max_bytes_before_external_join` for `hash` / `parallel_hash` / `default` / `auto`, and
        /// `prefer_partial_merge` falls back to the same branch); the other algorithms never look at it.
        /// The raw settings are tested rather than the effective threshold so the answer does not depend on
        /// the local memory limits of whoever asks.
        if (external_join_threshold_is_set
            && temporary_storage_available
            && spilling_hash_join_is_possible
            && (algorithm != JoinAlgorithm::PREFER_PARTIAL_MERGE || !merge_join_is_possible)
            && joinAlgorithmAlwaysBuildsSomeJoin(algorithm))
            return true;

        /// `grace_hash` always spills - when the join is one it can be built for at all.
        if (algorithm == JoinAlgorithm::GRACE_HASH && spilling_hash_join_is_possible)
            return true;

        if (joinAlgorithmAlwaysBuildsSomeJoin(algorithm))
            return false;
    }

    return false;
}

UInt64 JoinSettings::getMaxBytesBeforeExternalJoin(UInt64 max_bytes_before_external_join, double max_bytes_ratio_before_external_join)
{
    std::optional<UInt64> threshold;
    if (max_bytes_before_external_join != 0)
        threshold = max_bytes_before_external_join;

    if (max_bytes_ratio_before_external_join != 0.)
    {
        double ratio = max_bytes_ratio_before_external_join;
        if (ratio < 0 || ratio >= 1.)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting max_bytes_ratio_before_external_join should be >= 0 and < 1 ({:.3f})", ratio);

        auto available_system_memory = getMostStrictAvailableSystemMemory();
        if (available_system_memory.has_value())
        {
            UInt64 ratio_in_bytes = static_cast<UInt64>(static_cast<double>(*available_system_memory) * ratio);
            if (threshold)
                threshold = std::min(threshold.value(), ratio_in_bytes);
            else
                threshold = ratio_in_bytes;

            LOG_TRACE(getLogger("JoinSettings"), "Adjusting memory limit before external join with {} (ratio: {:.3f}, available system memory: {})",
                formatReadableSizeWithBinarySuffix(ratio_in_bytes),
                ratio,
                formatReadableSizeWithBinarySuffix(*available_system_memory));
        }
        else
        {
            LOG_TRACE(getLogger("JoinSettings"), "No system memory limits configured. Ignoring max_bytes_ratio_before_external_join");
        }
    }

    return threshold.value_or(0);
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

/// The test `addJoinPredicatesToTableJoin` (`JoinStepLogical.cpp`) uses to claim a condition as a
/// hash-join key: an equality whose operands come from the two different inputs.
static bool hasCrossSideEquality(const std::vector<JoinActionRef> & conditions)
{
    for (const auto & condition : conditions)
    {
        auto [op, lhs, rhs] = condition.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        if ((lhs.fromLeft() && rhs.fromRight()) || (lhs.fromRight() && rhs.fromLeft()))
            return true;
    }

    return false;
}

bool JoinOperator::canBecomeConstantJoin() const
{
    if (isCrossOrComma(kind))
        return true;

    /// A cross-side equality is claimed as a hash-join key (`addJoinPredicatesToTableJoin` in
    /// `JoinStepLogical.cpp`), so with one present the planning keeps at least one key clause: the
    /// predicate is not constant and the no-keys conversion to CROSS never fires, ruling `ConstantJoin`
    /// out.
    if (hasCrossSideEquality(expression))
        return false;

    /// A top-level disjunction splits into one clause per disjunct (`tryAddDisjunctiveConditions` in
    /// `JoinStepLogical.cpp`) when every disjunct carries its own key, keeping the join keyed; only a
    /// keyless disjunct makes the planning fall back to the conversion to CROSS.
    if (expressionIsTopLevelDisjunction())
    {
        for (const auto & disjunct : expression.front().getArguments())
        {
            auto conjuncts = disjunct.isFunction(JoinConditionOperator::And) ? disjunct.getArguments() : std::vector<JoinActionRef>{disjunct};
            if (!hasCrossSideEquality(conjuncts))
                return true;
        }

        return false;
    }

    /// Any other expression shape may still degenerate to a constant or convert to CROSS, so it
    /// conservatively keeps the answer true.
    return true;
}

bool JoinOperator::expressionIsTopLevelDisjunction() const
{
    /// The shape test of `tryAddDisjunctiveConditions` (`JoinStepLogical.cpp`). A disjunction that is one
    /// of several top-level conjuncts does not split the join: the other conjuncts still provide the keys
    /// of a single clause, and the disjunction becomes a filter or a residual condition.
    return expression.size() == 1 && expression.front().isFunction(JoinConditionOperator::Or);
}

bool JoinOperator::hasCrossSideEqualityCondition() const
{
    return hasCrossSideEquality(expression);
}

bool ieJoinCanCompareOperandTypes(const DataTypePtr & lhs_type, const DataTypePtr & rhs_type)
{
    auto comparison_is_incompatible = [](const DataTypePtr & type)
    {
        bool result = false;
        auto check = [&](const IDataType & t) { result |= isTuple(t) || isDynamic(t) || isVariant(t); };
        check(*type);
        if (!result)
            type->forEachChild(check);
        return result;
    };

    if (comparison_is_incompatible(lhs_type) || comparison_is_incompatible(rhs_type))
        return false;

    /// `tryExtractIEJoinDescription` casts both sides of the condition to a common type; a combination
    /// `predicateOperandsToCommonType` cannot handle is declined, so that the planning falls back to the
    /// generic handling (which compares such operands in a filter) instead of throwing.
    return lhs_type->equals(*rhs_type) || tryGetLeastSupertype(DataTypes{lhs_type, rhs_type}) != nullptr;
}

bool JoinOperator::hasCrossSideInequalityPair() const
{
    /// `tryGetIEJoinKeyCondition` (`JoinStepLogical.cpp`): an inequality whose operands come from the two
    /// different inputs and whose operand types the operator can compare.
    size_t count = 0;
    for (const auto & condition : expression)
    {
        auto [op, lhs, rhs] = condition.asBinaryPredicate();
        if (op != JoinConditionOperator::Less && op != JoinConditionOperator::LessOrEquals
            && op != JoinConditionOperator::Greater && op != JoinConditionOperator::GreaterOrEquals)
            continue;
        if (!(lhs.fromLeft() && rhs.fromRight()) && !(lhs.fromRight() && rhs.fromLeft()))
            continue;
        if (!ieJoinCanCompareOperandTypes(lhs.getType(), rhs.getType()))
            continue;
        if (++count >= 2)
            return true;
    }

    return false;
}

bool JoinOperator::canPushDownFromOn(std::optional<JoinTableSide> side) const
{
    switch (strictness)
    {
        case JoinStrictness::Any:
            /// We cannot push down to either side for ANY JOIN.
            /// Let's say we have LEFT ANY JOIN:
            /// 1. If we push down filter to the right side,
            /// we may filter out rows that would otherwise match the left side rows,
            /// resulting in different join results.
            /// 2. If we push down filter to the left side,
            /// we may filter out rows that should be included in the join result
            /// with defaults or NULLs.
            return false;
        case JoinStrictness::All:
        {
            /// Filter pushdown for PASTE JOIN is *disabled* to preserve positional alignment
            bool is_suitable_kind = kind == JoinKind::Inner
                || kind == JoinKind::Cross
                || kind == JoinKind::Comma
                || (side == JoinTableSide::Left && kind == JoinKind::Right)
                || (side == JoinTableSide::Right && kind == JoinKind::Left);

            return is_suitable_kind;
        }
        case JoinStrictness::Semi:
            /// We can push down to both sides for LEFT SEMI and RIGHT SEMI joins
            return side.has_value();
        case JoinStrictness::Anti:
            /// We can push down to only to opposite sides for LEFT ANTI and RIGHT ANTI joins
            /// See https://github.com/ClickHouse/ClickHouse/issues/93483
            return (side == JoinTableSide::Left && kind == JoinKind::Right)
                || (side == JoinTableSide::Right && kind == JoinKind::Left);
        default:
            /// TODO: Support RightAny strictness?
            return false;
    }
}

bool JoinOperator::hasSingleSidePreFilterCondition() const
{
    for (const auto & condition : expression)
    {
        /// `concatConditions(join_expression, side)` (`JoinStepLogical.cpp`), which builds the pre-filter
        /// condition of the clause, groups a condition over the left input or over no input at all with the
        /// left side; a condition over both inputs belongs to no side and stays in the ON clause.
        std::optional<JoinTableSide> side;
        if (condition.fromLeft() || condition.fromNone())
            side = JoinTableSide::Left;
        else if (condition.fromRight())
            side = JoinTableSide::Right;

        if (side && !canPushDownFromOn(side))
            return true;
    }

    return false;
}

bool JoinOperator::buildsMixedJoinExpression() const
{
    /// A condition left in the ON clause can be applied as a filter over the join result instead of being
    /// evaluated during the join, which is what the planning does whenever the kind and the strictness
    /// allow it (`build_mixed_join_expression` in `JoinStepLogical.cpp`).
    if (canPushDownFromOn())
        return false;

    /// An ASOF join claims its one cross-side inequality as the ASOF key (more than one is an error), so
    /// that condition does not end up in the mixed expression.
    bool asof_key_is_pending = strictness == JoinStrictness::Asof;

    for (const auto & condition : expression)
    {
        /// A condition over a single input (or over no input at all) becomes the pre-filter condition of
        /// the clause, not part of the mixed expression.
        if (condition.fromLeft() || condition.fromNone() || condition.fromRight())
            continue;

        auto [op, lhs, rhs] = condition.asBinaryPredicate();
        const bool operands_are_cross_side = (lhs.fromLeft() && rhs.fromRight()) || (lhs.fromRight() && rhs.fromLeft());

        /// Claimed as a hash-join key (`addJoinPredicatesToTableJoin`).
        if (operands_are_cross_side && (op == JoinConditionOperator::Equals || op == JoinConditionOperator::NullSafeEquals))
            continue;

        if (asof_key_is_pending && operands_are_cross_side
            && (op == JoinConditionOperator::Less || op == JoinConditionOperator::LessOrEquals
                || op == JoinConditionOperator::Greater || op == JoinConditionOperator::GreaterOrEquals))
        {
            asof_key_is_pending = false;
            continue;
        }

        return true;
    }

    return false;
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
    size_t num_nodes = 0;
    readVarUInt(num_nodes, in);

    size_t max_node_id = id_to_node.size();

    std::vector<JoinActionRef> result;
    result.reserve(num_nodes);

    for (size_t i = 0; i < num_nodes; ++i)
    {
        size_t node_id = 0;
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
