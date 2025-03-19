#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{

// clang-format off

#define PLAN_SERIALIZATION_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size in rows for reading", 0) \
    \
    DECLARE(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    DECLARE(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    DECLARE(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    DECLARE(UInt64, max_rows_to_sort, 0, "If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    DECLARE(UInt64, max_bytes_to_sort, 0, "If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    DECLARE(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    DECLARE(UInt64, prefer_external_sort_block_bytes, DEFAULT_BLOCK_SIZE * 256, "Prefer maximum block bytes for external sort, reduce the memory usage during merging.", 0) \
    DECLARE(UInt64, max_bytes_before_external_sort, 0, "If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    DECLARE(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    DECLARE(Float, remerge_sort_lowered_memory_bytes_ratio, 2., "If memory usage after remerge does not reduced by this ratio, remerge will be disabled.", 0) \
    DECLARE(UInt64, min_free_disk_space_for_temporary_data, 0, "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", 0) \
    \
    DECLARE(UInt64, aggregation_in_order_max_block_bytes, 50000000, "Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.", 0) \
    DECLARE(Bool, aggregation_in_order_memory_bound_merging, true, "Enable memory bound merging strategy when in-order is applied.", 0) \
    DECLARE(Bool, aggregation_sort_result_by_bucket_number, true, "Send intermediate aggregation result in order of bucket number.", 0) \
    \
    DECLARE(UInt64, max_rows_to_group_by, 0, "If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.", 0) \
    DECLARE(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    DECLARE(UInt64, group_by_two_level_threshold, 100000, "From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.", 0) \
    DECLARE(UInt64, group_by_two_level_threshold_bytes, 50000000, "From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.", 0) \
    DECLARE(UInt64, max_bytes_before_external_group_by, 0, "If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    DECLARE(Bool, empty_result_for_aggregation_by_empty_set, false, "Return empty result when aggregating without keys on empty set.", 0) \
    DECLARE(Bool, compile_aggregate_expressions, true, "Compile aggregate functions to native code.", 0) \
    DECLARE(UInt64, min_count_to_compile_aggregate_expression, 3, "The number of identical aggregate expressions before they are JIT-compiled", 0) \
    DECLARE(Bool, enable_software_prefetch_in_aggregation, true, "Enable use of software prefetch in aggregation", 0) \
    DECLARE(Bool, optimize_group_by_constant_keys, true, "Optimize GROUP BY when all keys in block are constant", 0) \
    DECLARE(Float, min_hit_rate_to_use_consecutive_keys_optimization, 0.5, "Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled", 0) \
    DECLARE(Bool, collect_hash_table_stats_during_aggregation, true, "Enable collecting hash table statistics to optimize memory allocation", 0) \
    DECLARE(UInt64, max_entries_for_hash_table_stats, 10'000, "How many entries hash table statistics collected during aggregation is allowed to have", 0) \
    DECLARE(UInt64, max_size_to_preallocate_for_aggregation, 100'000'000, "For how many elements it is allowed to preallocate space in all hash tables in total before aggregation", 0) \
    \
    DECLARE(TotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present.", IMPORTANT) \
    DECLARE(Float, totals_auto_threshold, 0.5, "The threshold for totals_mode = 'auto'.", 0) \
    \
    DECLARE(JoinAlgorithm, join_algorithm, "direct,parallel_hash,hash", "Specifies which JOIN algorithm is used.", 0) \
    \
    DECLARE(UInt64, max_rows_in_join, 0, "Maximum size of the hash table for JOIN (in number of rows).", 0) \
    DECLARE(UInt64, max_bytes_in_join, 0, "Maximum size of the hash table for JOIN (in number of bytes in memory).", 0) \
    DECLARE(UInt64, default_max_bytes_in_join, 1000000000, "Maximum size of right-side table if limit is required but max_bytes_in_join is not set.", 0) \
    DECLARE(UInt64, max_joined_block_size_rows, DEFAULT_BLOCK_SIZE, "Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.", 0) \
    DECLARE(UInt64, min_joined_block_size_bytes, 524288, "Minimum block size for JOIN result (if join algorithm supports it). 0 means unlimited.", 0) \
    \
    DECLARE(OverflowMode, join_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    DECLARE(Bool, join_any_take_last_row, false, "Changes the behaviour of join operations with `ANY` strictness.", 0) \
    \
    DECLARE(UInt64, cross_join_min_rows_to_compress, 10000000, "Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.", 0) \
    DECLARE(UInt64, cross_join_min_bytes_to_compress, 1_GiB, "Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.", 0) \
    \
    DECLARE(UInt64, partial_merge_join_left_table_buffer_bytes, 0, "If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.", 0) \
    DECLARE(UInt64, partial_merge_join_rows_in_right_blocks, 65536, "Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.", 0) \
    DECLARE(UInt64, join_on_disk_max_files_to_merge, 64, "Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk.", 0) \
    \
    DECLARE(NonZeroUInt64, grace_hash_join_initial_buckets, 1, "Initial number of grace hash join buckets", 0) \
    DECLARE(NonZeroUInt64, grace_hash_join_max_buckets, 1024, "Limit on the number of grace hash join buckets", 0) \
    \
    DECLARE(UInt64, max_rows_in_set_to_optimize_join, 0, "Maximal size of the set to filter joined tables by each other's row sets before joining.", 0) \
    DECLARE(String, temporary_files_codec, "LZ4", "Sets compression codec for temporary files used in sorting and joining operations on disk.", 0) \
    \
    DECLARE(Bool, collect_hash_table_stats_during_joins, true, "Enable collecting hash table statistics to optimize memory allocation", 0) \
    DECLARE(UInt64, max_size_to_preallocate_for_joins, 1'000'000'000'000, "For how many elements it is allowed to preallocate space in all hash tables in total before join", 0) \
    DECLARE(UInt64, join_output_by_rowlist_perkey_rows_threshold, 5, "The lower limit of per-key average rows in the right table to determine whether to output by row list in hash join.", 0) \
    DECLARE(Bool, allow_experimental_join_right_table_sorting, false, "If it is set to true, and the conditions of `join_to_sort_minimum_perkey_rows` and `join_to_sort_maximum_table_rows` are met, rerange the right table by key to improve the performance in left or inner hash join.", 0) \
    DECLARE(UInt64, join_to_sort_minimum_perkey_rows, 40, "The lower limit of per-key average rows in the right table to determine whether to rerange the right table by key in left or inner join. This setting ensures that the optimization is not applied for sparse table keys", 0) \
    DECLARE(UInt64, join_to_sort_maximum_table_rows, 10000, "The maximum number of rows in the right table to determine whether to rerange the right table by key in left or inner join.", 0) \
    \

// clang-format on

DECLARE_SETTINGS_TRAITS(QueryPlanSerializationSettingsTraits, PLAN_SERIALIZATION_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(QueryPlanSerializationSettingsTraits, PLAN_SERIALIZATION_SETTINGS)

struct QueryPlanSerializationSettingsImpl : public BaseSettings<QueryPlanSerializationSettingsTraits>
{
};


#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) QueryPlanSerializationSettings##TYPE NAME = &QueryPlanSerializationSettingsImpl ::NAME;

namespace QueryPlanSerializationSetting
{
PLAN_SERIALIZATION_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN


QueryPlanSerializationSettings::QueryPlanSerializationSettings() : impl(std::make_unique<QueryPlanSerializationSettingsImpl>())
{
}

QueryPlanSerializationSettings::QueryPlanSerializationSettings(const QueryPlanSerializationSettings & settings) : impl(std::make_unique<QueryPlanSerializationSettingsImpl>(*settings.impl))
{
}

QueryPlanSerializationSettings::~QueryPlanSerializationSettings() = default;

void QueryPlanSerializationSettings::writeChangedBinary(WriteBuffer & out) const
{
    impl->writeChangedBinary(out);
}
void QueryPlanSerializationSettings::readBinary(ReadBuffer & in)
{
    impl->readBinary(in);
}

QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

}
