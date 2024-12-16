#pragma once

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/Defines.h>


namespace DB
{

#define PLAN_SERIALIZATION_SETTINGS(M, ALIAS) \
    M(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size in rows for reading", 0) \
    \
    M(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    M(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    M(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_to_sort, 0, "If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(UInt64, max_bytes_to_sort, 0, "If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, prefer_external_sort_block_bytes, DEFAULT_BLOCK_SIZE * 256, "Prefer maximum block bytes for external sort, reduce the memory usage during merging.", 0) \
    M(UInt64, max_bytes_before_external_sort, 0, "If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    M(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    M(Float, remerge_sort_lowered_memory_bytes_ratio, 2., "If memory usage after remerge does not reduced by this ratio, remerge will be disabled.", 0) \
    M(UInt64, min_free_disk_space_for_temporary_data, 0, "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", 0) \
    \
    M(UInt64, aggregation_in_order_max_block_bytes, 50000000, "Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.", 0) \
    M(Bool, aggregation_in_order_memory_bound_merging, true, "Enable memory bound merging strategy when in-order is applied.", 0) \
    M(Bool, aggregation_sort_result_by_bucket_number, true, "Send intermediate aggregation result in order of bucket number.", 0) \
    \
    M(UInt64, max_rows_to_group_by, 0, "If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.", 0) \
    M(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, group_by_two_level_threshold, 100000, "From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.", 0) \
    M(UInt64, group_by_two_level_threshold_bytes, 50000000, "From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.", 0) \
    M(UInt64, max_bytes_before_external_group_by, 0, "If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    M(Bool, empty_result_for_aggregation_by_empty_set, false, "Return empty result when aggregating without keys on empty set.", 0) \
    M(Bool, compile_aggregate_expressions, true, "Compile aggregate functions to native code.", 0) \
    M(UInt64, min_count_to_compile_aggregate_expression, 3, "The number of identical aggregate expressions before they are JIT-compiled", 0) \
    M(Bool, enable_software_prefetch_in_aggregation, true, "Enable use of software prefetch in aggregation", 0) \
    M(Bool, optimize_group_by_constant_keys, true, "Optimize GROUP BY when all keys in block are constant", 0) \
    M(Float, min_hit_rate_to_use_consecutive_keys_optimization, 0.5, "Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled", 0) \
    M(Bool, collect_hash_table_stats_during_aggregation, true, "Enable collecting hash table statistics to optimize memory allocation", 0) \
    M(UInt64, max_entries_for_hash_table_stats, 10'000, "How many entries hash table statistics collected during aggregation is allowed to have", 0) \
    M(UInt64, max_size_to_preallocate_for_aggregation, 100'000'000, "For how many elements it is allowed to preallocate space in all hash tables in total before aggregation", 0) \
    \
    M(TotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.", IMPORTANT) \
    M(Float, totals_auto_threshold, 0.5, "The threshold for totals_mode = 'auto'.", 0) \
    \


DECLARE_SETTINGS_TRAITS(QueryPlanSerializationSettingsTraits, PLAN_SERIALIZATION_SETTINGS)

struct QueryPlanSerializationSettings : public BaseSettings<QueryPlanSerializationSettingsTraits>
{
    QueryPlanSerializationSettings() = default;
};

}
