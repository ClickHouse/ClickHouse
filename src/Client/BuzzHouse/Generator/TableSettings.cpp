#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{


static const auto bytesRangeSetting
    = CHSetting(bytesRange, {"0", "1", "2", "4", "8", "32", "1024", "2048", "4096", "16384", "'10M'"}, false);

static const auto bytesRangeNonZeroSetting
    = CHSetting(bytesRangeNonZero, {"1", "2", "4", "8", "32", "1024", "2048", "4096", "16384", "'10M'"}, false);

static const auto highRangeSetting
    = CHSetting(highRange, {"0", "1", "2", "4", "8", "32", "64", "1024", "2048", "4096", "16384", "'10M'"}, false);

static const auto highRangeNonZeroSetting
    = CHSetting(highRangeNonZero, {"1", "2", "4", "8", "32", "64", "1024", "2048", "4096", "16384"}, false);

/// Valid values: 0 (disabled) or >= 1024
static const auto indexGranularityBytesSetting = CHSetting(
    [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.nextBool() ? 0 : (rg.nextBool() ? 1024 : 10485760)); },
    {"0", "1024", "2048", "10485760"},
    false);

static const auto rowsRangeSetting
    = CHSetting(rowsRange, {"0", "1", "2", "4", "8", "32", "64", "1024", "2048", "4096", "16384", "'10M'"}, false);

static const auto bucketsRangeSetting = CHSetting(
    [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 16)); },
    {"0", "1", "2", "4", "8", "16"},
    false);

static const auto bucketsRangeNonZeroSetting = CHSetting(
    [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 16)); },
    {"1", "2", "4", "8", "16"},
    false);

static std::unordered_map<String, CHSetting> mergeTreeTableSettings = {
    {"adaptive_write_buffer_initial_size", bytesRangeNonZeroSetting},
    {"add_implicit_sign_column_constraint_for_collapsing_engine", trueOrFalseSetting},
    {"add_minmax_index_for_numeric_columns", trueOrFalseSetting},
    {"add_minmax_index_for_string_columns", trueOrFalseSetting},
    {"add_minmax_index_for_temporal_columns", trueOrFalseSetting},
    {"allow_coalescing_columns_in_partition_or_order_key", trueOrFalseSetting},
    {"allow_commit_order_projection", trueOrFalseSetting},
    {"allow_experimental_replacing_merge_with_cleanup", trueOrFalseSetting},
    {"allow_experimental_reverse_key", trueOrFalseSetting},
    {"allow_floating_point_partition_key", trueOrFalseSetting},
    {"allow_nullable_key", trueOrFalseSetting},
    {"allow_part_offset_column_in_projections", trueOrFalseSetting},
    {"allow_reduce_blocking_parts_task", trueOrFalseSetting},
    {"allow_remote_fs_zero_copy_replication", trueOrFalseSetting},
    {"allow_summing_columns_in_partition_or_order_key", trueOrFalseSetting},
    {"allow_suspicious_indices", trueOrFalseSetting},
    {"allow_vertical_merges_from_compact_to_wide_parts", trueOrFalseSetting},
    {"alter_column_secondary_index_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'throw'", "'drop'", "'rebuild'", "'compatibility'"};
             return rg.pickRandomly(choices);
         },
         {"'throw'", "'drop'", "'rebuild'", "'compatibility'"},
         false)},
    {"always_fetch_merged_part", trueOrFalseSetting},
    {"always_use_copy_instead_of_hardlinks", trueOrFalseSetting},
    {"apply_patches_on_merge", trueOrFalseSetting},
    {"assign_part_uuids", trueOrFalseSetting},
    {"async_insert", trueOrFalseSetting},
    {"auto_statistics_types",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return settingCombinations(rg, {"tdigest", "countmin", "minmax", "uniq"}); },
         {"'tdigest'", "'countmin'", "'minmax'", "'uniq'"},
         false)},
    {"cache_populated_by_fetch", trueOrFalseSetting},
    {"check_sample_column_is_correct", trueOrFalseSetting},
    {"cleanup_thread_preferred_points_per_iteration", rowsRangeSetting},
    {"cleanup_threads", threadSetting},
    {"clone_replica_zookeeper_create_get_part_batch_size", highRangeNonZeroSetting},
    {"columns_and_secondary_indices_sizes_lazy_calculation", trueOrFalseSetting},
    {"compact_parts_max_bytes_to_buffer", bytesRangeSetting},
    {"compact_parts_max_granules_to_buffer", highRangeNonZeroSetting},
    {"compact_parts_merge_max_bytes_to_prefetch_part", bytesRangeSetting},
    {"compatibility_allow_sampling_expression_not_in_primary_key", trueOrFalseSetting},
    {"compress_marks", trueOrFalseSetting},
    {"compress_primary_key", trueOrFalseSetting},
    {"concurrent_part_removal_threshold",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); }, {}, false)},
    {"deduplicate_merge_projection_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'ignore'", "'throw'", "'drop'", "'rebuild'"};
             return rg.pickRandomly(choices);
         },
         {"'ignore'", "'throw'", "'drop'", "'rebuild'"},
         false)},
    {"detach_not_byte_identical_parts", trueOrFalseSetting},
    {"detach_old_local_parts_when_cloning_replica", trueOrFalseSetting},
    {"disable_detach_partition_for_zero_copy_replication", trueOrFalseSetting},
    {"disable_fetch_partition_for_zero_copy_replication", trueOrFalseSetting},
    {"disable_freeze_partition_for_zero_copy_replication", trueOrFalseSetting},
    {"distributed_index_analysis_min_parts_to_activate",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); },
         {"0", "1"},
         false)},
    {"distributed_index_analysis_min_indexes_bytes_to_activate", bytesRangeSetting},
    {"distributed_index_analysis_min_indexes_size_to_activate",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); },
         {"0", "1"},
         false)},
    {"dynamic_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'v1'", "'v2'", "'v3'"};
             return rg.pickRandomly(choices);
         },
         {"'v1'", "'v2'", "'v3'"},
         false)},
    {"enable_block_number_column", trueOrFalseSetting},
    {"enable_block_offset_column", trueOrFalseSetting},
    {"enable_index_granularity_compression", trueOrFalseSetting},
    {"enable_max_bytes_limit_for_min_age_to_force_merge", trueOrFalseSetting},
    {"enable_mixed_granularity_parts", trueOrFalseSetting},
    {"enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", trueOrFalseSetting},
    {"enable_the_endpoint_id_with_zookeeper_name_prefix", trueOrFalseSetting},
    {"enable_vertical_merge_algorithm", trueOrFalseSetting},
    {"enforce_index_structure_match_on_partition_manipulation", trueOrFalseSetting},
    {"escape_index_filenames", trueOrFalseSetting},
    {"escape_variant_subcolumn_filenames", trueOrFalseSetting},
    {"exclude_deleted_rows_for_part_size_in_merge", trueOrFalseSetting},
    {"exclude_materialize_skip_indexes_on_merge",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             String res;
             std::vector<uint32_t> choices = {0, 1, 2, 3, 4};
             const uint32_t nchoices = rg.randomInt<uint32_t>(0, static_cast<uint32_t>(choices.size()));

             std::shuffle(choices.begin(), choices.end(), rg.generator);
             for (uint32_t i = 0; i < nchoices; i++)
             {
                 if (i != 0)
                 {
                     res += ",";
                 }
                 res += "i";
                 res += std::to_string(choices[i]);
             }
             return "'" + res + "'";
         },
         {},
         false)},
    {"finished_mutations_to_keep", rowsRangeSetting},
    {"force_read_through_cache_for_merges", trueOrFalseSetting},
    {"fsync_after_insert", trueOrFalseSetting},
    {"fsync_part_directory", trueOrFalseSetting},
    {"index_granularity", highRangeNonZeroSetting},
    {"index_granularity_bytes", indexGranularityBytesSetting},
    {"lightweight_mutation_projection_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'throw'", "'drop'", "'rebuild'"};
             return rg.pickRandomly(choices);
         },
         {"'throw'", "'drop'", "'rebuild'"},
         false)},
    {"load_existing_rows_count_for_old_parts", trueOrFalseSetting},
    {"map_buckets_coefficient", probRangeSetting},
    {"map_buckets_min_avg_size", rowsRangeSetting},
    {"map_buckets_strategy",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'sqrt'", "'linear'"};
             return rg.pickRandomly(choices);
         },
         {"'sqrt'", "'linear'"},
         false)},
    {"map_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'basic'", "'with_buckets'"};
             return rg.pickRandomly(choices);
         },
         {"'basic'", "'with_buckets'"},
         false)},
    {"map_serialization_version_for_zero_level_parts",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'basic'", "'with_buckets'", "'advanced'"};
             return rg.pickRandomly(choices);
         },
         {"'basic'", "'with_buckets'", "'advanced'"},
         false)},
    {"marks_compress_block_size", highRangeNonZeroSetting},
    {"materialize_skip_indexes_on_merge", trueOrFalseSetting},
    {"materialize_statistics_on_merge", trueOrFalseSetting},
    {"materialize_ttl_recalculate_only", trueOrFalseSetting},
    {"max_buckets_in_map",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 64)); },
         {"1", "2", "4", "8", "16", "32"},
         false)},
    {"max_bytes_to_merge_at_max_space_in_pool", bytesRangeSetting},
    {"max_bytes_to_merge_at_min_space_in_pool", bytesRangeSetting},
    {"max_compress_block_size", highRangeSetting},
    {"max_digestion_size_per_segment", bytesRangeSetting},
    {"max_file_name_length",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); }, {}, false)},
    {"max_files_to_modify_in_alter_columns",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 100)); }, {}, false)},
    {"max_files_to_remove_in_alter_columns",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 100)); }, {}, false)},
    {"max_merge_delayed_streams_for_parallel_write", threadSetting},
    {"max_number_of_merges_with_ttl_in_pool", threadSetting},
    {"max_number_of_mutations_for_replica",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 100)); }, {}, false)},
    {"max_part_loading_threads", threadSetting},
    {"max_part_removal_threads", threadSetting},
    {"max_parts_in_total", highRangeSetting},
    {"max_projections",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.3, 0.2, 0, 25)); },
         {"0", "1", "5", "25"},
         false)},
    {"max_parts_to_merge_at_once",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"max_replicated_merges_in_queue",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"max_replicated_mutations_in_queue",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"max_suspicious_broken_parts", highRangeSetting},
    {"max_suspicious_broken_parts_bytes", bytesRangeSetting},
    {"max_uncompressed_bytes_in_patches", bytesRangeSetting},
    {"merge_max_block_size", highRangeNonZeroSetting},
    {"merge_max_block_size_bytes", bytesRangeSetting},
    {"merge_max_bytes_to_prewarm_cache", bytesRangeSetting},
    {"merge_max_dynamic_subcolumns_in_compact_part",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"merge_max_dynamic_subcolumns_in_wide_part",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"merge_selector_algorithm",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'Simple'", "'Trivial'", "'StochasticSimple'"};
             return rg.pickRandomly(choices);
         },
         {"'Simple'", "'Trivial'", "'StochasticSimple'"},
         false)},
    {"merge_selector_base",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 8.0)); }, {}, false)},
    {"merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once", trueOrFalseSetting},
    {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", trueOrFalseSetting},
    {"merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"merge_selector_window_size", rowsRangeSetting},
    {"merge_total_max_bytes_to_prewarm_cache", bytesRangeSetting},
    {"min_age_to_force_merge_on_partition_only", trueOrFalseSetting},
    {"min_age_to_force_merge_seconds",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.3, 0.2, 0, 60)); },
         {"0", "1", "5", "10"},
         false)},
    {"min_bytes_for_compact_part", bytesRangeSetting},
    {"min_bytes_for_full_part_storage", bytesRangeSetting},
    {"min_bytes_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         { return std::to_string(rg.thresholdGenerator<uint64_t>(0.4, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); },
         {"0", "1"},
         false)},
    {"min_bytes_to_prewarm_caches", bytesRangeSetting},
    {"min_bytes_to_rebalance_partition_over_jbod", bytesRangeSetting},
    {"min_columns_to_activate_adaptive_write_buffer",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 100)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"min_compress_block_size", bytesRangeSetting},
    {"min_compressed_bytes_to_fsync_after_fetch", bytesRangeSetting},
    {"min_compressed_bytes_to_fsync_after_merge", bytesRangeSetting},
    {"min_index_granularity_bytes", bytesRangeSetting},
    /// ClickHouse cloud setting
    {"min_level_for_full_part_storage",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 10)); },
         {"0", "1"},
         false)},
    {"min_level_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 10)); },
         {"0", "1"},
         false)},
    {"min_marks_to_honor_max_concurrent_queries", highRangeSetting},
    {"min_merge_bytes_to_use_direct_io", bytesRangeSetting},
    {"min_parts_to_merge_at_once",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); },
         {"0", "1"},
         false)},
    {"min_rows_for_compact_part", rowsRangeSetting},
    {"min_rows_for_full_part_storage",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); },
         {"0", "1"},
         false)},
    {"min_rows_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.4, 0.2, 0, UINT32_C(8192))); },
         {"0", "1"},
         false)},
    {"min_rows_to_fsync_after_merge",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); },
         {"0", "1"},
         false)},
    {"non_replicated_deduplication_window", rowsRangeSetting},
    /// ClickHouse cloud setting
    {"notify_newest_block_number", trueOrFalseSetting},
    {"number_of_free_entries_in_pool_to_execute_mutation",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 30)); }, {}, false)},
    {"number_of_free_entries_in_pool_to_execute_optimize_entire_partition",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 30)); }, {}, false)},
    {"number_of_free_entries_in_pool_to_lower_max_size_of_merge",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 20)); }, {}, false)},
    {"number_of_mutations_to_delay",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 10, 1000)); },
         {},
         false)},
    {"number_of_mutations_to_throw",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 20, 2000)); },
         {},
         false)},
    {"object_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'v1'", "'v2'", "'v3'"};
             return rg.pickRandomly(choices);
         },
         {"'v1'", "'v2'", "'v3'"},
         false)},
    {"object_shared_data_buckets_for_compact_part", bucketsRangeNonZeroSetting},
    {"object_shared_data_buckets_for_wide_part", bucketsRangeNonZeroSetting},
    {"object_shared_data_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'map'", "'map_with_buckets'", "'advanced'"};
             return rg.pickRandomly(choices);
         },
         {"'map'", "'map_with_buckets'", "'advanced'"},
         false)},
    {"object_shared_data_serialization_version_for_zero_level_parts",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'map'", "'map_with_buckets'", "'advanced'"};
             return rg.pickRandomly(choices);
         },
         {"'map'", "'map_with_buckets'", "'advanced'"},
         false)},
    {"old_parts_lifetime",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 10, 8 * 60)); },
         {},
         false)},
    {"optimize_row_order", trueOrFalseSetting},
    {"prefer_fetch_merged_part_size_threshold", bytesRangeSetting},
    {"prewarm_mark_cache", trueOrFalseSetting},
    {"prewarm_primary_key_cache", trueOrFalseSetting},
    {"primary_key_compress_block_size", highRangeNonZeroSetting},
    {"primary_key_lazy_load", trueOrFalseSetting},
    {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", probRangeSetting},
    {"propagate_types_serialization_versions_to_nested_types", trueOrFalseSetting},
    {"ratio_of_defaults_for_sparse_serialization", CHSetting(probRange, {"0", "0.0001", "0.001", "0.003", "0.005", "0.01", "0.1"}, false)},
    {"refresh_parts_interval",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 5)); },
         {"0", "1", "2", "5"},
         false)},
    {"refresh_statistics_interval",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 5)); },
         {"0", "1", "2", "5"},
         false)},
    {"remote_fs_zero_copy_path_compatible_mode", trueOrFalseSetting},
    {"remove_empty_parts", trueOrFalseSetting},
    {"remove_rolled_back_parts_immediately", trueOrFalseSetting},
    {"remove_unused_patch_parts", trueOrFalseSetting},
    {"replace_long_file_name_to_hash", trueOrFalseSetting},
    {"replicated_can_become_leader", trueOrFalseSetting},
    {"replicated_deduplication_window",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); }, {}, false)},
    {"replicated_deduplication_window_seconds",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 3600)); }, {}, false)},
    {"replicated_deduplication_window_seconds_for_async_inserts",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 86400)); },
         {},
         false)},
    {"replicated_fetches_min_part_level",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 10)); },
         {"0", "1", "2", "4"},
         false)},
    {"replicated_fetches_min_part_level_timeout_seconds",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 600)); },
         {"0", "60", "300"},
         false)},
    {"replicated_max_mutations_in_one_entry",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 10000)); },
         {},
         false)},
    {"replicated_max_ratio_of_wrong_parts", probRangeSetting},
    {"search_orphaned_parts_disks",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'none'", "'local'", "'any'"};
             return rg.pickRandomly(choices);
         },
         {"'none'", "'local'", "'any'"},
         false)},
    {"serialization_info_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'basic'", "'with_types'"};
             return rg.pickRandomly(choices);
         },
         {"'basic'", "'with_types'"},
         false)},
    /// ClickHouse cloud setting
    {"shared_merge_tree_activate_coordinated_merges_tasks", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_create_per_replica_metadata_nodes", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_disable_merges_and_mutations_assignment", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_enable_automatic_empty_partitions_cleanup", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_enable_coordinated_merges", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_enable_keeper_parts_extra_data", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_enable_outdated_parts_check", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_max_outdated_parts_to_process_at_once", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_max_parts_update_leaders_in_total", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_max_parts_update_leaders_per_az", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_max_suspicious_broken_parts", rowsRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_max_suspicious_broken_parts_bytes", bytesRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_merge_coordinator_factor", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_merge_coordinator_max_merge_request_size", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_merge_coordinator_merges_prepare_count", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_outdated_parts_group_size", rowsRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations", probRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_parts_load_batch_size",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    /// ClickHouse cloud setting
    {"shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold", rowsRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_range_for_merge_window_size", highRangeSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_read_virtual_parts_from_leader", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_replica_set_max_lifetime_seconds",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 600)); },
         {"0", "60", "300"},
         false)},
    /// ClickHouse cloud setting
    {"shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_use_metadata_hints_cache", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_use_outdated_parts_compact_format", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_use_too_many_parts_count_from_virtual_parts", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_use_zookeeper_connection_pool", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"shared_merge_tree_virtual_parts_discovery_batch", rowsRangeSetting},
    {"simultaneous_parts_removal_limit",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); },
         {"0", "1", "2", "8", "10", "100"},
         false)},
    {"string_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'single_stream'", "'with_size_stream'"};
             return rg.pickRandomly(choices);
         },
         {"'single_stream'", "'with_size_stream'"},
         false)},
    {"nullable_serialization_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings choices = {"'basic'", "'allow_sparse'"};
             return rg.pickRandomly(choices);
         },
         {"'basic'", "'allow_sparse'"},
         false)},
    {"table_disk", trueOrFalseSetting},
    {"table_readonly", trueOrFalseSetting},
    {"ttl_only_drop_parts", trueOrFalseSetting},
    {"use_adaptive_write_buffer_for_dynamic_subcolumns", trueOrFalseSetting},
    {"use_async_block_ids_cache", trueOrFalseSetting},
    {"use_compact_variant_discriminators_serialization", trueOrFalseSetting},
    {"use_const_adaptive_granularity", trueOrFalseSetting},
    {"use_minimalistic_checksums_in_zookeeper", trueOrFalseSetting},
    {"use_minimalistic_part_header_in_zookeeper", trueOrFalseSetting},
    {"use_primary_key_cache", trueOrFalseSetting},
    {"vertical_merge_algorithm_min_bytes_to_activate", bytesRangeSetting},
    {"vertical_merge_algorithm_min_columns_to_activate",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 16)); },
         {"0", "1"},
         false)},
    {"vertical_merge_algorithm_min_rows_to_activate", rowsRangeSetting},
    {"vertical_merge_optimize_lightweight_delete", trueOrFalseSetting},
    {"vertical_merge_optimize_ttl_delete", trueOrFalseSetting},
    {"vertical_merge_remote_filesystem_prefetch", trueOrFalseSetting},
    {"write_marks_for_substreams_in_compact_parts", trueOrFalseSetting},
    {"zero_copy_concurrent_part_removal_max_postpone_ratio", probRangeSetting},
    {"zero_copy_concurrent_part_removal_max_split_times", highRangeSetting}};

std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allColumnSettings;

std::unordered_map<DictionaryLayouts, std::unordered_map<String, CHSetting>> allDictionaryLayoutSettings;

std::unordered_map<String, CHSetting> restoreSettings
    = {{"allow_azure_native_copy", trueOrFalseSettingNoOracle},
       {"allow_different_database_def", trueOrFalseSettingNoOracle},
       {"allow_different_table_def", trueOrFalseSettingNoOracle},
       {"allow_non_empty_tables", trueOrFalseSettingNoOracle},
       {"allow_s3_native_copy", trueOrFalseSettingNoOracle},
       {"async", trueOrFalseSettingNoOracle},
       {"internal", trueOrFalseSettingNoOracle},
       {"restore_broken_parts_as_detached", trueOrFalseSettingNoOracle},
       {"skip_unresolved_access_dependencies", trueOrFalseSettingNoOracle},
       {"structure_only", trueOrFalseSettingNoOracle},
       {"update_access_entities_dependents", trueOrFalseSettingNoOracle},
       {"use_same_password_for_base_backup", trueOrFalseSettingNoOracle},
       {"use_same_s3_credentials_for_base_backup", trueOrFalseSettingNoOracle}};

std::unordered_map<String, CHSetting> backupSettings
    = {{"allow_azure_native_copy", trueOrFalseSettingNoOracle},
       {"allow_backup_broken_projections", trueOrFalseSettingNoOracle},
       {"allow_checksums_from_remote_paths", trueOrFalseSettingNoOracle},
       {"allow_s3_native_copy", trueOrFalseSettingNoOracle},
       {"async", trueOrFalseSettingNoOracle},
       {"azure_attempt_to_create_container", trueOrFalseSettingNoOracle},
       {"backup_data_from_refreshable_materialized_view_targets", trueOrFalseSettingNoOracle},
       {"check_parts", trueOrFalseSettingNoOracle},
       {"check_projection_parts", trueOrFalseSettingNoOracle},
       {"decrypt_files_from_encrypted_disks", trueOrFalseSettingNoOracle},
       {"deduplicate_files", trueOrFalseSettingNoOracle},
       {"experimental_lightweight_snapshot", trueOrFalseSettingNoOracle},
       {"internal", trueOrFalseSettingNoOracle},
       {"read_from_filesystem_cache", trueOrFalseSettingNoOracle},
       {"s3_storage_class", CHSetting([](RandomGenerator &, FuzzConfig &) { return "'STANDARD'"; }, {}, false)},
       {"structure_only", trueOrFalseSettingNoOracle},
       {"write_access_entities_dependents", trueOrFalseSettingNoOracle}};

std::unordered_map<String, CHSetting> projectionSettings
    = {{"index_granularity", highRangeNonZeroSetting}, {"index_granularity_bytes", indexGranularityBytesSetting}};

static std::unordered_map<String, CHSetting> flatLayoutSettings
    = {{"INITIAL_ARRAY_SIZE", CHSetting(bytesRange, {}, false)}, {"MAX_ARRAY_SIZE", CHSetting(bytesRange, {}, false)}};

static std::unordered_map<String, CHSetting> hashedLayoutSettings
    = {{"MAX_LOAD_FACTOR", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "0.5" : "0.99"; }, {}, false)},
       {"SHARD_LOAD_QUEUE_BACKLOG",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                return std::to_string(
                    rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
            },
            {},
            false)},
       {"SHARDS", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {}, false)}};

static std::unordered_map<String, CHSetting> hashedArrayLayoutSettings
    = {{"SHARDS", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {}, false)}};

static std::unordered_map<String, CHSetting> rangeHashedLayoutSettings = {
    {"RANGE_LOOKUP_STRATEGY", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "'min'" : "'max'"; }, {}, false)}};

static std::unordered_map<String, CHSetting> cachedLayoutSettings
    = {{"ALLOW_READ_EXPIRED_KEYS", trueOrFalseSettingNoOracle},
       {"MAX_THREADS_FOR_UPDATES", threadSetting},
       {"MAX_UPDATE_QUEUE_SIZE",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                return std::to_string(
                    rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
            },
            {},
            false)}};

static std::unordered_map<String, CHSetting> ssdCachedLayoutSettings
    = {{"BLOCK_SIZE", CHSetting(bytesRange, {}, false)},
       {"FILE_SIZE", CHSetting(bytesRange, {}, false)},
       {"READ_BUFFER_SIZE", CHSetting(bytesRange, {}, false)},
       {"WRITE_BUFFER_SIZE", CHSetting(bytesRange, {}, false)}};

static std::unordered_map<String, CHSetting> ipTreeLayoutSettings = {{"ACCESS_TO_KEY_FROM_ATTRIBUTES", trueOrFalseSettingNoOracle}};

static std::unordered_map<String, CHSetting> dataLakeSettings
    = {{"iceberg_format_version",
        CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(1, 3)); }, {"1", "2", "3"}, false)},
       {"iceberg_recent_metadata_file_by_last_updated_ms_field", trueOrFalseSetting},
       {"iceberg_use_version_hint", trueOrFalseSetting}};

static std::unordered_map<String, CHSetting> fileTableSettings
    = {{"engine_file_allow_create_multiple_files", trueOrFalseSettingNoOracle},
       {"engine_file_empty_if_not_exists", trueOrFalseSettingNoOracle},
       {"engine_file_skip_empty_files", trueOrFalseSettingNoOracle},
       {"engine_file_truncate_on_insert", trueOrFalseSettingNoOracle},
       {"storage_file_read_method",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings choices = {"'read'", "'pread'", "'mmap'"};
                return rg.pickRandomly(choices);
            },
            {"'read'", "'pread'", "'mmap'"},
            false)}};

static std::unordered_map<String, CHSetting> distributedTableSettings
    = {{"background_insert_batch", trueOrFalseSetting},
       {"background_insert_split_batch_on_failure", trueOrFalseSetting},
       {"flush_on_detach", trueOrFalseSetting},
       {"fsync_after_insert", trueOrFalseSetting},
       {"fsync_directories", trueOrFalseSetting},
       {"skip_unavailable_shards", trueOrFalseSetting}};

static std::unordered_map<String, CHSetting> memoryTableSettings
    = {{"min_bytes_to_keep", CHSetting(bytesRange, {}, false)},
       {"max_bytes_to_keep", CHSetting(bytesRange, {}, false)},
       {"min_rows_to_keep", CHSetting(rowsRange, {}, false)},
       {"max_rows_to_keep", CHSetting(rowsRange, {}, false)},
       {"compress", trueOrFalseSetting}};

static std::unordered_map<String, CHSetting> setTableSettings = {{"persistent", trueOrFalseSettingNoOracle}};

static std::unordered_map<String, CHSetting> joinTableSettings = {{"persistent", trueOrFalseSettingNoOracle}};

static std::unordered_map<String, CHSetting> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", trueOrFalseSetting},
    {"bulk_insert_block_size", highRangeSetting},
};

static std::unordered_map<String, CHSetting> mySQLTableSettings = {
    {"connection_pool_size",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(UINT32_C(1) << rg.randomInt<uint32_t>(0, 6)); }, {}, false)},
    {"connection_max_tries",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(1, 16)); }, {}, false)},
    {"connection_auto_close", trueOrFalseSettingNoOracle}};

static std::unordered_map<String, CHSetting> kafkaTableSettings
    = {{"kafka_schema_registry_skip_bytes",
        CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 512)); }, {}, false)},
       {"kafka_num_consumers",
        CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 32)); }, {}, false)},
       {"kafka_max_block_size", CHSetting(highRange, {}, false)},
       {"kafka_skip_broken_messages", CHSetting(highRange, {}, false)},
       {"kafka_commit_every_batch", trueOrFalseSetting},
       {"kafka_client_id",
        CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 16)); }, {}, false)},
       {"kafka_poll_max_batch_size", CHSetting(highRange, {}, false)},
       {"kafka_thread_per_consumer", threadSetting},
       {"kafka_handle_error_mode",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings choices = {"'default'", "'stream'", "'dead_letter_queue'"};
                return rg.pickRandomly(choices);
            },
            {"'default'", "'stream'", "'dead_letter_queue'"},
            false)},
       {"kafka_commit_on_select", trueOrFalseSetting},
       {"kafka_max_rows_per_message", CHSetting(rowsRange, {}, false)},
       {"kafka_compression_codec",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings choices = {"''", "'none'", "'gzip'", "'snappy'", "'lz4'", "'zstd'"};
                return rg.pickRandomly(choices);
            },
            {"''", "'none'", "'gzip'", "'snappy'", "'lz4'", "'zstd'"},
            false)},
       {"kafka_compression_level",
        CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<int32_t>(-1, 12)); }, {}, false)}};

static std::unordered_map<String, CHSetting> mergeTreeColumnSettings
    = {{"min_compress_block_size", highRangeSetting}, {"max_compress_block_size", highRangeSetting}};

std::unordered_map<String, CHSetting> allDatabaseSettings = {{"lazy_load_tables", trueOrFalseSetting}};

void loadFuzzerTableSettings(const FuzzConfig & fc)
{
    std::unordered_set<String> codecsEscpated;
    std::unordered_map<String, CHSetting> s3Settings;
    std::unordered_map<String, CHSetting> s3QueueTableSettings;
    std::unordered_map<String, CHSetting> azureBlobStorageSettings;
    std::unordered_map<String, CHSetting> azureQueueSettings;
    std::unordered_map<String, CHSetting> logTableSettings;

    for (const auto & codec : codecs)
    {
        codecsEscpated.insert("'" + codec + "'");
    }
    mergeTreeTableSettings.insert(
        {{"default_compression_codec",
          CHSetting([](RandomGenerator & rg, FuzzConfig &) { return "'" + generateNextCodecString(rg) + "'"; }, codecsEscpated, false)}});

    /// marks and primary key codecs are passed to CompressionCodecFactory::get() directly
    /// (no type context), so only block-compression codecs are valid — no transform codecs.
    static const DB::Strings blockCodecs = {"LZ4", "LZ4HC", "ZSTD", "AES_128_GCM_SIV", "AES_256_GCM_SIV", "NONE"};
    std::unordered_set<String> blockCodecsEscaped;
    for (const auto & codec : blockCodecs)
    {
        blockCodecsEscaped.insert("'" + codec + "'");
    }
    const auto & blockCompressSetting = CHSetting(
        [](RandomGenerator & rg, FuzzConfig &)
        {
            const String & codec = rg.pickRandomly(blockCodecs);
            String res = codec;
            if (codec == "LZ4HC" && rg.nextBool())
                res += "(" + std::to_string(rg.randomInt<uint32_t>(0, 12)) + ")";
            else if (codec == "ZSTD" && rg.nextBool())
                res += "(" + std::to_string(rg.randomInt<uint32_t>(1, 22)) + ")";
            return "'" + res + "'";
        },
        blockCodecsEscaped,
        false);
    mergeTreeTableSettings.insert({{"marks_compression_codec", blockCompressSetting}});
    mergeTreeTableSettings.insert({{"primary_key_compression_codec", blockCompressSetting}});

    if (!fc.storage_policies.empty())
    {
        const auto & storage_policy_setting
            = CHSetting([&](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(fc.storage_policies) + "'"; }, {}, false);
        mergeTreeTableSettings.insert({{"storage_policy", storage_policy_setting}});
        logTableSettings.insert({{"storage_policy", storage_policy_setting}});
        restoreSettings.insert({{"storage_policy", storage_policy_setting}});
    }
    if (!fc.disks.empty())
    {
        const auto & disk_setting = CHSetting(
            [&](RandomGenerator & rg, FuzzConfig &) -> String
            {
                const auto & di = rg.pickRandomly(fc.disks);
                /// Inline local disk definition
                if (di.type == "Local" && rg.nextSmallNumber() < 4)
                {
                    String res = "disk(type = local, path = '" + di.path + "'";
                    const uint32_t space_opt = rg.nextSmallNumber();
                    if (space_opt < 3)
                        res += ", keep_free_space_bytes = "
                            + std::to_string(
                                   rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT64_C(1024) * UINT64_C(1024) * UINT64_C(1024)));
                    else if (space_opt < 5)
                        res += ", keep_free_space_ratio = " + std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0));
                    if (rg.nextBool())
                        res += ", thread_pool_size = " + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 128));
                    return res + ")";
                }
                /// Inline object_storage disk definition (only for Local object storage — no external credentials needed)
                if (di.type == "ObjectStorage" && di.object_storage_type == "Local" && rg.nextSmallNumber() < 4)
                {
                    String res = "disk(type = object_storage, object_storage_type = local_blob_storage, path = '" + di.path + "'";
                    /// metadata_type: local, plain, or plain_rewritable
                    if (rg.nextBool())
                    {
                        static const DB::Strings meta_types = {"local", "plain", "plain_rewritable"};
                        const String & mt = rg.pickRandomly(meta_types);
                        res += ", metadata_type = " + mt;
                        if (mt == "local" && rg.nextBool())
                            res += ", metadata_path = '" + di.path + "metadata/'";
                        if (rg.nextBool())
                            res += ", metadata_keep_free_space_bytes = "
                                + std::to_string(
                                       rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT64_C(1024) * UINT64_C(1024) * UINT64_C(1024)));
                    }
                    if (rg.nextSmallNumber() < 4)
                        res += ", enable_distributed_cache = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (fc.allow_transactions && rg.nextSmallNumber() < 4)
                        res += ", use_fake_transaction = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                        res += ", read_only = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                        res += ", remove_shared_recursive_file_limit = "
                            + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000));
                    if (rg.nextSmallNumber() < 4)
                        res += ", object_metadata_cache_size = "
                            + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT64_C(1024) * UINT64_C(1024)));
                    if (rg.nextSmallNumber() < 4)
                        res += ", key_compatibility_prefix = 'compat_" + di.name + "/'";
                    if (rg.nextSmallNumber() < 4)
                        res += ", thread_pool_size = " + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 128));
                    return res + ")";
                }
                /// Inline encrypted disk wrapping a non-encrypted local disk
                const auto enc_it = std::find_if(
                    fc.disks.begin(),
                    fc.disks.end(),
                    [&](const DiskInfo & d) { return !d.is_encrypted && d.type == "Local" && d.name != di.name; });
                if (!di.is_encrypted && !di.is_cached && enc_it != fc.disks.end() && rg.nextSmallNumber() < 3)
                {
                    struct
                    {
                        const char * algo;
                        size_t hex_len;
                    } static constexpr algos[] = {{"AES_128_CTR", 32}, {"AES_192_CTR", 48}, {"AES_256_CTR", 64}};
                    const auto & alg = algos[rg.randomInt<uint32_t>(0, 2)];
                    String key;
                    for (size_t i = 0; i < alg.hex_len / 16; i++)
                        key += fmt::format("{:016x}", rg.randomInt<uint64_t>(0, std::numeric_limits<uint64_t>::max()));
                    return "disk(type = encrypted, disk = '" + enc_it->name + "', path = '/var/lib/clickhouse/disks/encrypted_"
                        + enc_it->name + "/', algorithm = " + alg.algo + ", key_hex = " + key + ")";
                }
                /// Inline cache disk wrapping a non-cached local disk
                const auto cache_it = std::find_if(
                    fc.disks.begin(),
                    fc.disks.end(),
                    [&](const DiskInfo & d) { return !d.is_cached && d.type == "Local" && d.name != di.name; });
                if (!di.is_cached && cache_it != fc.disks.end() && rg.nextSmallNumber() < 3)
                {
                    String res = "disk(type = cache, disk = '" + cache_it->name + "', path = '/var/lib/clickhouse/disks/inline_cache_"
                        + cache_it->name + "/'";
                    res += ", max_size = '"
                        + std::to_string(rg.thresholdGenerator<uint64_t>(
                            0.2, 0.2, UINT64_C(1024) * UINT64_C(1024), UINT64_C(1024) * UINT64_C(1024) * UINT64_C(1024)))
                        + "'";
                    if (rg.nextSmallNumber() < 4)
                        res += ", cache_on_write_operations = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                    {
                        const bool slru = rg.nextBool();
                        res += ", cache_policy = " + String(slru ? "'SLRU'" : "'LRU'");
                        if (slru && rg.nextBool())
                            res += ", slru_size_ratio = " + std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.1, 0.9));
                    }
                    if (rg.nextSmallNumber() < 4)
                        res += ", max_elements = " + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT64_C(1000000)));
                    if (rg.nextSmallNumber() < 4)
                        res += ", max_file_segment_size = "
                            + std::to_string(rg.thresholdGenerator<uint64_t>(
                                0.2, 0.2, UINT64_C(1024), UINT64_C(1024) * UINT64_C(1024) * UINT64_C(128)));
                    if (rg.nextSmallNumber() < 4)
                        res += ", enable_filesystem_query_cache_limit = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                        res += ", load_metadata_asynchronously = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                        res += ", background_download_threads = " + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 16));
                    if (rg.nextSmallNumber() < 4)
                        res += ", allow_dynamic_cache_resize = " + std::to_string(static_cast<uint32_t>(rg.nextBool()));
                    if (rg.nextSmallNumber() < 4)
                        res += ", boundary_alignment = "
                            + std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, UINT64_C(512), UINT64_C(1024) * UINT64_C(1024)));
                    if (rg.nextSmallNumber() < 4)
                        res += ", keep_free_space_size_ratio = " + std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0));
                    if (rg.nextSmallNumber() < 4)
                        res += ", keep_free_space_elements_ratio = " + std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0));
                    return res + ")";
                }
                return "'" + di.name + "'";
            },
            {},
            false);
        mergeTreeTableSettings.insert({{"disk", disk_setting}});
        logTableSettings.insert({{"disk", disk_setting}});
        dataLakeSettings.insert({{"disk", disk_setting}});
        allDatabaseSettings.insert({{"disk", disk_setting}});
    }
    if (fc.enable_fault_injection_settings)
    {
        mergeTreeTableSettings.insert(
            {{"fault_probability_after_part_commit", CHSetting(probRange, {}, false)},
             {"fault_probability_before_part_commit", CHSetting(probRange, {}, false)},
             {"min_free_disk_bytes_to_perform_insert", CHSetting(bytesRange, {}, false)},
             {"min_free_disk_ratio_to_perform_insert", CHSetting(probRange, {}, false)}});
    }

    std::unordered_map<String, CHSetting> queueSettings
        = {{"after_processing",
            CHSetting(
                [](RandomGenerator & rg, FuzzConfig &)
                {
                    static const DB::Strings choices = {"'keep'", "'delete'", "'move'", "'tag'"};
                    return rg.pickRandomly(choices);
                },
                {"'keep'", "'delete'", "'move'", "'tag'"},
                false)},
           {"buckets",
            CHSetting(
                [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 3000)); },
                {"0", "1", "2", "8", "10", "100", "1000"},
                false)},
           {"bucketing_mode",
            CHSetting(
                [](RandomGenerator & rg, FuzzConfig &)
                {
                    static const DB::Strings choices = {"'path'", "'partition'"};
                    return rg.pickRandomly(choices);
                },
                {"'path'", "'partition'"},
                false)},
           {"commit_on_select", trueOrFalseSettingNoOracle},
           {"deduplication_v2", trueOrFalseSettingNoOracle},
           {"enable_hash_ring_filtering", trueOrFalseSetting},
           {"enable_logging_to_queue_log", trueOrFalseSetting},
           {"list_objects_batch_size",
            CHSetting(
                [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 3000)); },
                {"0", "1", "2", "8", "10", "100", "1000"},
                false)},
           {"max_processed_bytes_before_commit", CHSetting(bytesRange, {}, false)},
           {"max_processed_files_before_commit", CHSetting(rowsRange, {}, false)},
           {"max_processed_rows_before_commit", CHSetting(rowsRange, {}, false)},
           {"metadata_cache_size_bytes", CHSetting(bytesRange, {}, false)},
           {"metadata_cache_size_elements", CHSetting(rowsRange, {}, false)},
           {"min_insert_block_size_rows_for_materialized_views", CHSetting(rowsRange, {}, false)},
           {"min_insert_block_size_bytes_for_materialized_views", CHSetting(bytesRange, {}, false)},
           {"parallel_inserts", trueOrFalseSetting},
           {"partitioning_mode",
            CHSetting(
                [](RandomGenerator & rg, FuzzConfig &)
                {
                    static const DB::Strings choices = {"'none'", "'hive'", "'regex'"};
                    return rg.pickRandomly(choices);
                },
                {"'none'", "'hive'", "'regex'"},
                false)},
           {"processing_threads_num", threadSetting},
           {"tracked_files_limit", CHSetting(rowsRange, {}, false)},
           {"use_hive_partitioning", trueOrFalseSetting},
           {"use_persistent_processing_nodes", trueOrFalseSettingNoOracle}};
    s3QueueTableSettings.insert(s3Settings.begin(), s3Settings.end());
    s3QueueTableSettings.insert(queueSettings.begin(), queueSettings.end());
    /// s3queue_ settings are exclusive to S3Queue
    s3QueueTableSettings.insert(
        {{"s3queue_buckets", bucketsRangeSetting},
         {"s3queue_enable_logging_to_s3queue_log", trueOrFalseSetting},
         {"s3queue_processing_threads_num", threadSetting},
         {"s3queue_tracked_files_limit", CHSetting(rowsRange, {}, false)}});
    azureQueueSettings.insert(azureBlobStorageSettings.begin(), azureBlobStorageSettings.end());
    azureQueueSettings.insert(queueSettings.begin(), queueSettings.end());

    for (const auto & entry : fc.disallowed_settings)
    {
        mergeTreeTableSettings.erase(entry);
        restoreSettings.erase(entry);
        backupSettings.erase(entry);
        flatLayoutSettings.erase(entry);
        hashedLayoutSettings.erase(entry);
        hashedArrayLayoutSettings.erase(entry);
        rangeHashedLayoutSettings.erase(entry);
        cachedLayoutSettings.erase(entry);
        ssdCachedLayoutSettings.erase(entry);
        dataLakeSettings.erase(entry);
        fileTableSettings.erase(entry);
        distributedTableSettings.erase(entry);
        memoryTableSettings.erase(entry);
        setTableSettings.erase(entry);
        joinTableSettings.erase(entry);
        embeddedRocksDBTableSettings.erase(entry);
        kafkaTableSettings.erase(entry);
        mySQLTableSettings.erase(entry);
        mergeTreeColumnSettings.erase(entry);
        s3Settings.erase(entry);
        s3QueueTableSettings.erase(entry);
        azureBlobStorageSettings.erase(entry);
        azureQueueSettings.erase(entry);
        logTableSettings.erase(entry);
    }
    allTableSettings.insert(
        {{MergeTree, mergeTreeTableSettings},
         {ReplacingMergeTree, mergeTreeTableSettings},
         {CoalescingMergeTree, mergeTreeTableSettings},
         {SummingMergeTree, mergeTreeTableSettings},
         {AggregatingMergeTree, mergeTreeTableSettings},
         {CollapsingMergeTree, mergeTreeTableSettings},
         {VersionedCollapsingMergeTree, mergeTreeTableSettings},
         {GraphiteMergeTree, mergeTreeTableSettings},
         {File, fileTableSettings},
         {Null, {}},
         {Set, setTableSettings},
         {Join, joinTableSettings},
         {Memory, memoryTableSettings},
         {StripeLog, logTableSettings},
         {Log, logTableSettings},
         {TinyLog, logTableSettings},
         {EmbeddedRocksDB, embeddedRocksDBTableSettings},
         {Buffer, {}},
         {MySQL, mySQLTableSettings},
         {PostgreSQL, {}},
         {SQLite, {}},
         {MongoDB, {}},
         {Redis, {}},
         {S3, s3Settings},
         {S3Queue, s3QueueTableSettings},
         {Hudi, {}},
         {DeltaLakeS3, dataLakeSettings},
         {DeltaLakeAzure, dataLakeSettings},
         {DeltaLakeLocal, dataLakeSettings},
         {IcebergS3, dataLakeSettings},
         {IcebergAzure, dataLakeSettings},
         {IcebergLocal, dataLakeSettings},
         {Merge, {}},
         {Distributed, distributedTableSettings},
         {Dictionary, {}},
         {GenerateRandom, {}},
         {AzureBlobStorage, azureBlobStorageSettings},
         {AzureQueue, azureQueueSettings},
         {URL, {}},
         {KeeperMap, {}},
         {ExternalDistributed, {}},
         {MaterializedPostgreSQL, {}},
         {ArrowFlight, {}},
         {Alias, {}},
         {TimeSeries, {}},
         {HDFS, {}},
         {Hive, {}},
         {JDBC, {}},
         {Kafka, kafkaTableSettings},
         {NATS, {}},
         {ODBC, {}},
         {RabbitMQ, {}},
         {YTsaurus, {}},
         {Executable, {}},
         {ExecutablePool, {}},
         {FileLog, {}}});

    allColumnSettings.insert(
        {{MergeTree, mergeTreeColumnSettings},
         {ReplacingMergeTree, mergeTreeColumnSettings},
         {CoalescingMergeTree, mergeTreeColumnSettings},
         {SummingMergeTree, mergeTreeColumnSettings},
         {AggregatingMergeTree, mergeTreeColumnSettings},
         {CollapsingMergeTree, mergeTreeColumnSettings},
         {VersionedCollapsingMergeTree, mergeTreeColumnSettings},
         {GraphiteMergeTree, mergeTreeColumnSettings},
         {File, {}},
         {Null, {}},
         {Set, {}},
         {Join, {}},
         {Memory, {}},
         {StripeLog, {}},
         {Log, {}},
         {TinyLog, {}},
         {EmbeddedRocksDB, {}},
         {Buffer, {}},
         {MySQL, {}},
         {PostgreSQL, {}},
         {SQLite, {}},
         {MongoDB, {}},
         {Redis, {}},
         {S3, {}},
         {S3Queue, {}},
         {Hudi, {}},
         {DeltaLakeS3, {}},
         {DeltaLakeAzure, {}},
         {DeltaLakeLocal, {}},
         {IcebergS3, {}},
         {IcebergAzure, {}},
         {IcebergLocal, {}},
         {Merge, {}},
         {Distributed, {}},
         {Dictionary, {}},
         {GenerateRandom, {}},
         {AzureBlobStorage, {}},
         {AzureQueue, {}},
         {URL, {}},
         {KeeperMap, {}},
         {ExternalDistributed, {}},
         {MaterializedPostgreSQL, {}},
         {ArrowFlight, {}},
         {Alias, {}},
         {TimeSeries, {}},
         {HDFS, {}},
         {Hive, {}},
         {JDBC, {}},
         {Kafka, {}},
         {NATS, {}},
         {ODBC, {}},
         {RabbitMQ, {}},
         {YTsaurus, {}},
         {Executable, {}},
         {ExecutablePool, {}},
         {FileLog, {}}});

    allDictionaryLayoutSettings.insert(
        {{CACHE, cachedLayoutSettings},
         {COMPLEX_KEY_CACHE, cachedLayoutSettings},
         {COMPLEX_KEY_DIRECT, {}},
         {COMPLEX_KEY_HASHED, hashedLayoutSettings},
         {COMPLEX_KEY_HASHED_ARRAY, hashedArrayLayoutSettings},
         {COMPLEX_KEY_RANGE_HASHED, rangeHashedLayoutSettings},
         {COMPLEX_KEY_SPARSE_HASHED, hashedLayoutSettings},
         {COMPLEX_KEY_SSD_CACHE, ssdCachedLayoutSettings},
         {DIRECT, {}},
         {FLAT, flatLayoutSettings},
         {HASHED, hashedLayoutSettings},
         {HASHED_ARRAY, hashedArrayLayoutSettings},
         {IP_TRIE, ipTreeLayoutSettings},
         {RANGE_HASHED, rangeHashedLayoutSettings},
         {SPARSE_HASHED, hashedLayoutSettings},
         {SSD_CACHE, ssdCachedLayoutSettings}});

    for (const auto & entry : fc.hot_table_settings)
    {
        if (!mergeTreeTableSettings.contains(entry))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown MergeTree table setting: {}", entry);
        }
    }
}

}
