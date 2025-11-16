#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static DB::Strings merge_storage_policies;

static std::unordered_map<String, CHSetting> mergeTreeTableSettings = {
    {"adaptive_write_buffer_initial_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1, 32 * 1024 * 1024)); }, {}, false)},
    {"add_minmax_index_for_numeric_columns", CHSetting(trueOrFalse, {}, false)},
    {"add_minmax_index_for_string_columns", CHSetting(trueOrFalse, {}, false)},
    {"allow_experimental_block_number_column", CHSetting(trueOrFalse, {}, false)},
    {"allow_experimental_replacing_merge_with_cleanup", CHSetting(trueOrFalse, {}, false)},
    {"allow_floating_point_partition_key", CHSetting(trueOrFalse, {}, false)},
    {"allow_reduce_blocking_parts_task", CHSetting(trueOrFalse, {}, false)},
    {"allow_remote_fs_zero_copy_replication", CHSetting(trueOrFalse, {}, false)},
    {"allow_suspicious_indices", CHSetting(trueOrFalse, {}, false)},
    {"allow_vertical_merges_from_compact_to_wide_parts", CHSetting(trueOrFalse, {}, false)},
    {"always_fetch_merged_part", CHSetting(trueOrFalse, {}, false)},
    {"always_use_copy_instead_of_hardlinks", CHSetting(trueOrFalse, {}, false)},
    {"assign_part_uuids", CHSetting(trueOrFalse, {}, false)},
    {"async_insert", CHSetting(trueOrFalse, {}, false)},
    {"cache_populated_by_fetch", CHSetting(trueOrFalse, {}, false)},
    {"check_sample_column_is_correct", CHSetting(trueOrFalse, {}, false)},
    {"compact_parts_max_bytes_to_buffer",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"compact_parts_max_granules_to_buffer",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.15, 0.15, 1, 256)); }, {}, false)},
    {"compact_parts_merge_max_bytes_to_prefetch_part",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1, 32 * 1024 * 1024)); }, {}, false)},
    {"compatibility_allow_sampling_expression_not_in_primary_key", CHSetting(trueOrFalse, {}, false)},
    {"compress_marks", CHSetting(trueOrFalse, {}, false)},
    {"compress_primary_key", CHSetting(trueOrFalse, {}, false)},
    {"concurrent_part_removal_threshold",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 100)); }, {}, false)},
    {"deduplicate_merge_projection_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'throw'", "'drop'", "'rebuild'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"detach_not_byte_identical_parts", CHSetting(trueOrFalse, {}, false)},
    {"detach_old_local_parts_when_cloning_replica", CHSetting(trueOrFalse, {}, false)},
    {"disable_detach_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {}, false)},
    {"disable_fetch_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {}, false)},
    {"disable_freeze_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {}, false)},
    {"enable_block_number_column", CHSetting(trueOrFalse, {}, false)},
    {"enable_block_offset_column", CHSetting(trueOrFalse, {}, false)},
    {"enable_index_granularity_compression", CHSetting(trueOrFalse, {}, false)},
    {"enable_mixed_granularity_parts", CHSetting(trueOrFalse, {}, false)},
    {"enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", CHSetting(trueOrFalse, {}, false)},
    {"enable_vertical_merge_algorithm", CHSetting(trueOrFalse, {}, false)},
    {"enforce_index_structure_match_on_partition_manipulation", CHSetting(trueOrFalse, {}, false)},
    {"exclude_deleted_rows_for_part_size_in_merge", CHSetting(trueOrFalse, {}, false)},
    {"force_read_through_cache_for_merges", CHSetting(trueOrFalse, {}, false)},
    {"fsync_after_insert", CHSetting(trueOrFalse, {}, false)},
    {"fsync_part_directory", CHSetting(trueOrFalse, {}, false)},
    {"index_granularity",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
    {"index_granularity_bytes",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1024, 30 * 1024 * 1024)); }, {}, false)},
    {"lightweight_mutation_projection_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'throw'", "'drop'", "'rebuild'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"load_existing_rows_count_for_old_parts", CHSetting(trueOrFalse, {}, false)},
    {"marks_compress_block_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
    {"materialize_skip_indexes_on_merge", CHSetting(trueOrFalse, {}, false)},
    {"materialize_ttl_recalculate_only", CHSetting(trueOrFalse, {}, false)},
    {"max_bytes_to_merge_at_max_space_in_pool",
     CHSetting(
         [](RandomGenerator & rg)
         {
             return std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {},
         false)},
    {"max_bytes_to_merge_at_min_space_in_pool",
     CHSetting(
         [](RandomGenerator & rg)
         {
             return std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {},
         false)},
    {"max_file_name_length",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
    {"max_number_of_mutations_for_replica",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 1, 100)); }, {}, false)},
    {"max_parts_to_merge_at_once",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 1000)); }, {}, false)},
    {"max_replicated_merges_in_queue",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }, {}, false)},
    {"max_replicated_mutations_in_queue",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }, {}, false)},
    {"merge_max_block_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
    {"merge_max_block_size_bytes",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }, {}, false)},
    {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", CHSetting(trueOrFalse, {}, false)},
    {"merge_selector_window_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 8192)); }, {}, false)},
    {"min_age_to_force_merge_on_partition_only", CHSetting(trueOrFalse, {}, false)},
    {"min_bytes_for_full_part_storage",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024)); }, {}, false)},
    {"min_bytes_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg)
         { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"min_compressed_bytes_to_fsync_after_fetch",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }, {}, false)},
    {"min_compressed_bytes_to_fsync_after_merge",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }, {}, false)},
    {"min_index_granularity_bytes",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1024, 30 * 1024 * 1024)); }, {}, false)},
    {"min_merge_bytes_to_use_direct_io",
     CHSetting(
         [](RandomGenerator & rg)
         {
             return std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {},
         false)},
    {"min_parts_to_merge_at_once",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
    {"min_rows_for_full_part_storage",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
    {"min_rows_to_fsync_after_merge",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
    {"min_rows_for_wide_part",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
    {"non_replicated_deduplication_window",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
    {"old_parts_lifetime",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60)); }, {}, false)},
    {"optimize_row_order", CHSetting(trueOrFalse, {}, false)},
    {"prefer_fetch_merged_part_size_threshold",
     CHSetting(
         [](RandomGenerator & rg)
         {
             return std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {},
         false)},
    {"prewarm_mark_cache", CHSetting(trueOrFalse, {}, false)},
    {"primary_key_compress_block_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
    {"primary_key_lazy_load", CHSetting(trueOrFalse, {}, false)},
    {"prewarm_primary_key_cache", CHSetting(trueOrFalse, {}, false)},
    {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }, {}, false)},
    {"ratio_of_defaults_for_sparse_serialization",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }, {}, false)},
    {"remote_fs_zero_copy_path_compatible_mode", CHSetting(trueOrFalse, {}, false)},
    {"remove_empty_parts", CHSetting(trueOrFalse, {}, false)},
    {"remove_rolled_back_parts_immediately", CHSetting(trueOrFalse, {}, false)},
    {"replace_long_file_name_to_hash", CHSetting(trueOrFalse, {}, false)},
    {"replicated_can_become_leader", CHSetting(trueOrFalse, {}, false)},
    {"replicated_max_mutations_in_one_entry",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000)); }, {}, false)},
    /// ClickHouse cloud setting
    {"shared_merge_tree_disable_merges_and_mutations_assignment", CHSetting(trueOrFalse, {}, false)},
    /// ClickHouse cloud setting
    {"shared_merge_tree_parts_load_batch_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
    {"simultaneous_parts_removal_limit",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
    {"ttl_only_drop_parts", CHSetting(trueOrFalse, {}, false)},
    {"use_adaptive_write_buffer_for_dynamic_subcolumns", CHSetting(trueOrFalse, {}, false)},
    {"use_async_block_ids_cache", CHSetting(trueOrFalse, {}, false)},
    {"use_compact_variant_discriminators_serialization", CHSetting(trueOrFalse, {}, false)},
    {"use_const_adaptive_granularity", CHSetting(trueOrFalse, {}, false)},
    {"use_minimalistic_part_header_in_zookeeper", CHSetting(trueOrFalse, {}, false)},
    {"use_primary_key_cache", CHSetting(trueOrFalse, {}, false)},
    {"vertical_merge_algorithm_min_bytes_to_activate",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }, {}, false)},
    {"vertical_merge_algorithm_min_columns_to_activate",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.8, 1, 16)); }, {}, false)},
    {"vertical_merge_algorithm_min_rows_to_activate",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }, {}, false)},
    {"vertical_merge_remote_filesystem_prefetch", CHSetting(trueOrFalse, {}, false)}};

std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

std::unordered_map<String, CHSetting> restoreSettings
    = {{"allow_different_database_def", CHSetting(trueOrFalse, {}, false)},
       {"allow_different_table_def", CHSetting(trueOrFalse, {}, false)},
       {"allow_non_empty_tables", CHSetting(trueOrFalse, {}, false)},
       {"allow_s3_native_copy", CHSetting(trueOrFalse, {}, false)},
       {"async", CHSetting(trueOrFalse, {}, false)},
       {"internal", CHSetting(trueOrFalse, {}, false)},
       {"restore_broken_parts_as_detached", CHSetting(trueOrFalse, {}, false)},
       {"skip_unresolved_access_dependencies", CHSetting(trueOrFalse, {}, false)},
       {"structure_only", CHSetting(trueOrFalse, {}, false)},
       {"update_access_entities_dependents", CHSetting(trueOrFalse, {}, false)},
       {"use_same_password_for_base_backup", CHSetting(trueOrFalse, {}, false)},
       {"use_same_s3_credentials_for_base_backup", CHSetting(trueOrFalse, {}, false)}};

void loadFuzzerTableSettings(const FuzzConfig & fc)
{
    if (!fc.storage_policies.empty())
    {
        merge_storage_policies.insert(merge_storage_policies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        const auto & storage_policy
            = CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(merge_storage_policies) + "'"; }, {}, false);
        mergeTreeTableSettings.insert({{"storage_policy", storage_policy}});
        restoreSettings.insert({{"storage_policy", storage_policy}});
    }
    allTableSettings.insert(
        {{MergeTree, mergeTreeTableSettings},
         {ReplacingMergeTree, mergeTreeTableSettings},
         {SummingMergeTree, mergeTreeTableSettings},
         {AggregatingMergeTree, mergeTreeTableSettings},
         {CollapsingMergeTree, mergeTreeTableSettings},
         {VersionedCollapsingMergeTree, mergeTreeTableSettings},
         {File, fileTableSettings},
         {Null, {}},
         {Set, setTableSettings},
         {Join, joinTableSettings},
         {Memory, memoryTableSettings},
         {StripeLog, {}},
         {Log, {}},
         {TinyLog, {}},
         {EmbeddedRocksDB, embeddedRocksDBTableSettings},
         {Buffer, {}},
         {MySQL, mySQLTableSettings},
         {PostgreSQL, {}},
         {SQLite, {}},
         {MongoDB, {}},
         {Redis, {}},
         {S3, {}},
         {S3Queue, s3QueueTableSettings},
         {Hudi, {}},
         {DeltaLake, {}},
         {IcebergS3, {}},
         {Merge, {}},
         {Distributed, distributedTableSettings}});
}

}
