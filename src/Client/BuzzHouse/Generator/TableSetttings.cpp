#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static std::vector<std::string> merge_storage_policies;

std::map<std::string, CHSetting> mergeTreeTableSettings = {
    {"adaptive_write_buffer_initial_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }, {})},
    {"add_minmax_index_for_numeric_columns", CHSetting(trueOrFalse, {})},
    {"add_minmax_index_for_string_columns", CHSetting(trueOrFalse, {})},
    {"allow_experimental_block_number_column", CHSetting(trueOrFalse, {})},
    {"allow_experimental_replacing_merge_with_cleanup", CHSetting(trueOrFalse, {})},
    {"allow_floating_point_partition_key", CHSetting(trueOrFalse, {})},
    {"allow_remote_fs_zero_copy_replication", CHSetting(trueOrFalse, {})},
    {"allow_suspicious_indices", CHSetting(trueOrFalse, {})},
    {"allow_vertical_merges_from_compact_to_wide_parts", CHSetting(trueOrFalse, {})},
    {"always_fetch_merged_part", CHSetting(trueOrFalse, {})},
    {"always_use_copy_instead_of_hardlinks", CHSetting(trueOrFalse, {})},
    {"assign_part_uuids", CHSetting(trueOrFalse, {})},
    {"async_insert", CHSetting(trueOrFalse, {})},
    {"cache_populated_by_fetch", CHSetting(trueOrFalse, {})},
    {"check_sample_column_is_correct", CHSetting(trueOrFalse, {})},
    {"compact_parts_max_bytes_to_buffer",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.RandomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024))); },
         {})},
    {"compact_parts_max_granules_to_buffer",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.15, 0.15, 1, 256)); }, {})},
    {"compact_parts_merge_max_bytes_to_prefetch_part",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }, {})},
    {"compatibility_allow_sampling_expression_not_in_primary_key", CHSetting(trueOrFalse, {})},
    {"compress_marks", CHSetting(trueOrFalse, {})},
    {"compress_primary_key", CHSetting(trueOrFalse, {})},
    {"concurrent_part_removal_threshold",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 100)); }, {})},
    {"deduplicate_merge_projection_mode",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"detach_not_byte_identical_parts", CHSetting(trueOrFalse, {})},
    {"detach_old_local_parts_when_cloning_replica", CHSetting(trueOrFalse, {})},
    {"disable_detach_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {})},
    {"disable_fetch_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {})},
    {"disable_freeze_partition_for_zero_copy_replication", CHSetting(trueOrFalse, {})},
    {"enable_block_number_column", CHSetting(trueOrFalse, {})},
    {"enable_block_offset_column", CHSetting(trueOrFalse, {})},
    {"enable_index_granularity_compression", CHSetting(trueOrFalse, {})},
    {"enable_mixed_granularity_parts", CHSetting(trueOrFalse, {})},
    {"enable_vertical_merge_algorithm", CHSetting(trueOrFalse, {})},
    {"enforce_index_structure_match_on_partition_manipulation", CHSetting(trueOrFalse, {})},
    {"exclude_deleted_rows_for_part_size_in_merge", CHSetting(trueOrFalse, {})},
    {"force_read_through_cache_for_merges", CHSetting(trueOrFalse, {})},
    {"fsync_after_insert", CHSetting(trueOrFalse, {})},
    {"fsync_part_directory", CHSetting(trueOrFalse, {})},
    {"index_granularity",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"index_granularity_bytes",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }, {})},
    {"lightweight_mutation_projection_mode",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"load_existing_rows_count_for_old_parts", CHSetting(trueOrFalse, {})},
    {"marks_compress_block_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"materialize_ttl_recalculate_only", CHSetting(trueOrFalse, {})},
    {"max_bytes_to_merge_at_max_space_in_pool",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {})},
    {"max_bytes_to_merge_at_min_space_in_pool",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {})},
    {"max_file_name_length",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {})},
    {"max_number_of_mutations_for_replica",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 1, 100)); }, {})},
    {"max_parts_to_merge_at_once",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 1000)); }, {})},
    {"max_replicated_merges_in_queue",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }, {})},
    {"max_replicated_mutations_in_queue",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }, {})},
    {"merge_max_block_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"merge_max_block_size_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); },
         {})},
    {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", CHSetting(trueOrFalse, {})},
    {"merge_selector_window_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 8192)); }, {})},
    {"min_age_to_force_merge_on_partition_only", CHSetting(trueOrFalse, {})},
    {"min_bytes_for_full_part_storage",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024)); },
         {})},
    {"min_bytes_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {})},
    {"min_compressed_bytes_to_fsync_after_fetch",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); },
         {})},
    {"min_compressed_bytes_to_fsync_after_merge",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); },
         {})},
    {"min_index_granularity_bytes",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }, {})},
    {"min_merge_bytes_to_use_direct_io",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {})},
    {"min_parts_to_merge_at_once",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {})},
    {"min_rows_for_full_part_storage",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {})},
    {"min_rows_to_fsync_after_merge",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {})},
    {"min_rows_for_wide_part",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {})},
    {"non_replicated_deduplication_window",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {})},
    {"old_parts_lifetime",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60)); },
         {})},
    {"optimize_row_order", CHSetting(trueOrFalse, {})},
    {"prefer_fetch_merged_part_size_threshold",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {})},
    {"prewarm_mark_cache", CHSetting(trueOrFalse, {})},
    {"primary_key_compress_block_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"primary_key_lazy_load", CHSetting(trueOrFalse, {})},
    {"prewarm_primary_key_cache", CHSetting(trueOrFalse, {})},
    {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }, {})},
    {"ratio_of_defaults_for_sparse_serialization",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }, {})},
    {"remote_fs_zero_copy_path_compatible_mode", CHSetting(trueOrFalse, {})},
    {"remove_empty_parts", CHSetting(trueOrFalse, {})},
    {"remove_rolled_back_parts_immediately", CHSetting(trueOrFalse, {})},
    {"replace_long_file_name_to_hash", CHSetting(trueOrFalse, {})},
    {"replicated_can_become_leader", CHSetting(trueOrFalse, {})},
    {"replicated_max_mutations_in_one_entry",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000)); }, {})},
    {"shared_merge_tree_disable_merges_and_mutations_assignment", CHSetting(trueOrFalse, {})}, /* ClickHouse cloud */
    {"shared_merge_tree_parts_load_batch_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); },
         {})}, /* ClickHouse cloud */
    {"simultaneous_parts_removal_limit",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {})},
    {"ttl_only_drop_parts", CHSetting(trueOrFalse, {})},
    {"use_adaptive_write_buffer_for_dynamic_subcolumns", CHSetting(trueOrFalse, {})},
    {"use_async_block_ids_cache", CHSetting(trueOrFalse, {})},
    {"use_compact_variant_discriminators_serialization", CHSetting(trueOrFalse, {})},
    {"use_const_adaptive_granularity", CHSetting(trueOrFalse, {})},
    {"use_minimalistic_part_header_in_zookeeper", CHSetting(trueOrFalse, {})},
    {"use_primary_key_cache", CHSetting(trueOrFalse, {})},
    {"vertical_merge_algorithm_min_bytes_to_activate",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }, {})},
    {"vertical_merge_algorithm_min_columns_to_activate",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.8, 1, 16)); }, {})},
    {"vertical_merge_algorithm_min_rows_to_activate",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }, {})},
    {"vertical_merge_remote_filesystem_prefetch", CHSetting(trueOrFalse, {})}};

std::map<TableEngineValues, std::map<std::string, CHSetting>> allTableSettings
    = {{MergeTree, mergeTreeTableSettings},
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
       {Merge, {}}};

void loadFuzzerTableSettings(const FuzzConfig & fc)
{
    if (!fc.storage_policies.empty())
    {
        merge_storage_policies.insert(merge_storage_policies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        mergeTreeTableSettings.insert(
            {{"storage_policy",
              CHSetting(
                  [&](RandomGenerator & rg, std::string & ret)
                  {
                      ret += "'";
                      ret += rg.pickRandomlyFromVector(merge_storage_policies);
                      ret += "'";
                  },
                  {})}});
    }
}

}
