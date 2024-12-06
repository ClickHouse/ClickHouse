#include "RandomSettings.h"

namespace BuzzHouse
{

void setRandomSetting(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    std::string & ret,
    SetValue * set)
{
    std::string first;
    std::function<void(RandomGenerator &, std::string &)> second;
    std::tie(first, second) = rg.pickPairRandomlyFromMap(settings);

    set->set_property(first);
    ret.resize(0);
    second(rg, ret);
    set->set_value(ret);
}

static std::vector<std::string> merge_storage_policies;

std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> MergeTreeTableSettings
    = {{"adaptive_write_buffer_initial_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }},
       {"allow_experimental_block_number_column", trueOrFalse},
       {"allow_experimental_replacing_merge_with_cleanup", trueOrFalse},
       {"allow_floating_point_partition_key", trueOrFalse},
       {"allow_remote_fs_zero_copy_replication", trueOrFalse},
       {"allow_suspicious_indices", trueOrFalse},
       {"allow_vertical_merges_from_compact_to_wide_parts", trueOrFalse},
       {"always_fetch_merged_part", trueOrFalse},
       {"always_use_copy_instead_of_hardlinks", trueOrFalse},
       {"assign_part_uuids", trueOrFalse},
       {"async_insert", trueOrFalse},
       {"cache_populated_by_fetch", trueOrFalse},
       {"check_sample_column_is_correct", trueOrFalse},
       {"compact_parts_max_bytes_to_buffer",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.RandomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024))); }},
       {"compact_parts_max_granules_to_buffer",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.15, 0.15, 1, 256)); }},
       {"compact_parts_merge_max_bytes_to_prefetch_part",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }},
       {"compatibility_allow_sampling_expression_not_in_primary_key", trueOrFalse},
       {"compress_marks", trueOrFalse},
       {"compress_primary_key", trueOrFalse},
       {"concurrent_part_removal_threshold",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 100)); }},
       {"deduplicate_merge_projection_mode",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"detach_not_byte_identical_parts", trueOrFalse},
       {"detach_old_local_parts_when_cloning_replica", trueOrFalse},
       {"disable_detach_partition_for_zero_copy_replication", trueOrFalse},
       {"disable_fetch_partition_for_zero_copy_replication", trueOrFalse},
       {"disable_freeze_partition_for_zero_copy_replication", trueOrFalse},
       {"enable_block_number_column", trueOrFalse},
       {"enable_block_offset_column", trueOrFalse},
       {"enable_index_granularity_compression", trueOrFalse},
       {"enable_mixed_granularity_parts", trueOrFalse},
       {"enable_vertical_merge_algorithm", trueOrFalse},
       {"enforce_index_structure_match_on_partition_manipulation", trueOrFalse},
       {"exclude_deleted_rows_for_part_size_in_merge", trueOrFalse},
       {"force_read_through_cache_for_merges", trueOrFalse},
       {"fsync_after_insert", trueOrFalse},
       {"fsync_part_directory", trueOrFalse},
       {"index_granularity",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"index_granularity_bytes",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }},
       {"lightweight_mutation_projection_mode",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"load_existing_rows_count_for_old_parts", trueOrFalse},
       {"marks_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"materialize_ttl_recalculate_only", trueOrFalse},
       {"max_bytes_to_merge_at_max_space_in_pool",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"max_bytes_to_merge_at_min_space_in_pool",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"max_file_name_length",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"max_number_of_mutations_for_replica",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 1, 100)); }},
       {"max_parts_to_merge_at_once",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 1000)); }},
       {"max_replicated_merges_in_queue",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }},
       {"max_replicated_mutations_in_queue",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }},
       {"merge_max_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"merge_max_block_size_bytes",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", trueOrFalse},
       {"merge_selector_window_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 8192)); }},
       {"min_age_to_force_merge_on_partition_only", trueOrFalse},
       {"min_bytes_for_full_part_storage",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024)); }},
       {"min_bytes_for_wide_part",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); }},
       {"min_compressed_bytes_to_fsync_after_fetch",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"min_compressed_bytes_to_fsync_after_merge",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"min_index_granularity_bytes",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }},
       {"min_merge_bytes_to_use_direct_io",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"min_parts_to_merge_at_once",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"min_rows_for_full_part_storage",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"min_rows_to_fsync_after_merge",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"min_rows_for_wide_part",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"non_replicated_deduplication_window",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"old_parts_lifetime",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60)); }},
       {"optimize_row_order", trueOrFalse},
       {"prefer_fetch_merged_part_size_threshold",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"prewarm_mark_cache", trueOrFalse},
       {"primary_key_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"primary_key_lazy_load", trueOrFalse},
       {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }},
       {"ratio_of_defaults_for_sparse_serialization",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }},
       {"remote_fs_zero_copy_path_compatible_mode", trueOrFalse},
       {"remove_empty_parts", trueOrFalse},
       {"remove_rolled_back_parts_immediately", trueOrFalse},
       {"replace_long_file_name_to_hash", trueOrFalse},
       {"replicated_can_become_leader", trueOrFalse},
       {"replicated_max_mutations_in_one_entry",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000)); }},
       {"shared_merge_tree_disable_merges_and_mutations_assignment", trueOrFalse}, /* ClickHouse cloud */
       {"shared_merge_tree_parts_load_batch_size",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }}, /* ClickHouse cloud */
       {"simultaneous_parts_removal_limit",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"ttl_only_drop_parts", trueOrFalse},
       {"use_adaptive_write_buffer_for_dynamic_subcolumns", trueOrFalse},
       {"use_async_block_ids_cache", trueOrFalse},
       {"use_compact_variant_discriminators_serialization", trueOrFalse},
       {"use_const_adaptive_granularity", trueOrFalse},
       {"use_index_for_in_with_subqueries", trueOrFalse},
       {"use_minimalistic_part_header_in_zookeeper", trueOrFalse},
       {"vertical_merge_algorithm_min_bytes_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }},
       {"vertical_merge_algorithm_min_columns_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.8, 1, 16)); }},
       {"vertical_merge_algorithm_min_rows_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }},
       {"vertical_merge_remote_filesystem_prefetch", trueOrFalse}};

std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allTableSettings
    = {{MergeTree, MergeTreeTableSettings},
       {ReplacingMergeTree, MergeTreeTableSettings},
       {SummingMergeTree, MergeTreeTableSettings},
       {AggregatingMergeTree, MergeTreeTableSettings},
       {CollapsingMergeTree, MergeTreeTableSettings},
       {VersionedCollapsingMergeTree, MergeTreeTableSettings},
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
       {S3, S3TableSettings},
       {S3Queue, S3QueueTableSettings},
       {Hudi, {}},
       {DeltaLake, {}},
       {IcebergS3, {}}};

void loadFuzzerSettings(const FuzzConfig & fc)
{
    if (!fc.storage_policies.empty())
    {
        merge_storage_policies.insert(merge_storage_policies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        MergeTreeTableSettings.insert(
            {{"storage_policy",
              [&](RandomGenerator & rg, std::string & ret)
              {
                  ret += "'";
                  ret += rg.pickRandomlyFromVector(merge_storage_policies);
                  ret += "'";
              }}});
    }
}

}
