#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static DB::Strings mergeStoragePolicies;

static std::unordered_map<String, CHSetting> mergeTreeTableSettings
    = {{"adaptive_write_buffer_initial_size", CHSetting(bytesRange, {}, false)},
       {"add_implicit_sign_column_constraint_for_collapsing_engine", CHSetting(trueOrFalse, {}, false)},
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
       {"columns_and_secondary_indices_sizes_lazy_calculation", CHSetting(trueOrFalse, {}, false)},
       {"compact_parts_max_bytes_to_buffer", CHSetting(bytesRange, {}, false)},
       {"compact_parts_max_granules_to_buffer", CHSetting(highRange, {}, false)},
       {"compact_parts_merge_max_bytes_to_prefetch_part", CHSetting(bytesRange, {}, false)},
       {"compatibility_allow_sampling_expression_not_in_primary_key", CHSetting(trueOrFalse, {}, false)},
       {"compress_marks", CHSetting(trueOrFalse, {}, false)},
       {"compress_primary_key", CHSetting(trueOrFalse, {}, false)},
       {"concurrent_part_removal_threshold",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 100)); }, {}, false)},
       {"deduplicate_merge_projection_mode",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'ignore'", "'throw'", "'drop'", "'rebuild'"};
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
       {"enable_max_bytes_limit_for_min_age_to_force_merge", CHSetting(trueOrFalse, {}, false)},
       {"enable_mixed_granularity_parts", CHSetting(trueOrFalse, {}, false)},
       {"enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", CHSetting(trueOrFalse, {}, false)},
       {"enable_the_endpoint_id_with_zookeeper_name_prefix", CHSetting(trueOrFalse, {}, false)},
       {"enable_vertical_merge_algorithm", CHSetting(trueOrFalse, {}, false)},
       {"enforce_index_structure_match_on_partition_manipulation", CHSetting(trueOrFalse, {}, false)},
       {"exclude_deleted_rows_for_part_size_in_merge", CHSetting(trueOrFalse, {}, false)},
       {"force_read_through_cache_for_merges", CHSetting(trueOrFalse, {}, false)},
       {"fsync_after_insert", CHSetting(trueOrFalse, {}, false)},
       {"fsync_part_directory", CHSetting(trueOrFalse, {}, false)},
       {"index_granularity", CHSetting(highRange, {}, false)},
       {"index_granularity_bytes", CHSetting(bytesRange, {}, false)},
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
       {"marks_compress_block_size", CHSetting(highRange, {}, false)},
       {"materialize_skip_indexes_on_merge", CHSetting(trueOrFalse, {}, false)},
       {"materialize_ttl_recalculate_only", CHSetting(trueOrFalse, {}, false)},
       {"max_bytes_to_merge_at_max_space_in_pool", CHSetting(bytesRange, {}, false)},
       {"max_bytes_to_merge_at_min_space_in_pool", CHSetting(bytesRange, {}, false)},
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
       {"merge_max_block_size", CHSetting(highRange, {}, false)},
       {"merge_max_block_size_bytes", CHSetting(bytesRange, {}, false)},
       {"merge_selector_algorithm",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'Simple'", "'Trivial'", "'StochasticSimple'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", CHSetting(trueOrFalse, {}, false)},
       {"merge_selector_window_size", CHSetting(rowsRange, {}, false)},
       {"min_age_to_force_merge_on_partition_only", CHSetting(trueOrFalse, {}, false)},
       {"min_bytes_for_full_part_storage", CHSetting(bytesRange, {}, false)},
       {"min_bytes_for_wide_part", CHSetting(bytesRange, {}, false)},
       {"min_compressed_bytes_to_fsync_after_fetch", CHSetting(bytesRange, {}, false)},
       {"min_compressed_bytes_to_fsync_after_merge", CHSetting(bytesRange, {}, false)},
       {"min_index_granularity_bytes", CHSetting(bytesRange, {}, false)},
       {"min_merge_bytes_to_use_direct_io", CHSetting(bytesRange, {}, false)},
       {"min_parts_to_merge_at_once",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
       {"min_rows_for_full_part_storage",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
       {"min_rows_to_fsync_after_merge",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }, {}, false)},
       {"min_rows_for_wide_part", CHSetting(rowsRange, {}, false)},
       {"non_replicated_deduplication_window", CHSetting(rowsRange, {}, false)},
       {"notify_newest_block_number", CHSetting(trueOrFalse, {}, false)},
       {"old_parts_lifetime",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60)); }, {}, false)},
       {"optimize_row_order", CHSetting(trueOrFalse, {}, false)},
       {"prefer_fetch_merged_part_size_threshold", CHSetting(bytesRange, {}, false)},
       {"prewarm_mark_cache", CHSetting(trueOrFalse, {}, false)},
       {"prewarm_primary_key_cache", CHSetting(trueOrFalse, {}, false)},
       {"primary_key_compress_block_size", CHSetting(highRange, {}, false)},
       {"primary_key_lazy_load", CHSetting(trueOrFalse, {}, false)},
       {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", CHSetting(probRange, {}, false)},
       {"ratio_of_defaults_for_sparse_serialization", CHSetting(probRange, {}, false)},
       {"remote_fs_zero_copy_path_compatible_mode", CHSetting(trueOrFalse, {}, false)},
       {"remove_empty_parts", CHSetting(trueOrFalse, {}, false)},
       {"remove_rolled_back_parts_immediately", CHSetting(trueOrFalse, {}, false)},
       {"replace_long_file_name_to_hash", CHSetting(trueOrFalse, {}, false)},
       {"replicated_can_become_leader", CHSetting(trueOrFalse, {}, false)},
       {"replicated_max_mutations_in_one_entry",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000)); }, {}, false)},
       {"replicated_max_ratio_of_wrong_parts", CHSetting(probRange, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_create_per_replica_metadata_nodes", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_disable_merges_and_mutations_assignment", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_enable_keeper_parts_extra_data", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_enable_outdated_parts_check", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations", CHSetting(probRange, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_parts_load_batch_size",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_read_virtual_parts_from_leader", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_metadata_hints_cache", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_outdated_parts_compact_format", CHSetting(trueOrFalse, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_too_many_parts_count_from_virtual_parts", CHSetting(trueOrFalse, {}, false)},
       {"simultaneous_parts_removal_limit",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }, {}, false)},
       {"table_disk", CHSetting(trueOrFalse, {}, false)},
       {"ttl_only_drop_parts", CHSetting(trueOrFalse, {}, false)},
       {"use_adaptive_write_buffer_for_dynamic_subcolumns", CHSetting(trueOrFalse, {}, false)},
       {"use_async_block_ids_cache", CHSetting(trueOrFalse, {}, false)},
       {"use_compact_variant_discriminators_serialization", CHSetting(trueOrFalse, {}, false)},
       {"use_const_adaptive_granularity", CHSetting(trueOrFalse, {}, false)},
       {"use_minimalistic_checksums_in_zookeeper", CHSetting(trueOrFalse, {}, false)},
       {"use_minimalistic_part_header_in_zookeeper", CHSetting(trueOrFalse, {}, false)},
       {"use_primary_key_cache", CHSetting(trueOrFalse, {}, false)},
       {"vertical_merge_algorithm_min_bytes_to_activate", CHSetting(bytesRange, {}, false)},
       {"vertical_merge_algorithm_min_columns_to_activate",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.8, 1, 16)); }, {}, false)},
       {"vertical_merge_algorithm_min_rows_to_activate", CHSetting(bytesRange, {}, false)},
       {"vertical_merge_remote_filesystem_prefetch", CHSetting(trueOrFalse, {}, false)},
       {"zero_copy_concurrent_part_removal_max_postpone_ratio", CHSetting(probRange, {}, false)}};

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

std::unordered_map<DictionaryLayouts, std::unordered_map<String, CHSetting>> allDictionaryLayoutSettings;

std::unordered_map<String, CHSetting> flatLayoutSettings
    = {{"INITIAL_ARRAY_SIZE", CHSetting(bytesRange, {}, false)}, {"MAX_ARRAY_SIZE", CHSetting(bytesRange, {}, false)}};

std::unordered_map<String, CHSetting> hashedLayoutSettings
    = {{"MAX_LOAD_FACTOR", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "0.5" : "0.99"; }, {}, false)},
       {"SHARD_LOAD_QUEUE_BACKLOG",
        CHSetting(
            [](RandomGenerator & rg)
            {
                return std::to_string(
                    rg.thresholdGenerator<uint32_t>(0.25, 0.25, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
            },
            {},
            false)},
       {"SHARDS", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {}, false)}};

std::unordered_map<String, CHSetting> hashedArrayLayoutSettings
    = {{"SHARDS", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {}, false)}};

std::unordered_map<String, CHSetting> rangeHashedLayoutSettings
    = {{"RANGE_LOOKUP_STRATEGY", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'min'" : "'max'"; }, {}, false)}};

std::unordered_map<String, CHSetting> cachedLayoutSettings
    = {{"ALLOW_READ_EXPIRED_KEYS", CHSetting(trueOrFalse, {}, false)},
       {"MAX_THREADS_FOR_UPDATES", threadSetting},
       {"MAX_UPDATE_QUEUE_SIZE",
        CHSetting(
            [](RandomGenerator & rg)
            {
                return std::to_string(
                    rg.thresholdGenerator<uint32_t>(0.25, 0.25, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
            },
            {},
            false)},
       {"SIZE_IN_CELLS",
        CHSetting(
            [](RandomGenerator & rg)
            {
                return std::to_string(
                    rg.thresholdGenerator<uint32_t>(0.25, 0.25, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
            },
            {},
            false)}};

std::unordered_map<String, CHSetting> ssdCachedLayoutSettings
    = {{"BLOCK_SIZE", CHSetting(bytesRange, {}, false)},
       {"FILE_SIZE", CHSetting(bytesRange, {}, false)},
       {"READ_BUFFER_SIZE", CHSetting(bytesRange, {}, false)},
       {"WRITE_BUFFER_SIZE", CHSetting(bytesRange, {}, false)}};

std::unordered_map<String, CHSetting> ipTreeLayoutSettings = {{"ACCESS_TO_KEY_FROM_ATTRIBUTES", CHSetting(trueOrFalse, {}, false)}};

void loadFuzzerTableSettings(const FuzzConfig & fc)
{
    if (!fc.storage_policies.empty())
    {
        mergeStoragePolicies.insert(mergeStoragePolicies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        const auto & storage_policy
            = CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(mergeStoragePolicies) + "'"; }, {}, false);
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
         {Distributed, distributedTableSettings},
         {Dictionary, {}},
         {GenerateRandom, {}}});

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
}

}
