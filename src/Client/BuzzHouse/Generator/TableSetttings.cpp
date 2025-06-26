#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static DB::Strings storagePolicies, disks, caches;

static const auto compressSetting = CHSetting(
    [](RandomGenerator & rg)
    {
        const DB::Strings & choices
            = {"'ZSTD'", "'LZ4'", "'LZ4HC'", "'ZSTD_QAT'", "'DEFLATE_QPL'", "'GCD'", "'FPC'", "'AES_128_GCM_SIV'", "'AES_256_GCM_SIV'"};
        return rg.pickRandomly(choices);
    },
    {"'ZSTD'", "'LZ4'", "'LZ4HC'", "'ZSTD_QAT'", "'DEFLATE_QPL'", "'GCD'", "'FPC'", "'AES_128_GCM_SIV'", "'AES_256_GCM_SIV'"},
    false);

static const auto bytesRangeSetting = CHSetting(bytesRange, {"0", "4", "8", "32", "1024", "4096", "16384", "'10M'"}, false);

static const auto highRangeSetting = CHSetting(highRange, {"0", "4", "8", "32", "64", "1024", "4096", "16384", "'10M'"}, false);

static const auto rowsRangeSetting = CHSetting(rowsRange, {"0", "4", "8", "32", "64", "4096", "16384", "'10M'"}, false);

static std::unordered_map<String, CHSetting> mergeTreeTableSettings
    = {{"adaptive_write_buffer_initial_size", bytesRangeSetting},
       {"add_implicit_sign_column_constraint_for_collapsing_engine", trueOrFalseSetting},
       {"add_minmax_index_for_numeric_columns", trueOrFalseSetting},
       {"add_minmax_index_for_string_columns", trueOrFalseSetting},
       {"allow_experimental_replacing_merge_with_cleanup", trueOrFalseSetting},
       {"allow_floating_point_partition_key", trueOrFalseSettingNoOracle},
       {"allow_reduce_blocking_parts_task", trueOrFalseSetting},
       {"allow_remote_fs_zero_copy_replication", trueOrFalseSetting},
       {"allow_suspicious_indices", trueOrFalseSettingNoOracle},
       {"allow_vertical_merges_from_compact_to_wide_parts", trueOrFalseSetting},
       {"always_fetch_merged_part", trueOrFalseSetting},
       {"always_use_copy_instead_of_hardlinks", trueOrFalseSetting},
       {"apply_patches_on_merge", trueOrFalseSetting},
       {"assign_part_uuids", trueOrFalseSetting},
       {"async_insert", trueOrFalseSetting},
       {"cache_populated_by_fetch", trueOrFalseSetting},
       {"check_sample_column_is_correct", trueOrFalseSetting},
       {"columns_and_secondary_indices_sizes_lazy_calculation", trueOrFalseSetting},
       {"compact_parts_max_bytes_to_buffer", bytesRangeSetting},
       {"compact_parts_max_granules_to_buffer", highRangeSetting},
       {"compact_parts_merge_max_bytes_to_prefetch_part", bytesRangeSetting},
       {"compatibility_allow_sampling_expression_not_in_primary_key", trueOrFalseSetting},
       {"compress_marks", trueOrFalseSetting},
       {"compress_primary_key", trueOrFalseSetting},
       {"concurrent_part_removal_threshold",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 100)); }, {}, false)},
       {"deduplicate_merge_projection_mode",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'ignore'", "'throw'", "'drop'", "'rebuild'"};
                return rg.pickRandomly(choices);
            },
            {"'ignore'", "'throw'", "'drop'", "'rebuild'"},
            false)},
       {"default_compression_codec",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'NONE'", "'LZ4'", "'LZ4HC'", "'ZSTD'", "'T64'", "'AES_128_GCM_SIV'"};
                return rg.pickRandomly(choices);
            },
            {"'NONE'", "'LZ4'", "'LZ4HC'", "'ZSTD'", "'T64'", "'AES_128_GCM_SIV'"},
            false)},
       {"detach_not_byte_identical_parts", trueOrFalseSetting},
       {"detach_old_local_parts_when_cloning_replica", trueOrFalseSetting},
       {"disable_detach_partition_for_zero_copy_replication", trueOrFalseSetting},
       {"disable_fetch_partition_for_zero_copy_replication", trueOrFalseSetting},
       {"disable_freeze_partition_for_zero_copy_replication", trueOrFalseSetting},
       {"enable_index_granularity_compression", trueOrFalseSetting},
       {"enable_max_bytes_limit_for_min_age_to_force_merge", trueOrFalseSetting},
       {"enable_mixed_granularity_parts", trueOrFalseSetting},
       {"enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", trueOrFalseSetting},
       {"enable_the_endpoint_id_with_zookeeper_name_prefix", trueOrFalseSetting},
       {"enable_vertical_merge_algorithm", trueOrFalseSetting},
       {"enforce_index_structure_match_on_partition_manipulation", trueOrFalseSetting},
       {"exclude_deleted_rows_for_part_size_in_merge", trueOrFalseSetting},
       {"force_read_through_cache_for_merges", trueOrFalseSetting},
       {"fsync_after_insert", trueOrFalseSetting},
       {"fsync_part_directory", trueOrFalseSetting},
       {"index_granularity", highRangeSetting},
       {"index_granularity_bytes", bytesRangeSetting},
       {"lightweight_mutation_projection_mode",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'throw'", "'drop'", "'rebuild'"};
                return rg.pickRandomly(choices);
            },
            {"'throw'", "'drop'", "'rebuild'"},
            false)},
       {"load_existing_rows_count_for_old_parts", trueOrFalseSetting},
       {"marks_compress_block_size", highRangeSetting},
       {"marks_compression_codec", compressSetting},
       {"materialize_skip_indexes_on_merge", trueOrFalseSetting},
       {"materialize_ttl_recalculate_only", trueOrFalseSetting},
       {"max_bytes_to_merge_at_max_space_in_pool", bytesRangeSetting},
       {"max_bytes_to_merge_at_min_space_in_pool", bytesRangeSetting},
       {"max_file_name_length",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
       {"max_number_of_mutations_for_replica",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 100)); }, {}, false)},
       {"max_parts_to_merge_at_once",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1000)); }, {}, false)},
       {"max_replicated_merges_in_queue",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 100)); }, {}, false)},
       {"max_replicated_mutations_in_queue",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 100)); }, {}, false)},
       {"merge_max_block_size", highRangeSetting},
       {"merge_max_block_size_bytes", bytesRangeSetting},
       {"merge_selector_algorithm",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'Simple'", "'Trivial'", "'StochasticSimple'"};
                return rg.pickRandomly(choices);
            },
            {"'Simple'", "'Trivial'", "'StochasticSimple'"},
            false)},
       {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", trueOrFalseSetting},
       {"merge_selector_window_size", rowsRangeSetting},
       {"min_age_to_force_merge_on_partition_only", trueOrFalseSetting},
       {"min_bytes_for_full_part_storage", bytesRangeSetting},
       {"min_bytes_for_wide_part", bytesRangeSetting},
       {"min_compressed_bytes_to_fsync_after_fetch", bytesRangeSetting},
       {"min_compressed_bytes_to_fsync_after_merge", bytesRangeSetting},
       {"min_index_granularity_bytes", bytesRangeSetting},
       {"min_merge_bytes_to_use_direct_io", bytesRangeSetting},
       {"min_parts_to_merge_at_once",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
       {"min_rows_for_full_part_storage",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1000)); }, {}, false)},
       {"min_rows_to_fsync_after_merge",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1000)); }, {}, false)},
       {"min_rows_for_wide_part", rowsRangeSetting},
       {"non_replicated_deduplication_window", rowsRangeSetting},
       /// ClickHouse cloud setting
       {"notify_newest_block_number", trueOrFalseSetting},
       {"old_parts_lifetime",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 10, 8 * 60)); }, {}, false)},
       {"optimize_row_order", trueOrFalseSetting},
       {"prefer_fetch_merged_part_size_threshold", bytesRangeSetting},
       {"prewarm_mark_cache", trueOrFalseSetting},
       {"prewarm_primary_key_cache", trueOrFalseSetting},
       {"primary_key_compress_block_size", highRangeSetting},
       {"primary_key_compression_codec", compressSetting},
       {"primary_key_lazy_load", trueOrFalseSetting},
       {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", probRangeSetting},
       {"ratio_of_defaults_for_sparse_serialization", probRangeSetting},
       {"remote_fs_zero_copy_path_compatible_mode", trueOrFalseSetting},
       {"remove_empty_parts", trueOrFalseSetting},
       {"remove_rolled_back_parts_immediately", trueOrFalseSetting},
       {"remove_unused_patch_parts", trueOrFalseSetting},
       {"replace_long_file_name_to_hash", trueOrFalseSetting},
       {"replicated_can_become_leader", trueOrFalseSetting},
       {"replicated_max_mutations_in_one_entry",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 10000)); }, {}, false)},
       {"replicated_max_ratio_of_wrong_parts", probRangeSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_create_per_replica_metadata_nodes", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_disable_merges_and_mutations_assignment", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_enable_coordinated_merges", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_enable_keeper_parts_extra_data", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_enable_outdated_parts_check", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations", probRangeSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_parts_load_batch_size",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
       /// ClickHouse cloud setting
       {"shared_merge_tree_read_virtual_parts_from_leader", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_metadata_hints_cache", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_outdated_parts_compact_format", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"shared_merge_tree_use_too_many_parts_count_from_virtual_parts", trueOrFalseSetting},
       {"simultaneous_parts_removal_limit",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
       {"table_disk", trueOrFalseSetting},
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
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 16)); }, {}, false)},
       {"vertical_merge_algorithm_min_rows_to_activate", rowsRangeSetting},
       {"vertical_merge_remote_filesystem_prefetch", trueOrFalseSetting},
       {"write_marks_for_substreams_in_compact_parts", trueOrFalseSetting},
       {"zero_copy_concurrent_part_removal_max_postpone_ratio", probRangeSetting}};

static std::unordered_map<String, CHSetting> logTableSettings = {};

std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

std::unordered_map<String, CHSetting> restoreSettings
    = {{"allow_azure_native_copy", CHSetting(trueOrFalse, {}, false)},
       {"allow_different_database_def", CHSetting(trueOrFalse, {}, false)},
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
                    rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
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
                    rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
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
    std::unordered_map<String, CHSetting> s3Settings;
    std::unordered_map<String, CHSetting> s3QueueTableSettings;
    std::unordered_map<String, CHSetting> azureBlobStorageSettings;
    std::unordered_map<String, CHSetting> azureQueueSettings;

    if (!fc.storage_policies.empty())
    {
        storagePolicies.insert(storagePolicies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        const auto & storage_policy_setting
            = CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(storagePolicies) + "'"; }, {}, false);
        mergeTreeTableSettings.insert({{"storage_policy", storage_policy_setting}});
        logTableSettings.insert({{"storage_policy", storage_policy_setting}});
        restoreSettings.insert({{"storage_policy", storage_policy_setting}});
    }
    if (!fc.disks.empty())
    {
        disks.insert(disks.end(), fc.disks.begin(), fc.disks.end());
        const auto & disk_setting = CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(disks) + "'"; }, {}, false);
        mergeTreeTableSettings.insert({{"disk", disk_setting}});
        logTableSettings.insert({{"disk", disk_setting}});
    }
    if (!fc.caches.empty())
    {
        caches.insert(caches.end(), fc.caches.begin(), fc.caches.end());
        const auto & cache_setting
            = CHSetting([&](RandomGenerator & rg) { return "1, filesystem_cache_name = '" + rg.pickRandomly(caches) + "'"; }, {}, false);

        s3Settings.insert({{"enable_filesystem_cache", cache_setting}});
        azureBlobStorageSettings.insert({{"enable_filesystem_cache", cache_setting}});
    }

    s3QueueTableSettings.insert(s3Settings.begin(), s3Settings.end());
    azureQueueSettings.insert(azureBlobStorageSettings.begin(), azureBlobStorageSettings.end());
    s3QueueTableSettings.insert(
        {{"after_processing",
          CHSetting(
              [](RandomGenerator & rg)
              {
                  const DB::Strings & choices = {"'keep'", "'delete'"};
                  return rg.pickRandomly(choices);
              },
              {},
              false)},
         {"enable_hash_ring_filtering", CHSetting(trueOrFalse, {}, false)},
         {"list_objects_batch_size",
          CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 3000)); }, {}, false)},
         {"max_processed_bytes_before_commit", CHSetting(bytesRange, {}, false)},
         {"max_processed_files_before_commit", CHSetting(rowsRange, {}, false)},
         {"max_processed_rows_before_commit", CHSetting(rowsRange, {}, false)},
         {"mode", CHSetting([](RandomGenerator & rg) { return fmt::format("'{}orderded'", rg.nextBool() ? "un" : ""); }, {}, false)},
         {"parallel_inserts", CHSetting(trueOrFalse, {}, false)},
         {"s3queue_buckets",
          CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 16)); }, {}, false)},
         {"s3queue_enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {}, false)},
         {"s3queue_processing_threads_num", threadSetting},
         {"s3queue_tracked_files_limit", CHSetting(rowsRange, {}, false)}});

    allTableSettings.insert(
        {{MergeTree, mergeTreeTableSettings},
         {ReplacingMergeTree, mergeTreeTableSettings},
         {CoalescingMergeTree, mergeTreeTableSettings},
         {SummingMergeTree, mergeTreeTableSettings},
         {AggregatingMergeTree, mergeTreeTableSettings},
         {CollapsingMergeTree, mergeTreeTableSettings},
         {VersionedCollapsingMergeTree, mergeTreeTableSettings},
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
         {DeltaLakeS3, {}},
         {DeltaLakeAzure, {}},
         {DeltaLakeLocal, {}},
         {IcebergS3, {}},
         {IcebergAzure, {}},
         {IcebergLocal, {}},
         {Merge, {}},
         {Distributed, distributedTableSettings},
         {Dictionary, {}},
         {GenerateRandom, {}},
         {AzureBlobStorage, azureBlobStorageSettings},
         {AzureQueue, azureQueueSettings},
         {URL, {}},
         {KeeperMap, {}},
         {ExternalDistributed, {}},
         {MaterializedPostgreSQL, {}}});

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
