#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/BaseSettingsProgramOptions.h>
#include <Core/MergeSelectorAlgorithm.h>
#include <Core/SettingsChangesHistory.h>
#include <Disks/DiskFomAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>
#include <Common/NamePrompter.h>
#include <Common/logger_useful.h>


#include <boost/program_options.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
}

// clang-format off

/** These settings represent fine tunes for internal details of MergeTree storages
  * and should not be changed by the user without a reason.
  */
#define MERGE_TREE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, min_compress_block_size, 0, "When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the specified threshold. If this setting is not set, the corresponding global setting is used.", 0) \
    DECLARE(UInt64, max_compress_block_size, 0, "Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.", 0) \
    DECLARE(UInt64, index_granularity, 8192, "How many rows correspond to one primary key value.", 0) \
    DECLARE(UInt64, max_digestion_size_per_segment, 256_MiB, "Max number of bytes to digest per segment to build GIN index.", 0) \
    \
    /** Data storing format settings. */ \
    DECLARE(UInt64, min_bytes_for_wide_part, 10485760, "Minimal uncompressed size in bytes to create part in wide format instead of compact", 0) \
    DECLARE(UInt64, min_rows_for_wide_part, 0, "Minimal number of rows to create part in wide format instead of compact", 0) \
    DECLARE(Float, ratio_of_defaults_for_sparse_serialization, 0.9375f, "Minimal ratio of number of default values to number of all values in column to store it in sparse serializations. If >= 1, columns will be always written in full serialization.", 0) \
    DECLARE(Bool, replace_long_file_name_to_hash, true, "If the file name for column is too long (more than 'max_file_name_length' bytes) replace it to SipHash128", 0) \
    DECLARE(UInt64, max_file_name_length, 127, "The maximal length of the file name to keep it as is without hashing", 0) \
    DECLARE(UInt64, min_bytes_for_full_part_storage, 0, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, min_rows_for_full_part_storage, 0, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, compact_parts_max_bytes_to_buffer, 128 * 1024 * 1024, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, compact_parts_max_granules_to_buffer, 128, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, compact_parts_merge_max_bytes_to_prefetch_part, 16 * 1024 * 1024, "Only available in ClickHouse Cloud", 0) \
    DECLARE(Bool, load_existing_rows_count_for_old_parts, false, "Whether to load existing_rows_count for existing parts. If false, existing_rows_count will be equal to rows_count for existing parts.", 0) \
    DECLARE(Bool, use_compact_variant_discriminators_serialization, true, "Use compact version of Variant discriminators serialization.", 0) \
    \
    /** Merge selector settings. */ \
    DECLARE(UInt64, merge_selector_blurry_base_scale_factor, 0, "Controls when the logic kicks in relatively to the number of parts in partition. The bigger the factor the more belated reaction will be.", 0) \
    DECLARE(UInt64, merge_selector_window_size, 1000, "How many parts to look at once.", 0) \
    \
    /** Merge settings. */ \
    DECLARE(UInt64, merge_max_block_size, 8192, "How many rows in blocks should be formed for merge operations. By default has the same value as `index_granularity`.", 0) \
    DECLARE(UInt64, merge_max_block_size_bytes, 10 * 1024 * 1024, "How many bytes in blocks should be formed for merge operations. By default has the same value as `index_granularity_bytes`.", 0) \
    DECLARE(UInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, "Maximum in total size of parts to merge, when there are maximum free threads in background pool (or entries in replication queue).", 0) \
    DECLARE(UInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, "Maximum in total size of parts to merge, when there are minimum free threads in background pool (or entries in replication queue).", 0) \
    DECLARE(UInt64, max_replicated_merges_in_queue, 1000, "How many tasks of merging and mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    DECLARE(UInt64, max_replicated_mutations_in_queue, 8, "How many tasks of mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    DECLARE(UInt64, max_replicated_merges_with_ttl_in_queue, 1, "How many tasks of merging parts with TTL are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue). This is to allow small merges to process - not filling the pool with long running merges.", 0) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_execute_mutation, 20, "When there is less than specified number of free entries in pool, do not execute part mutations. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    DECLARE(UInt64, max_number_of_mutations_for_replica, 0, "Limit the number of part mutations per replica to the specified amount. Zero means no limit on the number of mutations per replica (the execution can still be constrained by other settings).", 0) \
    DECLARE(UInt64, max_number_of_merges_with_ttl_in_pool, 2, "When there is more than specified number of merges with TTL entries in pool, do not assign new merge with TTL. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    DECLARE(Seconds, old_parts_lifetime, 8 * 60, "How many seconds to keep obsolete parts.", 0) \
    DECLARE(Seconds, temporary_directories_lifetime, 86400, "How many seconds to keep tmp_-directories. You should not lower this value because merges and mutations may not be able to work with low value of this setting.", 0) \
    DECLARE(Seconds, lock_acquire_timeout_for_background_operations, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "For background operations like merges, mutations etc. How many seconds before failing to acquire table locks.", 0) \
    DECLARE(UInt64, min_rows_to_fsync_after_merge, 0, "Minimal number of rows to do fsync for part after merge (0 - disabled)", 0) \
    DECLARE(UInt64, min_compressed_bytes_to_fsync_after_merge, 0, "Minimal number of compressed bytes to do fsync for part after merge (0 - disabled)", 0) \
    DECLARE(UInt64, min_compressed_bytes_to_fsync_after_fetch, 0, "Minimal number of compressed bytes to do fsync for part after fetch (0 - disabled)", 0) \
    DECLARE(Bool, fsync_after_insert, false, "Do fsync for every inserted part. Significantly decreases performance of inserts, not recommended to use with wide parts.", 0) \
    DECLARE(Bool, fsync_part_directory, false, "Do fsync for part directory after all part operations (writes, renames, etc.).", 0) \
    DECLARE(UInt64, non_replicated_deduplication_window, 0, "How many last blocks of hashes should be kept on disk (0 - disabled).", 0) \
    DECLARE(UInt64, max_parts_to_merge_at_once, 100, "Max amount of parts which can be merged at once (0 - disabled). Doesn't affect OPTIMIZE FINAL query.", 0) \
    DECLARE(UInt64, merge_selecting_sleep_ms, 5000, "Minimum time to wait before trying to select parts to merge again after no parts were selected. A lower setting will trigger selecting tasks in background_schedule_pool frequently which result in large amount of requests to zookeeper in large-scale clusters", 0) \
    DECLARE(UInt64, max_merge_selecting_sleep_ms, 60000, "Maximum time to wait before trying to select parts to merge again after no parts were selected. A lower setting will trigger selecting tasks in background_schedule_pool frequently which result in large amount of requests to zookeeper in large-scale clusters", 0) \
    DECLARE(Float, merge_selecting_sleep_slowdown_factor, 1.2f, "The sleep time for merge selecting task is multiplied by this factor when there's nothing to merge and divided when a merge was assigned", 0) \
    DECLARE(UInt64, merge_tree_clear_old_temporary_directories_interval_seconds, 60, "The period of executing the clear old temporary directories operation in background.", 0) \
    DECLARE(UInt64, merge_tree_clear_old_parts_interval_seconds, 1, "The period of executing the clear old parts operation in background.", 0) \
    DECLARE(UInt64, min_age_to_force_merge_seconds, 0, "If all parts in a certain range are older than this value, range will be always eligible for merging. Set to 0 to disable.", 0) \
    DECLARE(Bool, min_age_to_force_merge_on_partition_only, false, "Whether min_age_to_force_merge_seconds should be applied only on the entire partition and not on subset.", false) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_execute_optimize_entire_partition, 25, "When there is less than specified number of free entries in pool, do not try to execute optimize entire partition with a merge (this merge is created when set min_age_to_force_merge_seconds > 0 and min_age_to_force_merge_on_partition_only = true). This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    DECLARE(Bool, remove_rolled_back_parts_immediately, 1, "Setting for an incomplete experimental feature.", EXPERIMENTAL) \
    DECLARE(UInt64, replicated_max_mutations_in_one_entry, 10000, "Max number of mutation commands that can be merged together and executed in one MUTATE_PART entry (0 means unlimited)", 0) \
    DECLARE(UInt64, number_of_mutations_to_delay, 500, "If table has at least that many unfinished mutations, artificially slow down mutations of table. Disabled if set to 0", 0) \
    DECLARE(UInt64, number_of_mutations_to_throw, 1000, "If table has at least that many unfinished mutations, throw 'Too many mutations' exception. Disabled if set to 0", 0) \
    DECLARE(UInt64, min_delay_to_mutate_ms, 10, "Min delay of mutating MergeTree table in milliseconds, if there are a lot of unfinished mutations", 0) \
    DECLARE(UInt64, max_delay_to_mutate_ms, 1000, "Max delay of mutating MergeTree table in milliseconds, if there are a lot of unfinished mutations", 0) \
    DECLARE(Bool, exclude_deleted_rows_for_part_size_in_merge, false, "Use an estimated source part size (excluding lightweight deleted rows) when selecting parts to merge", 0) \
    DECLARE(String, merge_workload, "", "Name of workload to be used to access resources for merges", 0) \
    DECLARE(String, mutation_workload, "", "Name of workload to be used to access resources for mutations", 0) \
    DECLARE(Milliseconds, background_task_preferred_step_execution_time_ms, 50, "Target time to execution of one step of merge or mutation. Can be exceeded if one step takes longer time", 0) \
    DECLARE(MergeSelectorAlgorithm, merge_selector_algorithm, MergeSelectorAlgorithm::SIMPLE, "The algorithm to select parts for merges assignment", EXPERIMENTAL) \
    DECLARE(Bool, merge_selector_enable_heuristic_to_remove_small_parts_at_right, true, "Enable heuristic for selecting parts for merge which removes parts from right side of range, if their size is less than specified ratio (0.01) of sum_size. Works for Simple and StochasticSimple merge selectors", 0) \
    \
    /** Inserts settings. */ \
    DECLARE(UInt64, parts_to_delay_insert, 1000, "If table contains at least that many active parts in single partition, artificially slow down insert into table. Disabled if set to 0", 0) \
    DECLARE(UInt64, inactive_parts_to_delay_insert, 0, "If table contains at least that many inactive parts in single partition, artificially slow down insert into table.", 0) \
    DECLARE(UInt64, parts_to_throw_insert, 3000, "If more than this number active parts in single partition, throw 'Too many parts ...' exception.", 0) \
    DECLARE(UInt64, inactive_parts_to_throw_insert, 0, "If more than this number inactive parts in single partition, throw 'Too many inactive parts ...' exception.", 0) \
    DECLARE(UInt64, max_avg_part_size_for_too_many_parts, 1ULL * 1024 * 1024 * 1024, "The 'too many parts' check according to 'parts_to_delay_insert' and 'parts_to_throw_insert' will be active only if the average part size (in the relevant partition) is not larger than the specified threshold. If it is larger than the specified threshold, the INSERTs will be neither delayed or rejected. This allows to have hundreds of terabytes in a single table on a single server if the parts are successfully merged to larger parts. This does not affect the thresholds on inactive parts or total parts.", 0) \
    DECLARE(UInt64, max_delay_to_insert, 1, "Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts in single partition.", 0) \
    DECLARE(UInt64, min_delay_to_insert_ms, 10, "Min delay of inserting data into MergeTree table in milliseconds, if there are a lot of unmerged parts in single partition.", 0) \
    DECLARE(UInt64, max_parts_in_total, 100000, "If more than this number active parts in all partitions in total, throw 'Too many parts ...' exception.", 0) \
    DECLARE(Bool, async_insert, false, "If true, data from INSERT query is stored in queue and later flushed to table in background.", 0) \
    DECLARE(Bool, add_implicit_sign_column_constraint_for_collapsing_engine, false, "If true, add implicit constraint for sign column for CollapsingMergeTree engine.", 0) \
    DECLARE(Milliseconds, sleep_before_commit_local_part_in_replicated_table_ms, 0, "For testing. Do not change it.", 0) \
    DECLARE(Bool, optimize_row_order, false, "Allow reshuffling of rows during part inserts and merges to improve the compressibility of the new part", 0) \
    DECLARE(Bool, use_adaptive_write_buffer_for_dynamic_subcolumns, true, "Allow to use adaptive writer buffers during writing dynamic subcolumns to reduce memory usage", 0) \
    DECLARE(UInt64, adaptive_write_buffer_initial_size, 16 * 1024, "Initial size of an adaptive write buffer", 0) \
    DECLARE(UInt64, min_free_disk_bytes_to_perform_insert, 0, "Minimum free disk space bytes to perform an insert.", 0) \
    DECLARE(Float, min_free_disk_ratio_to_perform_insert, 0.0, "Minimum free disk space ratio to perform an insert.", 0) \
    \
    /* Part removal settings. */ \
    DECLARE(UInt64, simultaneous_parts_removal_limit, 0, "Maximum number of parts to remove during one CleanupThread iteration (0 means unlimited).", 0) \
    \
    /** Replication settings. */ \
    DECLARE(UInt64, replicated_deduplication_window, 1000, "How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).", 0) \
    DECLARE(UInt64, replicated_deduplication_window_seconds, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", 0) \
    DECLARE(UInt64, replicated_deduplication_window_for_async_inserts, 10000, "How many last hash values of async_insert blocks should be kept in ZooKeeper (old blocks will be deleted).", 0) \
    DECLARE(UInt64, replicated_deduplication_window_seconds_for_async_inserts, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window_for_async_inserts\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", 0) \
    DECLARE(Milliseconds, async_block_ids_cache_update_wait_ms, 100, "How long each insert iteration will wait for async_block_ids_cache update", 0) \
    DECLARE(Bool, use_async_block_ids_cache, true, "Use in-memory cache to filter duplicated async inserts based on block ids", 0) \
    DECLARE(UInt64, max_replicated_logs_to_keep, 1000, "How many records may be in log, if there is inactive replica. Inactive replica becomes lost when when this number exceed.", 0) \
    DECLARE(UInt64, min_replicated_logs_to_keep, 10, "Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.", 0) \
    DECLARE(Seconds, prefer_fetch_merged_part_time_threshold, 3600, "If time passed after replication log entry creation exceeds this threshold and sum size of parts is greater than \"prefer_fetch_merged_part_size_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    DECLARE(UInt64, prefer_fetch_merged_part_size_threshold, 10ULL * 1024 * 1024 * 1024, "If sum size of parts exceeds this threshold and time passed after replication log entry creation is greater than \"prefer_fetch_merged_part_time_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    DECLARE(Seconds, execute_merges_on_single_replica_time_threshold, 0, "When greater than zero only a single replica starts the merge immediately, others wait up to that amount of time to download the result instead of doing merges locally. If the chosen replica doesn't finish the merge during that amount of time, fallback to standard behavior happens.", 0) \
    DECLARE(Seconds, remote_fs_execute_merges_on_single_replica_time_threshold, 3 * 60 * 60, "When greater than zero only a single replica starts the merge immediately if merged part on shared storage and 'allow_remote_fs_zero_copy_replication' is enabled.", 0) \
    DECLARE(Seconds, try_fetch_recompressed_part_timeout, 7200, "Recompression works slow in most cases, so we don't start merge with recompression until this timeout and trying to fetch recompressed part from replica which assigned this merge with recompression.", 0) \
    DECLARE(Bool, always_fetch_merged_part, false, "If true, replica never merge parts and always download merged parts from other replicas.", 0) \
    DECLARE(UInt64, max_suspicious_broken_parts, 100, "Max broken parts, if more - deny automatic deletion.", 0) \
    DECLARE(UInt64, max_suspicious_broken_parts_bytes, 1ULL * 1024 * 1024 * 1024, "Max size of all broken parts, if more - deny automatic deletion.", 0) \
    DECLARE(UInt64, max_files_to_modify_in_alter_columns, 75, "Not apply ALTER if number of files for modification(deletion, addition) more than this.", 0) \
    DECLARE(UInt64, max_files_to_remove_in_alter_columns, 50, "Not apply ALTER, if number of files for deletion more than this.", 0) \
    DECLARE(Float, replicated_max_ratio_of_wrong_parts, 0.5, "If ratio of wrong parts to total number of parts is less than this - allow to start.", 0) \
    DECLARE(Bool, replicated_can_become_leader, true, "If true, Replicated tables replicas on this node will try to acquire leadership.", 0) \
    DECLARE(Seconds, zookeeper_session_expiration_check_period, 60, "ZooKeeper session expiration check period, in seconds.", 0) \
    DECLARE(Seconds, initialization_retry_period, 60, "Retry period for table initialization, in seconds.", 0) \
    DECLARE(Bool, detach_old_local_parts_when_cloning_replica, true, "Do not remove old local parts when repairing lost replica.", 0) \
    DECLARE(Bool, detach_not_byte_identical_parts, false, "Do not remove non byte-idential parts for ReplicatedMergeTree, instead detach them (maybe useful for further analysis).", 0) \
    DECLARE(UInt64, max_replicated_fetches_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.", 0) \
    DECLARE(UInt64, max_replicated_sends_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.", 0) \
    DECLARE(Milliseconds, wait_for_unique_parts_send_before_shutdown_ms, 0, "Before shutdown table will wait for required amount time for unique parts (exist only on current replica) to be fetched by other replicas (0 means disabled).", 0) \
    DECLARE(Float, fault_probability_before_part_commit, 0, "For testing. Do not change it.", 0) \
    DECLARE(Float, fault_probability_after_part_commit, 0, "For testing. Do not change it.", 0) \
    DECLARE(Bool, shared_merge_tree_disable_merges_and_mutations_assignment, false, "Only available in ClickHouse Cloud", 0) \
    DECLARE(Float, shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations, 0.5, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, shared_merge_tree_parts_load_batch_size, 32, "Only available in ClickHouse Cloud", 0) \
    \
    /** Check delay of replicas settings. */ \
    DECLARE(UInt64, min_relative_delay_to_measure, 120, "Calculate relative replica delay only if absolute delay is not less that this value.", 0) \
    DECLARE(UInt64, cleanup_delay_period, 30, "Minimum period to clean old queue logs, blocks hashes and parts.", 0) \
    DECLARE(UInt64, max_cleanup_delay_period, 300, "Maximum period to clean old queue logs, blocks hashes and parts.", 0) \
    DECLARE(UInt64, cleanup_delay_period_random_add, 10, "Add uniformly distributed value from 0 to x seconds to cleanup_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables.", 0) \
    DECLARE(UInt64, cleanup_thread_preferred_points_per_iteration, 150, "Preferred batch size for background cleanup (points are abstract but 1 point is approximately equivalent to 1 inserted block).", 0) \
    DECLARE(UInt64, cleanup_threads, 128, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, kill_delay_period, 30, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, kill_delay_period_random_add, 10, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, kill_threads, 128, "Only available in ClickHouse Cloud", 0) \
    DECLARE(UInt64, min_relative_delay_to_close, 300, "Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.", 0) \
    DECLARE(UInt64, min_absolute_delay_to_close, 0, "Minimal absolute delay to close, stop serving requests and not return Ok during status check.", 0) \
    DECLARE(UInt64, enable_vertical_merge_algorithm, 1, "Enable usage of Vertical merge algorithm.", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * 8192, "Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm.", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_bytes_to_activate, 0, "Minimal (approximate) uncompressed size in bytes in merging parts to activate Vertical merge algorithm.", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_columns_to_activate, 11, "Minimal amount of non-PK columns to activate Vertical merge algorithm.", 0) \
    DECLARE(Bool, vertical_merge_remote_filesystem_prefetch, true, "If true prefetching of data from remote filesystem is used for the next column during merge", 0) \
    DECLARE(UInt64, max_postpone_time_for_failed_mutations_ms, 5ULL * 60 * 1000, "The maximum postpone time for failed mutations.", 0) \
    \
    /** Compatibility settings */ \
    DECLARE(Bool, allow_suspicious_indices, false, "Reject primary/secondary indexes and sorting keys with identical expressions", 0) \
    DECLARE(Bool, compatibility_allow_sampling_expression_not_in_primary_key, false, "Allow to create a table with sampling expression not in primary key. This is needed only to temporarily allow to run the server with wrong tables for backward compatibility.", 0) \
    DECLARE(Bool, use_minimalistic_checksums_in_zookeeper, true, "Use small format (dozens bytes) for part checksums in ZooKeeper instead of ordinary ones (dozens KB). Before enabling check that all replicas support new format.", 0) \
    DECLARE(Bool, use_minimalistic_part_header_in_zookeeper, true, "Store part header (checksums and columns) in a compact format and a single part znode instead of separate znodes (<part>/columns and <part>/checksums). This can dramatically reduce snapshot size in ZooKeeper. Before enabling check that all replicas support new format.", 0) \
    DECLARE(UInt64, finished_mutations_to_keep, 100, "How many records about mutations that are done to keep. If zero, then keep all of them.", 0) \
    DECLARE(UInt64, min_merge_bytes_to_use_direct_io, 10ULL * 1024 * 1024 * 1024, "Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled).", 0) \
    DECLARE(UInt64, index_granularity_bytes, 10 * 1024 * 1024, "Approximate amount of bytes in single granule (0 - disabled).", 0) \
    DECLARE(UInt64, min_index_granularity_bytes, 1024, "Minimum amount of bytes in single granule.", 1024) \
    DECLARE(Int64, merge_with_ttl_timeout, 3600 * 4, "Minimal time in seconds, when merge with delete TTL can be repeated.", 0) \
    DECLARE(Int64, merge_with_recompression_ttl_timeout, 3600 * 4, "Minimal time in seconds, when merge with recompression TTL can be repeated.", 0) \
    DECLARE(Bool, ttl_only_drop_parts, false, "Only drop altogether the expired parts and not partially prune them.", 0) \
    DECLARE(Bool, materialize_ttl_recalculate_only, false, "Only recalculate ttl info when MATERIALIZE TTL", 0) \
    DECLARE(Bool, enable_mixed_granularity_parts, true, "Enable parts with adaptive and non adaptive granularity", 0) \
    DECLARE(UInt64, concurrent_part_removal_threshold, 100, "Activate concurrent part removal (see 'max_part_removal_threads') only if the number of inactive data parts is at least this.", 0) \
    DECLARE(UInt64, zero_copy_concurrent_part_removal_max_split_times, 5, "Max recursion depth for splitting independent Outdated parts ranges into smaller subranges (highly not recommended to change)", 0) \
    DECLARE(Float, zero_copy_concurrent_part_removal_max_postpone_ratio, static_cast<Float32>(0.05), "Max percentage of top level parts to postpone removal in order to get smaller independent ranges (highly not recommended to change)", 0) \
    DECLARE(String, storage_policy, "default", "Name of storage disk policy", 0) \
    DECLARE(String, disk, "", "Name of storage disk. Can be specified instead of storage policy.", 0) \
    DECLARE(Bool, allow_nullable_key, false, "Allow Nullable types as primary keys.", 0) \
    DECLARE(Bool, remove_empty_parts, true, "Remove empty parts after they were pruned by TTL, mutation, or collapsing merge algorithm.", 0) \
    DECLARE(Bool, assign_part_uuids, false, "Generate UUIDs for parts. Before enabling check that all replicas support new format.", 0) \
    DECLARE(Int64, max_partitions_to_read, -1, "Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited. This setting is the default that can be overridden by the query-level setting with the same name.", 0) \
    DECLARE(UInt64, max_concurrent_queries, 0, "Max number of concurrently executed queries related to the MergeTree table (0 - disabled). Queries will still be limited by other max_concurrent_queries settings.", 0) \
    DECLARE(UInt64, min_marks_to_honor_max_concurrent_queries, 0, "Minimal number of marks to honor the MergeTree-level's max_concurrent_queries (0 - disabled). Queries will still be limited by other max_concurrent_queries settings.", 0) \
    DECLARE(UInt64, min_bytes_to_rebalance_partition_over_jbod, 0, "Minimal amount of bytes to enable part rebalance over JBOD array (0 - disabled).", 0) \
    DECLARE(Bool, check_sample_column_is_correct, true, "Check columns or columns by hash for sampling are unsigned integer.", 0) \
    DECLARE(Bool, allow_vertical_merges_from_compact_to_wide_parts, true, "Allows vertical merges from compact to wide parts. This settings must have the same value on all replicas", 0) \
    DECLARE(Bool, enable_the_endpoint_id_with_zookeeper_name_prefix, false, "Enable the endpoint id with zookeeper name prefix for the replicated merge tree table", 0) \
    DECLARE(UInt64, zero_copy_merge_mutation_min_parts_size_sleep_before_lock, 1ULL * 1024 * 1024 * 1024, "If zero copy replication is enabled sleep random amount of time before trying to lock depending on parts size for merge or mutation", 0) \
    DECLARE(Bool, allow_floating_point_partition_key, false, "Allow floating point as partition key", 0) \
    DECLARE(UInt64, sleep_before_loading_outdated_parts_ms, 0, "For testing. Do not change it.", 0) \
    DECLARE(Bool, always_use_copy_instead_of_hardlinks, false, "Always copy data instead of hardlinking during mutations/replaces/detaches and so on.", 0) \
    DECLARE(Bool, disable_freeze_partition_for_zero_copy_replication, true, "Disable FREEZE PARTITION query for zero copy replication.", 0) \
    DECLARE(Bool, disable_detach_partition_for_zero_copy_replication, true, "Disable DETACH PARTITION query for zero copy replication.", 0) \
    DECLARE(Bool, disable_fetch_partition_for_zero_copy_replication, true, "Disable FETCH PARTITION query for zero copy replication.", 0) \
    DECLARE(Bool, enable_block_number_column, false, "Enable persisting column _block_number for each row.", 0) ALIAS(allow_experimental_block_number_column) \
    DECLARE(Bool, enable_block_offset_column, false, "Enable persisting column _block_offset for each row.", 0) \
    \
    /** Experimental/work in progress feature. Unsafe for production. */ \
    DECLARE(UInt64, part_moves_between_shards_enable, 0, "Experimental/Incomplete feature to move parts between shards. Does not take into account sharding expressions.", EXPERIMENTAL) \
    DECLARE(UInt64, part_moves_between_shards_delay_seconds, 30, "Time to wait before/after moving parts between shards.", EXPERIMENTAL) \
    DECLARE(Bool, allow_remote_fs_zero_copy_replication, false, "Don't use this setting in production, because it is not ready.", BETA) \
    DECLARE(String, remote_fs_zero_copy_zookeeper_path, "/clickhouse/zero_copy", "ZooKeeper path for zero-copy table-independent info.", EXPERIMENTAL) \
    DECLARE(Bool, remote_fs_zero_copy_path_compatible_mode, false, "Run zero-copy in compatible mode during conversion process.", EXPERIMENTAL) \
    DECLARE(Bool, cache_populated_by_fetch, false, "Only available in ClickHouse Cloud", EXPERIMENTAL) \
    DECLARE(Bool, force_read_through_cache_for_merges, false, "Force read-through filesystem cache for merges", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_replacing_merge_with_cleanup, false, "Allow experimental CLEANUP merges for ReplacingMergeTree with is_deleted column.", EXPERIMENTAL) \
    \
    /** Compress marks and primary key. */ \
    DECLARE(Bool, compress_marks, true, "Marks support compression, reduce mark file size and speed up network transmission.", 0) \
    DECLARE(Bool, compress_primary_key, true, "Primary key support compression, reduce primary key file size and speed up network transmission.", 0) \
    DECLARE(String, marks_compression_codec, "ZSTD(3)", "Compression encoding used by marks, marks are small enough and cached, so the default compression is ZSTD(3).", 0) \
    DECLARE(String, primary_key_compression_codec, "ZSTD(3)", "Compression encoding used by primary, primary key is small enough and cached, so the default compression is ZSTD(3).", 0) \
    DECLARE(UInt64, marks_compress_block_size, 65536, "Mark compress block size, the actual size of the block to compress.", 0) \
    DECLARE(UInt64, primary_key_compress_block_size, 65536, "Primary compress block size, the actual size of the block to compress.", 0) \
    DECLARE(Bool, primary_key_lazy_load, true, "Load primary key in memory on first use instead of on table initialization. This can save memory in the presence of a large number of tables.", 0) \
    DECLARE(Float, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns, 0.9f, "If the value of a column of the primary key in data part changes at least in this ratio of times, skip loading next columns in memory. This allows to save memory usage by not loading useless columns of the primary key.", 0) \
    DECLARE(Bool, prewarm_mark_cache, false, "If true mark cache will be prewarmed by saving marks to mark cache on inserts, merges, fetches and on startup of server", 0) \
    DECLARE(String, columns_to_prewarm_mark_cache, "", "List of columns to prewarm mark cache for (if enabled). Empty means all columns", 0) \
    /** Projection settings. */ \
    DECLARE(UInt64, max_projections, 25, "The maximum number of merge tree projections.", 0) \
    DECLARE(LightweightMutationProjectionMode, lightweight_mutation_projection_mode, LightweightMutationProjectionMode::THROW, "When lightweight delete happens on a table with projection(s), the possible operations include throw the exception as projection exists, or drop projections of this table's relevant parts, or rebuild the projections.", 0) \
    DECLARE(DeduplicateMergeProjectionMode, deduplicate_merge_projection_mode, DeduplicateMergeProjectionMode::THROW, "Whether to allow create projection for the table with non-classic MergeTree. Ignore option is purely for compatibility which might result in incorrect answer. Otherwise, if allowed, what is the action when merge, drop or rebuild.", 0) \

#define MAKE_OBSOLETE_MERGE_TREE_SETTING(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", SettingsTierType::OBSOLETE)

#define OBSOLETE_MERGE_TREE_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing but left for compatibility reasons. */ \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_relative_delay_to_yield_leadership, 120) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, check_delay_period, 60) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_sends, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_sends_for_table, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches_for_table, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, write_final_mark, true) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_bytes_for_compact_part, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_rows_for_compact_part, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, in_memory_parts_enable_wal, true) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_max_bytes, 1024 * 1024 * 1024) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_bytes_to_fsync, 100ULL * 1024 * 1024) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_interval_ms_to_fsync, 100) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, in_memory_parts_insert_sync, false) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, MaxThreads, max_part_loading_threads, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, MaxThreads, max_part_removal_threads, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, use_metadata_cache, false) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, merge_tree_enable_clear_old_broken_detached, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds, 1ULL * 3600 * 24 * 30) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_connection_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_send_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_receive_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches_for_host, DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, CleanDeletedRows, clean_deleted_rows, CleanDeletedRows::Never) \

    /// Settings that should not change after the creation of a table.
    /// NOLINTNEXTLINE
#define APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(MACRO) \
    MACRO(index_granularity)

#define LIST_OF_MERGE_TREE_SETTINGS(M, ALIAS) \
    MERGE_TREE_SETTINGS(M, ALIAS)             \
    OBSOLETE_MERGE_TREE_SETTINGS(M, ALIAS)

// clang-format on

DECLARE_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)

/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettingsImpl : public BaseSettings<MergeTreeSettingsTraits>
{
    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach);

    /// Check that the values are sane taking also query-level settings into account.
    void sanityCheck(size_t background_pool_tasks) const;
};

IMPLEMENT_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)

void MergeTreeSettingsImpl::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    if (storage_def.settings)
    {
        try
        {
            bool found_disk_setting = false;
            bool found_storage_policy_setting = false;

            auto changes = storage_def.settings->changes;
            for (auto & [name, value] : changes)
            {
                CustomType custom;
                if (name == "disk")
                {
                    ASTPtr value_as_custom_ast = nullptr;
                    if (value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
                        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

                    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
                    {
                        auto disk_name = DiskFomAST::createCustomDisk(value_as_custom_ast, context, is_attach);
                        LOG_DEBUG(getLogger("MergeTreeSettings"), "Created custom disk {}", disk_name);
                        value = disk_name;
                    }
                    else
                    {
                        DiskFomAST::ensureDiskIsNotCustom(value.safeGet<String>(), context);
                    }

                    if (has("storage_policy"))
                        resetToDefault("storage_policy");

                    found_disk_setting = true;
                }
                else if (name == "storage_policy")
                    found_storage_policy_setting = true;

                if (!is_attach && found_disk_setting && found_storage_policy_setting)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time");
                }

            }

            applyChanges(changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;

#define ADD_IF_ABSENT(NAME)                                                                                   \
    if (std::find_if(changes.begin(), changes.end(),                                                          \
                  [](const SettingChange & c) { return c.name == #NAME; })                                    \
            == changes.end())                                                                                 \
        changes.push_back(SettingChange{#NAME, (NAME).value});

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

void MergeTreeSettingsImpl::sanityCheck(size_t background_pool_tasks) const
{
    if (number_of_free_entries_in_pool_to_execute_mutation > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_mutation' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because mutations cannot work with these settings.",
            number_of_free_entries_in_pool_to_execute_mutation,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_lower_max_size_of_merge > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_lower_max_size_of_merge' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_lower_max_size_of_merge,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_execute_optimize_entire_partition > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_execute_optimize_entire_partition,
            background_pool_tasks);
    }

    // Zero index_granularity is nonsensical.
    if (index_granularity < 1)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity: value {} makes no sense",
            index_granularity);
    }

    // The min_index_granularity_bytes value is 1024 b and index_granularity_bytes is 10 mb by default.
    // If index_granularity_bytes is not disabled i.e > 0 b, then always ensure that it's greater than
    // min_index_granularity_bytes. This is mainly a safeguard against accidents whereby a really low
    // index_granularity_bytes SETTING of 1b can create really large parts with large marks.
    if (index_granularity_bytes > 0 && index_granularity_bytes < min_index_granularity_bytes)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity_bytes: {} is lower than specified min_index_granularity_bytes: {}",
            index_granularity_bytes,
            min_index_granularity_bytes);
    }

    // If min_bytes_to_rebalance_partition_over_jbod is not disabled i.e > 0 b, then always ensure that
    // it's not less than min_bytes_to_rebalance_partition_over_jbod. This is a safeguard to avoid tiny
    // parts to participate JBOD balancer which will slow down the merge process.
    if (min_bytes_to_rebalance_partition_over_jbod > 0
        && min_bytes_to_rebalance_partition_over_jbod < max_bytes_to_merge_at_max_space_in_pool / 1024)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "min_bytes_to_rebalance_partition_over_jbod: {} is lower than specified max_bytes_to_merge_at_max_space_in_pool / 1024: {}",
            min_bytes_to_rebalance_partition_over_jbod,
            max_bytes_to_merge_at_max_space_in_pool / 1024);
    }

    if (max_cleanup_delay_period < cleanup_delay_period)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_cleanup_delay_period setting ({}) must be greater than the value of cleanup_delay_period setting ({})",
            max_cleanup_delay_period, cleanup_delay_period);
    }

    if (max_merge_selecting_sleep_ms < merge_selecting_sleep_ms)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_merge_selecting_sleep_ms setting ({}) must be greater than the value of merge_selecting_sleep_ms setting ({})",
            max_merge_selecting_sleep_ms, merge_selecting_sleep_ms);
    }

    if (merge_selecting_sleep_slowdown_factor < 1.f)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of merge_selecting_sleep_slowdown_factor setting ({}) cannot be less than 1.0",
            merge_selecting_sleep_slowdown_factor);
    }
}

void MergeTreeColumnSettings::validate(const SettingsChanges & changes)
{
    static const MergeTreeSettings merge_tree_settings;
    static const std::set<String> allowed_column_level_settings =
    {
        "min_compress_block_size",
        "max_compress_block_size"
    };

    for (const auto & change : changes)
    {
        if (!allowed_column_level_settings.contains(change.name))
            throw Exception(
                ErrorCodes::UNKNOWN_SETTING,
                "Setting {} is unknown or not supported at column level, supported settings: {}",
                change.name,
                fmt::join(allowed_column_level_settings, ", "));
        MergeTreeSettingsImpl::checkCanSet(change.name, change.value);
    }
}

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) MergeTreeSettings##TYPE NAME = &MergeTreeSettingsImpl ::NAME;

namespace MergeTreeSetting
{
    LIST_OF_MERGE_TREE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

MergeTreeSettings::MergeTreeSettings() : impl(std::make_unique<MergeTreeSettingsImpl>())
{
}

MergeTreeSettings::MergeTreeSettings(const MergeTreeSettings & settings) : impl(std::make_unique<MergeTreeSettingsImpl>(*settings.impl))
{
}

MergeTreeSettings::MergeTreeSettings(MergeTreeSettings && settings) noexcept
    : impl(std::make_unique<MergeTreeSettingsImpl>(std::move(*settings.impl)))
{
}

MergeTreeSettings::~MergeTreeSettings() = default;

MERGETREE_SETTINGS_SUPPORTED_TYPES(MergeTreeSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

bool MergeTreeSettings::has(std::string_view name) const
{
    return impl->has(name);
}

bool MergeTreeSettings::tryGet(std::string_view name, Field & value) const
{
    return impl->tryGet(name, value);
}

Field MergeTreeSettings::get(std::string_view name) const
{
    return impl->get(name);
}

void MergeTreeSettings::set(std::string_view name, const Field & value)
{
    impl->set(name, value);
}

SettingsChanges MergeTreeSettings::changes() const
{
    return impl->changes();
}

void MergeTreeSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void MergeTreeSettings::applyChange(const SettingChange & change)
{
    impl->applyChange(change);
}

void MergeTreeSettings::applyCompatibilitySetting(const String & compatibility_value)
{
    /// If setting value is empty, we don't need to change settings
    if (compatibility_value.empty())
        return;

    ClickHouseVersion version(compatibility_value);
    const auto & settings_changes_history = getMergeTreeSettingsChangesHistory();
    /// Iterate through ClickHouse version in descending order and apply reversed
    /// changes for each version that is higher that version from compatibility setting
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        if (version >= it->first)
            break;

        /// Apply reversed changes from this version.
        for (const auto & change : it->second)
        {
            /// In case the alias is being used (e.g. use enable_analyzer) we must change the original setting
            auto final_name = MergeTreeSettingsTraits::resolveName(change.name);
            set(final_name, change.previous_value);
        }
    }
}

std::vector<std::string_view> MergeTreeSettings::getAllRegisteredNames() const
{
    std::vector<std::string_view> setting_names;
    for (const auto & setting : impl->all())
    {
        setting_names.emplace_back(setting.getName());
    }
    return setting_names;
}

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    impl->loadFromQuery(storage_def, context, is_attach);
}

void MergeTreeSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            impl->set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in MergeTree config");
        throw;
    }
}

bool MergeTreeSettings::needSyncPart(size_t input_rows, size_t input_bytes) const
{
    return (
        (impl->min_rows_to_fsync_after_merge && input_rows >= impl->min_rows_to_fsync_after_merge)
        || (impl->min_compressed_bytes_to_fsync_after_merge && input_bytes >= impl->min_compressed_bytes_to_fsync_after_merge));
}

void MergeTreeSettings::sanityCheck(size_t background_pool_tasks) const
{
    impl->sanityCheck(background_pool_tasks);
}

void MergeTreeSettings::dumpToSystemMergeTreeSettingsColumns(MutableColumnsAndConstraints & params) const
{
    const auto & constraints = params.constraints;
    MutableColumns & res_columns = params.res_columns;

    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());

        Field min, max;
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        constraints.get(*this, setting_name, min, max, writability);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = MergeTreeSettings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = MergeTreeSettings::valueToStringUtil(setting_name, max);

        res_columns[4]->insert(min);
        res_columns[5]->insert(max);
        res_columns[6]->insert(writability == SettingConstraintWritability::CONST);
        res_columns[7]->insert(setting.getTypeName());
        res_columns[8]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
        res_columns[9]->insert(setting.getTier());
    }
}


namespace
{
/// Define transparent hash to we can use
/// std::string_view with the containers
struct TransparentStringHash
{
    using is_transparent = void;
    size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }
};
}

void MergeTreeSettings::addToProgramOptionsIfNotPresent(
    boost::program_options::options_description & main_options, bool allow_repeated_settings)
{
    /// Add merge tree settings manually, because names of some settings
    /// may clash. Query settings have higher priority and we just
    /// skip ambiguous merge tree settings.

    std::unordered_set<std::string, TransparentStringHash, std::equal_to<>> main_option_names;
    for (const auto & option : main_options.options())
        main_option_names.insert(option->long_name());

    const auto & settings_to_aliases = MergeTreeSettingsImpl::Traits::settingsToAliases();
    for (const auto & setting : impl->all())
    {
        const auto add_setting = [&](const std::string_view name)
        {
            if (auto it = main_option_names.find(name); it != main_option_names.end())
                return;

            if (allow_repeated_settings)
                addProgramOptionAsMultitoken(*impl, main_options, name, setting);
            else
                addProgramOption(*impl, main_options, name, setting);
        };

        const auto & setting_name = setting.getName();
        add_setting(setting_name);

        if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
        {
            for (const auto alias : it->second)
            {
                add_setting(alias);
            }
        }
    }
}

Field MergeTreeSettings::castValueUtil(std::string_view name, const Field & value)
{
    return MergeTreeSettingsImpl::castValueUtil(name, value);
}

String MergeTreeSettings::valueToStringUtil(std::string_view name, const Field & value)
{
    return MergeTreeSettingsImpl::valueToStringUtil(name, value);
}

Field MergeTreeSettings::stringToValueUtil(std::string_view name, const String & str)
{
    return MergeTreeSettingsImpl::stringToValueUtil(name, str);
}

bool MergeTreeSettings::hasBuiltin(std::string_view name)
{
    return MergeTreeSettingsImpl::hasBuiltin(name);
}

std::string_view MergeTreeSettings::resolveName(std::string_view name)
{
    return MergeTreeSettingsImpl::Traits::resolveName(name);
}

bool MergeTreeSettings::isReadonlySetting(const String & name)
{
    return name == "index_granularity" || name == "index_granularity_bytes" || name == "enable_mixed_granularity_parts";
}

void MergeTreeSettings::checkCanSet(std::string_view name, const Field & value)
{
    MergeTreeSettingsImpl::checkCanSet(name, value);
}

bool MergeTreeSettings::isPartFormatSetting(const String & name)
{
    return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part";
}
}
