#pragma once

#include <base/unit.h>
#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
struct Settings;


/** These settings represent fine tunes for internal details of MergeTree storages
  * and should not be changed by the user without a reason.
  */

#define LIST_OF_MERGE_TREE_SETTINGS(M, ALIAS) \
    M(UInt64, min_compress_block_size, 0, "When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the specified threshold. If this setting is not set, the corresponding global setting is used.", 0) \
    M(UInt64, max_compress_block_size, 0, "Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.", 0) \
    M(UInt64, index_granularity, 8192, "How many rows correspond to one primary key value.", 0) \
    M(UInt64, max_digestion_size_per_segment, 256_MiB, "Max number of bytes to digest per segment to build GIN index.", 0) \
    \
    /** Data storing format settings. */ \
    M(UInt64, min_bytes_for_wide_part, 10485760, "Minimal uncompressed size in bytes to create part in wide format instead of compact", 0) \
    M(UInt64, min_rows_for_wide_part, 0, "Minimal number of rows to create part in wide format instead of compact", 0) \
    M(Float, ratio_of_defaults_for_sparse_serialization, 0.9375f, "Minimal ratio of number of default values to number of all values in column to store it in sparse serializations. If >= 1, columns will be always written in full serialization.", 0) \
    \
    /** Merge settings. */ \
    M(UInt64, merge_max_block_size, 8192, "How many rows in blocks should be formed for merge operations. By default has the same value as `index_granularity`.", 0) \
    M(UInt64, merge_max_block_size_bytes, 10 * 1024 * 1024, "How many bytes in blocks should be formed for merge operations. By default has the same value as `index_granularity_bytes`.", 0) \
    M(UInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, "Maximum in total size of parts to merge, when there are maximum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, "Maximum in total size of parts to merge, when there are minimum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_replicated_merges_in_queue, 1000, "How many tasks of merging and mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, max_replicated_mutations_in_queue, 8, "How many tasks of mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, max_replicated_merges_with_ttl_in_queue, 1, "How many tasks of merging parts with TTL are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue). This is to allow small merges to process - not filling the pool with long running merges.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_execute_mutation, 20, "When there is less than specified number of free entries in pool, do not execute part mutations. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    M(UInt64, max_number_of_mutations_for_replica, 0, "Limit the number of part mutations per replica to the specified amount. Zero means no limit on the number of mutations per replica (the execution can still be constrained by other settings).", 0) \
    M(UInt64, max_number_of_merges_with_ttl_in_pool, 2, "When there is more than specified number of merges with TTL entries in pool, do not assign new merge with TTL. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    M(Seconds, old_parts_lifetime, 8 * 60, "How many seconds to keep obsolete parts.", 0) \
    M(Seconds, temporary_directories_lifetime, 86400, "How many seconds to keep tmp_-directories. You should not lower this value because merges and mutations may not be able to work with low value of this setting.", 0) \
    M(Seconds, lock_acquire_timeout_for_background_operations, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "For background operations like merges, mutations etc. How many seconds before failing to acquire table locks.", 0) \
    M(UInt64, min_rows_to_fsync_after_merge, 0, "Minimal number of rows to do fsync for part after merge (0 - disabled)", 0) \
    M(UInt64, min_compressed_bytes_to_fsync_after_merge, 0, "Minimal number of compressed bytes to do fsync for part after merge (0 - disabled)", 0) \
    M(UInt64, min_compressed_bytes_to_fsync_after_fetch, 0, "Minimal number of compressed bytes to do fsync for part after fetch (0 - disabled)", 0) \
    M(Bool, fsync_after_insert, false, "Do fsync for every inserted part. Significantly decreases performance of inserts, not recommended to use with wide parts.", 0) \
    M(Bool, fsync_part_directory, false, "Do fsync for part directory after all part operations (writes, renames, etc.).", 0) \
    M(UInt64, non_replicated_deduplication_window, 0, "How many last blocks of hashes should be kept on disk (0 - disabled).", 0) \
    M(UInt64, max_parts_to_merge_at_once, 100, "Max amount of parts which can be merged at once (0 - disabled). Doesn't affect OPTIMIZE FINAL query.", 0) \
    M(UInt64, merge_selecting_sleep_ms, 5000, "Maximum sleep time for merge selecting, a lower setting will trigger selecting tasks in background_schedule_pool frequently which result in large amount of requests to zookeeper in large-scale clusters", 0) \
    M(UInt64, max_merge_selecting_sleep_ms, 60000, "Maximum sleep time for merge selecting, a lower setting will trigger selecting tasks in background_schedule_pool frequently which result in large amount of requests to zookeeper in large-scale clusters", 0) \
    M(Float, merge_selecting_sleep_slowdown_factor, 1.2f, "The sleep time for merge selecting task is multiplied by this factor when there's nothing to merge and divided when a merge was assigned", 0) \
    M(UInt64, merge_tree_clear_old_temporary_directories_interval_seconds, 60, "The period of executing the clear old temporary directories operation in background.", 0) \
    M(UInt64, merge_tree_clear_old_parts_interval_seconds, 1, "The period of executing the clear old parts operation in background.", 0) \
    M(UInt64, merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds, 1ULL * 3600 * 24 * 30, "Remove old broken detached parts in the background if they remained intouched for a specified by this setting period of time.", 0) \
    M(UInt64, min_age_to_force_merge_seconds, 0, "If all parts in a certain range are older than this value, range will be always eligible for merging. Set to 0 to disable.", 0) \
    M(Bool, min_age_to_force_merge_on_partition_only, false, "Whether min_age_to_force_merge_seconds should be applied only on the entire partition and not on subset.", false) \
    M(UInt64, number_of_free_entries_in_pool_to_execute_optimize_entire_partition, 25, "When there is less than specified number of free entries in pool, do not try to execute optimize entire partition with a merge (this merge is created when set min_age_to_force_merge_seconds > 0 and min_age_to_force_merge_on_partition_only = true). This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    M(UInt64, merge_tree_enable_clear_old_broken_detached, false, "Enable clearing old broken detached parts operation in background.", 0) \
    M(Bool, remove_rolled_back_parts_immediately, 1, "Setting for an incomplete experimental feature.", 0) \
    M(CleanDeletedRows, clean_deleted_rows, CleanDeletedRows::Never, "Is the Replicated Merge cleanup has to be done automatically at each merge or manually (possible values are 'Always'/'Never' (default))", 0) \
    M(UInt64, replicated_max_mutations_in_one_entry, 10000, "Max number of mutation commands that can be merged together and executed in one MUTATE_PART entry (0 means unlimited)", 0) \
    M(UInt64, number_of_mutations_to_delay, 500, "If table has at least that many unfinished mutations, artificially slow down mutations of table. Disabled if set to 0", 0) \
    M(UInt64, number_of_mutations_to_throw, 1000, "If table has at least that many unfinished mutations, throw 'Too many mutations' exception. Disabled if set to 0", 0) \
    M(UInt64, min_delay_to_mutate_ms, 10, "Min delay of mutating MergeTree table in milliseconds, if there are a lot of unfinished mutations", 0) \
    M(UInt64, max_delay_to_mutate_ms, 1000, "Max delay of mutating MergeTree table in milliseconds, if there are a lot of unfinished mutations", 0) \
    \
    /** Inserts settings. */ \
    M(UInt64, parts_to_delay_insert, 1000, "If table contains at least that many active parts in single partition, artificially slow down insert into table. Disabled if set to 0", 0) \
    M(UInt64, inactive_parts_to_delay_insert, 0, "If table contains at least that many inactive parts in single partition, artificially slow down insert into table.", 0) \
    M(UInt64, parts_to_throw_insert, 3000, "If more than this number active parts in single partition, throw 'Too many parts ...' exception.", 0) \
    M(UInt64, inactive_parts_to_throw_insert, 0, "If more than this number inactive parts in single partition, throw 'Too many inactive parts ...' exception.", 0) \
    M(UInt64, max_avg_part_size_for_too_many_parts, 1ULL * 1024 * 1024 * 1024, "The 'too many parts' check according to 'parts_to_delay_insert' and 'parts_to_throw_insert' will be active only if the average part size (in the relevant partition) is not larger than the specified threshold. If it is larger than the specified threshold, the INSERTs will be neither delayed or rejected. This allows to have hundreds of terabytes in a single table on a single server if the parts are successfully merged to larger parts. This does not affect the thresholds on inactive parts or total parts.", 0) \
    M(UInt64, max_delay_to_insert, 1, "Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts in single partition.", 0) \
    M(UInt64, min_delay_to_insert_ms, 10, "Min delay of inserting data into MergeTree table in milliseconds, if there are a lot of unmerged parts in single partition.", 0) \
    M(UInt64, max_parts_in_total, 100000, "If more than this number active parts in all partitions in total, throw 'Too many parts ...' exception.", 0) \
    M(Bool, async_insert, false, "If true, data from INSERT query is stored in queue and later flushed to table in background.", 0) \
    \
    /* Part removal settings. */ \
    M(UInt64, simultaneous_parts_removal_limit, 0, "Maximum number of parts to remove during one CleanupThread iteration (0 means unlimited).", 0) \
    \
    /** Replication settings. */ \
    M(UInt64, replicated_deduplication_window, 100, "How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).", 0) \
    M(UInt64, replicated_deduplication_window_seconds, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", 0) \
    M(UInt64, replicated_deduplication_window_for_async_inserts, 10000, "How many last hash values of async_insert blocks should be kept in ZooKeeper (old blocks will be deleted).", 0) \
    M(UInt64, replicated_deduplication_window_seconds_for_async_inserts, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window_for_async_inserts\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", 0) \
    M(Milliseconds, async_block_ids_cache_min_update_interval_ms, 100, "minimum interval between updates of async_block_ids_cache", 0) \
    M(Bool, use_async_block_ids_cache, false, "use in-memory cache to filter duplicated async inserts based on block ids", 0) \
    M(UInt64, max_replicated_logs_to_keep, 1000, "How many records may be in log, if there is inactive replica. Inactive replica becomes lost when when this number exceed.", 0) \
    M(UInt64, min_replicated_logs_to_keep, 10, "Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.", 0) \
    M(Seconds, prefer_fetch_merged_part_time_threshold, 3600, "If time passed after replication log entry creation exceeds this threshold and sum size of parts is greater than \"prefer_fetch_merged_part_size_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    M(UInt64, prefer_fetch_merged_part_size_threshold, 10ULL * 1024 * 1024 * 1024, "If sum size of parts exceeds this threshold and time passed after replication log entry creation is greater than \"prefer_fetch_merged_part_time_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    M(Seconds, execute_merges_on_single_replica_time_threshold, 0, "When greater than zero only a single replica starts the merge immediately, others wait up to that amount of time to download the result instead of doing merges locally. If the chosen replica doesn't finish the merge during that amount of time, fallback to standard behavior happens.", 0) \
    M(Seconds, remote_fs_execute_merges_on_single_replica_time_threshold, 3 * 60 * 60, "When greater than zero only a single replica starts the merge immediately if merged part on shared storage and 'allow_remote_fs_zero_copy_replication' is enabled.", 0) \
    M(Seconds, try_fetch_recompressed_part_timeout, 7200, "Recompression works slow in most cases, so we don't start merge with recompression until this timeout and trying to fetch recompressed part from replica which assigned this merge with recompression.", 0) \
    M(Bool, always_fetch_merged_part, false, "If true, replica never merge parts and always download merged parts from other replicas.", 0) \
    M(UInt64, max_suspicious_broken_parts, 100, "Max broken parts, if more - deny automatic deletion.", 0) \
    M(UInt64, max_suspicious_broken_parts_bytes, 1ULL * 1024 * 1024 * 1024, "Max size of all broken parts, if more - deny automatic deletion.", 0) \
    M(UInt64, max_files_to_modify_in_alter_columns, 75, "Not apply ALTER if number of files for modification(deletion, addition) more than this.", 0) \
    M(UInt64, max_files_to_remove_in_alter_columns, 50, "Not apply ALTER, if number of files for deletion more than this.", 0) \
    M(Float, replicated_max_ratio_of_wrong_parts, 0.5, "If ratio of wrong parts to total number of parts is less than this - allow to start.", 0) \
    M(UInt64, replicated_max_parallel_fetches_for_host, DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT, "Limit parallel fetches from endpoint (actually pool size).", 0) \
    M(Seconds, replicated_fetches_http_connection_timeout, 0, "HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.", 0) \
    M(Seconds, replicated_fetches_http_send_timeout, 0, "HTTP send timeout for part fetch requests. Inherited from default profile `http_send_timeout` if not set explicitly.", 0) \
    M(Seconds, replicated_fetches_http_receive_timeout, 0, "HTTP receive timeout for fetch part requests. Inherited from default profile `http_receive_timeout` if not set explicitly.", 0) \
    M(Bool, replicated_can_become_leader, true, "If true, Replicated tables replicas on this node will try to acquire leadership.", 0) \
    M(Seconds, zookeeper_session_expiration_check_period, 60, "ZooKeeper session expiration check period, in seconds.", 0) \
    M(Seconds, initialization_retry_period, 60, "Retry period for table initialization, in seconds.", 0) \
    M(Bool, detach_old_local_parts_when_cloning_replica, true, "Do not remove old local parts when repairing lost replica.", 0) \
    M(Bool, detach_not_byte_identical_parts, false, "Do not remove non byte-idential parts for ReplicatedMergeTree, instead detach them (maybe useful for further analysis).", 0) \
    M(UInt64, max_replicated_fetches_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.", 0) \
    M(UInt64, max_replicated_sends_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.", 0) \
    M(Milliseconds, wait_for_unique_parts_send_before_shutdown_ms, 0, "Before shutdown table will wait for required amount time for unique parts (exist only on current replica) to be fetched by other replicas (0 means disabled).", 0) \
    \
    /** Check delay of replicas settings. */ \
    M(UInt64, min_relative_delay_to_measure, 120, "Calculate relative replica delay only if absolute delay is not less that this value.", 0) \
    M(UInt64, cleanup_delay_period, 30, "Minimum period to clean old queue logs, blocks hashes and parts.", 0) \
    M(UInt64, max_cleanup_delay_period, 300, "Maximum period to clean old queue logs, blocks hashes and parts.", 0) \
    M(UInt64, cleanup_delay_period_random_add, 10, "Add uniformly distributed value from 0 to x seconds to cleanup_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables.", 0) \
    M(UInt64, cleanup_thread_preferred_points_per_iteration, 150, "Preferred batch size for background cleanup (points are abstract but 1 point is approximately equivalent to 1 inserted block).", 0) \
    M(UInt64, min_relative_delay_to_close, 300, "Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, min_absolute_delay_to_close, 0, "Minimal absolute delay to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, enable_vertical_merge_algorithm, 1, "Enable usage of Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * 8192, "Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_bytes_to_activate, 0, "Minimal (approximate) uncompressed size in bytes in merging parts to activate Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_columns_to_activate, 11, "Minimal amount of non-PK columns to activate Vertical merge algorithm.", 0) \
    \
    /** Compatibility settings */ \
    M(Bool, allow_suspicious_indices, false, "Reject primary/secondary indexes and sorting keys with identical expressions", 0) \
    M(Bool, compatibility_allow_sampling_expression_not_in_primary_key, false, "Allow to create a table with sampling expression not in primary key. This is needed only to temporarily allow to run the server with wrong tables for backward compatibility.", 0) \
    M(Bool, use_minimalistic_checksums_in_zookeeper, true, "Use small format (dozens bytes) for part checksums in ZooKeeper instead of ordinary ones (dozens KB). Before enabling check that all replicas support new format.", 0) \
    M(Bool, use_minimalistic_part_header_in_zookeeper, true, "Store part header (checksums and columns) in a compact format and a single part znode instead of separate znodes (<part>/columns and <part>/checksums). This can dramatically reduce snapshot size in ZooKeeper. Before enabling check that all replicas support new format.", 0) \
    M(UInt64, finished_mutations_to_keep, 100, "How many records about mutations that are done to keep. If zero, then keep all of them.", 0) \
    M(UInt64, min_merge_bytes_to_use_direct_io, 10ULL * 1024 * 1024 * 1024, "Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled).", 0) \
    M(UInt64, index_granularity_bytes, 10 * 1024 * 1024, "Approximate amount of bytes in single granule (0 - disabled).", 0) \
    M(UInt64, min_index_granularity_bytes, 1024, "Minimum amount of bytes in single granule.", 1024) \
    M(Int64, merge_with_ttl_timeout, 3600 * 4, "Minimal time in seconds, when merge with delete TTL can be repeated.", 0) \
    M(Int64, merge_with_recompression_ttl_timeout, 3600 * 4, "Minimal time in seconds, when merge with recompression TTL can be repeated.", 0) \
    M(Bool, ttl_only_drop_parts, false, "Only drop altogether the expired parts and not partially prune them.", 0) \
    M(Bool, materialize_ttl_recalculate_only, false, "Only recalculate ttl info when MATERIALIZE TTL", 0) \
    M(Bool, enable_mixed_granularity_parts, true, "Enable parts with adaptive and non adaptive granularity", 0) \
    M(UInt64, concurrent_part_removal_threshold, 100, "Activate concurrent part removal (see 'max_part_removal_threads') only if the number of inactive data parts is at least this.", 0) \
    M(UInt64, zero_copy_concurrent_part_removal_max_split_times, 5, "Max recursion depth for splitting independent Outdated parts ranges into smaller subranges (highly not recommended to change)", 0) \
    M(Float, zero_copy_concurrent_part_removal_max_postpone_ratio, static_cast<Float32>(0.05), "Max percentage of top level parts to postpone removal in order to get smaller independent ranges (highly not recommended to change)", 0) \
    M(String, storage_policy, "default", "Name of storage disk policy", 0) \
    M(String, disk, "", "Name of storage disk. Can be specified instead of storage policy.", 0) \
    M(Bool, allow_nullable_key, false, "Allow Nullable types as primary keys.", 0) \
    M(Bool, remove_empty_parts, true, "Remove empty parts after they were pruned by TTL, mutation, or collapsing merge algorithm.", 0) \
    M(Bool, assign_part_uuids, false, "Generate UUIDs for parts. Before enabling check that all replicas support new format.", 0) \
    M(Int64, max_partitions_to_read, -1, "Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited. This setting is the default that can be overridden by the query-level setting with the same name.", 0) \
    M(UInt64, max_concurrent_queries, 0, "Max number of concurrently executed queries related to the MergeTree table (0 - disabled). Queries will still be limited by other max_concurrent_queries settings.", 0) \
    M(UInt64, min_marks_to_honor_max_concurrent_queries, 0, "Minimal number of marks to honor the MergeTree-level's max_concurrent_queries (0 - disabled). Queries will still be limited by other max_concurrent_queries settings.", 0) \
    M(UInt64, min_bytes_to_rebalance_partition_over_jbod, 0, "Minimal amount of bytes to enable part rebalance over JBOD array (0 - disabled).", 0) \
    M(Bool, check_sample_column_is_correct, true, "Check columns or columns by hash for sampling are unsigned integer.", 0) \
    M(Bool, allow_vertical_merges_from_compact_to_wide_parts, true, "Allows vertical merges from compact to wide parts. This settings must have the same value on all replicas", 0) \
    M(Bool, enable_the_endpoint_id_with_zookeeper_name_prefix, false, "Enable the endpoint id with zookeeper name prefix for the replicated merge tree table", 0) \
    M(UInt64, zero_copy_merge_mutation_min_parts_size_sleep_before_lock, 1ULL * 1024 * 1024 * 1024, "If zero copy replication is enabled sleep random amount of time before trying to lock depending on parts size for merge or mutation", 0) \
    \
    /** Experimental/work in progress feature. Unsafe for production. */ \
    M(UInt64, part_moves_between_shards_enable, 0, "Experimental/Incomplete feature to move parts between shards. Does not take into account sharding expressions.", 0) \
    M(UInt64, part_moves_between_shards_delay_seconds, 30, "Time to wait before/after moving parts between shards.", 0) \
    M(Bool, use_metadata_cache, false, "Experimental feature to speed up parts loading process by using MergeTree metadata cache", 0) \
    M(Bool, allow_remote_fs_zero_copy_replication, false, "Don't use this setting in production, because it is not ready.", 0) \
    M(String, remote_fs_zero_copy_zookeeper_path, "/clickhouse/zero_copy", "ZooKeeper path for zero-copy table-independent info.", 0) \
    M(Bool, remote_fs_zero_copy_path_compatible_mode, false, "Run zero-copy in compatible mode during conversion process.", 0) \
    \
    /** Compress marks and primary key. */ \
    M(Bool, compress_marks, true, "Marks support compression, reduce mark file size and speed up network transmission.", 0) \
    M(Bool, compress_primary_key, true, "Primary key support compression, reduce primary key file size and speed up network transmission.", 0) \
    M(String, marks_compression_codec, "ZSTD(3)", "Compression encoding used by marks, marks are small enough and cached, so the default compression is ZSTD(3).", 0) \
    M(String, primary_key_compression_codec, "ZSTD(3)", "Compression encoding used by primary, primary key is small enough and cached, so the default compression is ZSTD(3).", 0) \
    M(UInt64, marks_compress_block_size, 65536, "Mark compress block size, the actual size of the block to compress.", 0) \
    M(UInt64, primary_key_compress_block_size, 65536, "Primary compress block size, the actual size of the block to compress.", 0) \
    \
    /** Obsolete settings. Kept for backward compatibility only. */ \
    M(UInt64, min_relative_delay_to_yield_leadership, 120, "Obsolete setting, does nothing.", 0) \
    M(UInt64, check_delay_period, 60, "Obsolete setting, does nothing.", 0) \
    M(Bool, allow_floating_point_partition_key, false, "Allow floating point as partition key", 0) \
    M(UInt64, replicated_max_parallel_sends, 0, "Obsolete setting, does nothing.", 0) \
    M(UInt64, replicated_max_parallel_sends_for_table, 0, "Obsolete setting, does nothing.", 0) \
    M(UInt64, replicated_max_parallel_fetches, 0, "Obsolete setting, does nothing.", 0) \
    M(UInt64, replicated_max_parallel_fetches_for_table, 0, "Obsolete setting, does nothing.", 0) \
    M(Bool, write_final_mark, true, "Obsolete setting, does nothing.", 0)                                                                                                                                                                                            \
    M(UInt64, min_bytes_for_compact_part, 0, "Obsolete setting, does nothing.", 0) \
    M(UInt64, min_rows_for_compact_part, 0, "Obsolete setting, does nothing.", 0) \
    M(Bool, in_memory_parts_enable_wal, true, "Obsolete setting, does nothing.", 0) \
    M(UInt64, write_ahead_log_max_bytes, 1024 * 1024 * 1024, "Obsolete setting, does nothing.", 0) \
    M(UInt64, write_ahead_log_bytes_to_fsync, 100ULL * 1024 * 1024, "Obsolete setting, does nothing.", 0) \
    M(UInt64, write_ahead_log_interval_ms_to_fsync, 100, "Obsolete setting, does nothing.", 0) \
    M(Bool, in_memory_parts_insert_sync, false, "Obsolete setting, does nothing.", 0) \
    M(MaxThreads, max_part_loading_threads, 0, "Obsolete setting, does nothing.", 0) \
    M(MaxThreads, max_part_removal_threads, 0, "Obsolete setting, does nothing.", 0) \

    /// Settings that should not change after the creation of a table.
    /// NOLINTNEXTLINE
#define APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(M) \
    M(index_granularity)

DECLARE_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)


/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettings : public BaseSettings<MergeTreeSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context);

    /// We check settings after storage creation
    static bool isReadonlySetting(const String & name)
    {
        return name == "index_granularity" || name == "index_granularity_bytes"
            || name == "enable_mixed_granularity_parts";
    }

    static bool isPartFormatSetting(const String & name)
    {
        return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part";
    }

    /// Check that the values are sane taking also query-level settings into account.
    void sanityCheck(size_t background_pool_tasks) const;
};

using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

}
