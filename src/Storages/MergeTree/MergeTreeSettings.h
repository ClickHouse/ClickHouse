#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;


#define LIST_OF_MERGE_TREE_SETTINGS(M) \
    M(UInt64, index_granularity, 8192, "How many rows correspond to one primary key value.", 0) \
    \
    /** Data storing format settings. */ \
    M(UInt64, min_bytes_for_wide_part, 0, "Minimal uncompressed size in bytes to create part in wide format instead of compact", 0) \
    M(UInt64, min_rows_for_wide_part, 0, "Minimal number of rows to create part in wide format instead of compact", 0) \
    M(UInt64, min_bytes_for_compact_part, 0, "Experimental. Minimal uncompressed size in bytes to create part in compact format instead of saving it in RAM", 0) \
    M(UInt64, min_rows_for_compact_part, 0, "Experimental. Minimal number of rows to create part in compact format instead of saving it in RAM", 0) \
    M(Bool, in_memory_parts_enable_wal, true, "Whether to write blocks in Native format to write-ahead-log before creation in-memory part", 0) \
    M(UInt64, write_ahead_log_max_bytes, 1024 * 1024 * 1024, "Rotate WAL, if it exceeds that amount of bytes", 0) \
    \
    /** Merge settings. */ \
    M(UInt64, merge_max_block_size, DEFAULT_MERGE_BLOCK_SIZE, "How many rows in blocks should be formed for merge operations.", 0) \
    M(UInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, "Maximum in total size of parts to merge, when there are maximum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, "Maximum in total size of parts to merge, when there are minimum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_replicated_merges_in_queue, 16, "How many tasks of merging and mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, max_replicated_mutations_in_queue, 8, "How many tasks of mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue). This is to allow small merges to process - not filling the pool with long running merges.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_execute_mutation, 10, "When there is less than specified number of free entries in pool, do not execute part mutations. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    M(Seconds, old_parts_lifetime, 8 * 60, "How many seconds to keep obsolete parts.", 0) \
    M(Seconds, temporary_directories_lifetime, 86400, "How many seconds to keep tmp_-directories.", 0) \
    M(Seconds, lock_acquire_timeout_for_background_operations, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "For background operations like merges, mutations etc. How many seconds before failing to acquire table locks.", 0) \
    \
    /** Inserts settings. */ \
    M(UInt64, parts_to_delay_insert, 150, "If table contains at least that many active parts in single partition, artificially slow down insert into table.", 0) \
    M(UInt64, parts_to_throw_insert, 300, "If more than this number active parts in single partition, throw 'Too many parts ...' exception.", 0) \
    M(UInt64, max_delay_to_insert, 1, "Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts in single partition.", 0) \
    M(UInt64, max_parts_in_total, 100000, "If more than this number active parts in all partitions in total, throw 'Too many parts ...' exception.", 0) \
    \
    /** Replication settings. */ \
    M(UInt64, replicated_deduplication_window, 100, "How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).", 0) \
    M(UInt64, replicated_deduplication_window_seconds, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", 0) \
    M(UInt64, max_replicated_logs_to_keep, 100, "How many records may be in log, if there is inactive replica.", 0) \
    M(UInt64, min_replicated_logs_to_keep, 10, "Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.", 0) \
    M(Seconds, prefer_fetch_merged_part_time_threshold, 3600, "If time passed after replication log entry creation exceeds this threshold and sum size of parts is greater than \"prefer_fetch_merged_part_size_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    M(UInt64, prefer_fetch_merged_part_size_threshold, 10ULL * 1024 * 1024 * 1024, "If sum size of parts exceeds this threshold and time passed after replication log entry creation is greater than \"prefer_fetch_merged_part_time_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.", 0) \
    M(Bool, always_fetch_merged_part, 0, "If true, replica never merge parts and always download merged parts from other replicas.", 0) \
    M(UInt64, max_suspicious_broken_parts, 10, "Max broken parts, if more - deny automatic deletion.", 0) \
    M(UInt64, max_files_to_modify_in_alter_columns, 75, "Not apply ALTER if number of files for modification(deletion, addition) more than this.", 0) \
    M(UInt64, max_files_to_remove_in_alter_columns, 50, "Not apply ALTER, if number of files for deletion more than this.", 0) \
    M(Float, replicated_max_ratio_of_wrong_parts, 0.5, "If ratio of wrong parts to total number of parts is less than this - allow to start.", 0) \
    M(UInt64, replicated_max_parallel_fetches, 0, "Limit parallel fetches.", 0) \
    M(UInt64, replicated_max_parallel_fetches_for_table, 0, "Limit parallel fetches for one table.", 0) \
    M(UInt64, replicated_max_parallel_fetches_for_host, DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT, "Limit parallel fetches from endpoint (actually pool size).", 0) \
    M(UInt64, replicated_max_parallel_sends, 0, "Limit parallel sends.", 0) \
    M(UInt64, replicated_max_parallel_sends_for_table, 0, "Limit parallel sends for one table.", 0) \
    M(Bool, replicated_can_become_leader, true, "If true, Replicated tables replicas on this node will try to acquire leadership.", 0) \
    M(Seconds, zookeeper_session_expiration_check_period, 60, "ZooKeeper session expiration check period, in seconds.", 0) \
    \
    /** Check delay of replicas settings. */ \
    M(UInt64, min_relative_delay_to_measure, 120, "Calculate relative replica delay only if absolute delay is not less that this value.", 0) \
    M(UInt64, cleanup_delay_period, 30, "Period to clean old queue logs, blocks hashes and parts.", 0) \
    M(UInt64, cleanup_delay_period_random_add, 10, "Add uniformly distributed value from 0 to x seconds to cleanup_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables.", 0) \
    M(UInt64, min_relative_delay_to_close, 300, "Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, min_absolute_delay_to_close, 0, "Minimal absolute delay to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, enable_vertical_merge_algorithm, 1, "Enable usage of Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * DEFAULT_MERGE_BLOCK_SIZE, "Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_columns_to_activate, 11, "Minimal amount of non-PK columns to activate Vertical merge algorithm.", 0) \
    \
    /** Compatibility settings */ \
    M(Bool, compatibility_allow_sampling_expression_not_in_primary_key, false, "Allow to create a table with sampling expression not in primary key. This is needed only to temporarily allow to run the server with wrong tables for backward compatibility.", 0) \
    M(Bool, use_minimalistic_checksums_in_zookeeper, true, "Use small format (dozens bytes) for part checksums in ZooKeeper instead of ordinary ones (dozens KB). Before enabling check that all replicas support new format.", 0) \
    M(Bool, use_minimalistic_part_header_in_zookeeper, true, "Store part header (checksums and columns) in a compact format and a single part znode instead of separate znodes (<part>/columns and <part>/checksums). This can dramatically reduce snapshot size in ZooKeeper. Before enabling check that all replicas support new format.", 0) \
    M(UInt64, finished_mutations_to_keep, 100, "How many records about mutations that are done to keep. If zero, then keep all of them.", 0) \
    M(UInt64, min_merge_bytes_to_use_direct_io, 10ULL * 1024 * 1024 * 1024, "Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled).", 0) \
    M(UInt64, index_granularity_bytes, 10 * 1024 * 1024, "Approximate amount of bytes in single granule (0 - disabled).", 0) \
    M(Int64, merge_with_ttl_timeout, 3600 * 24, "Minimal time in seconds, when merge with TTL can be repeated.", 0) \
    M(Bool, ttl_only_drop_parts, false, "Only drop altogether the expired parts and not partially prune them.", 0) \
    M(Bool, write_final_mark, 1, "Write final mark after end of column (0 - disabled, do nothing if index_granularity_bytes=0)", 0) \
    M(Bool, enable_mixed_granularity_parts, 0, "Enable parts with adaptive and non adaptive granularity", 0) \
    M(MaxThreads, max_part_loading_threads, 0, "The number of threads to load data parts at startup.", 0) \
    M(MaxThreads, max_part_removal_threads, 0, "The number of threads for concurrent removal of inactive data parts. One is usually enough, but in 'Google Compute Environment SSD Persistent Disks' file removal (unlink) operation is extraordinarily slow and you probably have to increase this number (recommended is up to 16).", 0) \
    M(UInt64, concurrent_part_removal_threshold, 100, "Activate concurrent part removal (see 'max_part_removal_threads') only if the number of inactive data parts is at least this.", 0) \
    M(String, storage_policy, "default", "Name of storage disk policy", 0) \
    M(Bool, allow_nullable_key, false, "Allow Nullable types as primary keys.", 0) \
    \
    /** Obsolete settings. Kept for backward compatibility only. */ \
    M(UInt64, min_relative_delay_to_yield_leadership, 120, "Obsolete setting, does nothing.", 0) \
    M(UInt64, check_delay_period, 60, "Obsolete setting, does nothing.", 0) \

    /// Settings that should not change after the creation of a table.
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
    void loadFromQuery(ASTStorage & storage_def);

    /// We check settings after storage creation
    static bool isReadonlySetting(const String & name)
    {
        return name == "index_granularity" || name == "index_granularity_bytes";
    }

    static bool isPartFormatSetting(const String & name)
    {
        return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part"
            || name == "min_bytes_for_compact_part" || name == "min_rows_for_compact_part";
    }
};

using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

}
