#pragma once

#include <Core/Defines.h>
#include <Core/SettingsCommon.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{

class ASTStorage;

/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettings : public SettingsCollection<MergeTreeSettings>
{

#define LIST_OF_MERGE_TREE_SETTINGS(M) \
    M(SettingUInt64, index_granularity, 8192, "How many rows correspond to one primary key value.") \
    \
    /** Merge settings. */ \
    M(SettingUInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, "Maximum in total size of parts to merge, when there are maximum free threads in background pool (or entries in replication queue).") \
    M(SettingUInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, "Maximum in total size of parts to merge, when there are minimum free threads in background pool (or entries in replication queue).") \
    M(SettingUInt64, max_replicated_merges_in_queue, 16, "How many tasks of merging parts are allowed simultaneously in ReplicatedMergeTree queue.") \
    M(SettingUInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue). This is to allow small merges to process - not filling the pool with long running merges.") \
    M(SettingSeconds, old_parts_lifetime, 8 * 60, "How many seconds to keep obsolete parts.") \
    M(SettingSeconds, temporary_directories_lifetime, 86400, "How many seconds to keep tmp_-directories.") \
    \
    /** Inserts settings. */ \
    M(SettingUInt64, parts_to_delay_insert, 150, "If table contains at least that many active parts in single partition, artificially slow down insert into table.") \
    M(SettingUInt64, parts_to_throw_insert, 300, "If more than this number active parts in single partition, throw 'Too many parts ...' exception.") \
    M(SettingUInt64, max_delay_to_insert, 1, "Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts in single partition.") \
    M(SettingUInt64, max_parts_in_total, 100000, "If more than this number active parts in all partitions in total, throw 'Too many parts ...' exception.") \
    \
    /** Replication settings. */ \
    M(SettingUInt64, replicated_deduplication_window, 100, "How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).") \
    M(SettingUInt64, replicated_deduplication_window_seconds, 7 * 24 * 60 * 60 /* one week */, "Similar to \"replicated_deduplication_window\", but determines old blocks by their lifetime. Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.") \
    M(SettingUInt64, max_replicated_logs_to_keep, 10000, "How many records may be in log, if there is inactive replica.") \
    M(SettingUInt64, min_replicated_logs_to_keep, 100, "Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.") \
    M(SettingSeconds, prefer_fetch_merged_part_time_threshold, 3600, "If time passed after replication log entry creation exceeds this threshold and sum size of parts is greater than \"prefer_fetch_merged_part_size_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.") \
    M(SettingUInt64, prefer_fetch_merged_part_size_threshold, 10ULL * 1024 * 1024 * 1024, "If sum size of parts exceeds this threshold and time passed after replication log entry creation is greater than \"prefer_fetch_merged_part_time_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up very long merges.") \
    M(SettingUInt64, max_suspicious_broken_parts, 10, "Max broken parts, if more - deny automatic deletion.") \
    M(SettingUInt64, max_files_to_modify_in_alter_columns, 75, "Not apply ALTER if number of files for modification(deletion, addition) more than this.") \
    M(SettingUInt64, max_files_to_remove_in_alter_columns, 50, "Not apply ALTER, if number of files for deletion more than this.") \
    M(SettingFloat, replicated_max_ratio_of_wrong_parts, 0.5, "If ratio of wrong parts to total number of parts is less than this - allow to start.") \
    M(SettingUInt64, replicated_max_parallel_fetches, 0, "Limit parallel fetches.") \
    M(SettingUInt64, replicated_max_parallel_fetches_for_table, 0, "Limit parallel fetches for one table.") \
    M(SettingUInt64, replicated_max_parallel_fetches_for_host, DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT, "Limit parallel fetches from endpoint (actually pool size).") \
    M(SettingUInt64, replicated_max_parallel_sends, 0, "Limit parallel sends.") \
    M(SettingUInt64, replicated_max_parallel_sends_for_table, 0, "Limit parallel sends for one table.") \
    M(SettingBool, replicated_can_become_leader, true, "If true, Replicated tables replicas on this node will try to acquire leadership.") \
    M(SettingSeconds, zookeeper_session_expiration_check_period, 60, "ZooKeeper session expiration check period, in seconds.") \
    \
    /** Check delay of replicas settings. */ \
    M(SettingUInt64, check_delay_period, 60, "Period to check replication delay and compare with other replicas.") \
    M(SettingUInt64, cleanup_delay_period, 30, "Period to clean old queue logs, blocks hashes and parts.") \
    M(SettingUInt64, cleanup_delay_period_random_add, 10, "Add uniformly distributed value from 0 to x seconds to cleanup_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables.") \
    M(SettingUInt64, min_relative_delay_to_yield_leadership, 120, "Minimal delay from other replicas to yield leadership. Here and further 0 means unlimited.") \
    M(SettingUInt64, min_relative_delay_to_close, 300, "Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.") \
    M(SettingUInt64, min_absolute_delay_to_close, 0, "Minimal absolute delay to close, stop serving requests and not return Ok during status check.") \
    M(SettingUInt64, enable_vertical_merge_algorithm, 1, "Enable usage of Vertical merge algorithm.") \
    M(SettingUInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * DEFAULT_MERGE_BLOCK_SIZE, "Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm.") \
    M(SettingUInt64, vertical_merge_algorithm_min_columns_to_activate, 11, "Minimal amount of non-PK columns to activate Vertical merge algorithm.") \
    \
    /** Compatibility settings */ \
    M(SettingBool, compatibility_allow_sampling_expression_not_in_primary_key, false, "Allow to create a table with sampling expression not in primary key. This is needed only to temporarily allow to run the server with wrong tables for backward compatibility.") \
    M(SettingBool, use_minimalistic_checksums_in_zookeeper, true, "Use small format (dozens bytes) for part checksums in ZooKeeper instead of ordinary ones (dozens KB). Before enabling check that all replicas support new format.") \
    M(SettingBool, use_minimalistic_part_header_in_zookeeper, false, "Store part header (checksums and columns) in a compact format and a single part znode instead of separate znodes (<part>/columns and <part>/checksums). This can dramatically reduce snapshot size in ZooKeeper. Before enabling check that all replicas support new format.") \
    M(SettingUInt64, finished_mutations_to_keep, 100, "How many records about mutations that are done to keep. If zero, then keep all of them.") \
    M(SettingUInt64, min_merge_bytes_to_use_direct_io, 10ULL * 1024 * 1024 * 1024, "Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled).") \
    M(SettingUInt64, index_granularity_bytes, 0, "Approximate amount of bytes in single granule (0 - disabled).") \
    M(SettingInt64, merge_with_ttl_timeout, 3600 * 24, "Minimal time in seconds, when merge with TTL can be repeated.")

    DECLARE_SETTINGS_COLLECTION(LIST_OF_MERGE_TREE_SETTINGS)

    /// Settings that should not change after the creation of a table.
#define APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(M) \
    M(index_granularity)

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def);
};

}
