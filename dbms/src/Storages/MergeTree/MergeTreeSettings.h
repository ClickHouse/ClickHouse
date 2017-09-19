#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/Types.h>


namespace DB
{

class ASTStorage;

/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettings
{
    /// how many rows correspond to one primary key value.
    UInt64 index_granularity = 8192;

    /** Merge settings. */

    /// Maximum in total size of parts to merge, when there are maximum (minimum) free threads in background pool (or entries in replication queue).
    UInt64 max_bytes_to_merge_at_max_space_in_pool = 150ULL * 1024 * 1024 * 1024;
    UInt64 max_bytes_to_merge_at_min_space_in_pool = 1024 * 1024;

    /// How many tasks of merging parts are allowed simultaneously in ReplicatedMergeTree queue.
    UInt64 max_replicated_merges_in_queue = 16;

    /// When there is less than specified number of free entries in pool (or replicated queue),
    ///  start to lower maximum size of merge to process (or to put in queue).
    /// This is to allow small merges to process - not filling the pool with long running merges.
    UInt64 number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8;

    /// How many seconds to keep obsolete parts.
    time_t old_parts_lifetime = 8 * 60;

    /// How many seconds to keep tmp_-directories.
    time_t temporary_directories_lifetime = 86400;

    /** Inserts settings. */

    /// If table contains at least that many active parts, artificially slow down insert into table.
    UInt64 parts_to_delay_insert = 150;

    /// If more than this number active parts, throw 'Too much parts ...' exception
    UInt64 parts_to_throw_insert = 300;

    /// Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts.
    UInt64 max_delay_to_insert = 1;

    /** Replication settings. */

    /// How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).
    UInt64 replicated_deduplication_window = 100;
    /// Similar to previous, but determines old blocks by their lifetime.
    /// Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one "window".
    /// You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.
    UInt64 replicated_deduplication_window_seconds = 7 * 24 * 60 * 60; /// one week

    /// Keep about this number of last records in ZooKeeper log, even if they are obsolete.
    /// It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.
    UInt64 replicated_logs_to_keep = 100;

    /// After specified amount of time passed after replication log entry creation
    ///  and sum size of parts is greater than threshold,
    ///  prefer fetching merged part from replica instead of doing merge locally.
    /// To speed up very long merges.
    time_t prefer_fetch_merged_part_time_threshold = 3600;
    UInt64 prefer_fetch_merged_part_size_threshold = 10ULL * 1024 * 1024 * 1024;

    /// Max broken parts, if more - deny automatic deletion.
    UInt64 max_suspicious_broken_parts = 10;

    /// Not apply ALTER if number of files for modification(deletion, addition) more than this.
    UInt64 max_files_to_modify_in_alter_columns = 75;
    /// Not apply ALTER, if number of files for deletion more than this.
    UInt64 max_files_to_remove_in_alter_columns = 50;

    /// If ratio of wrong parts to total number of parts is less than this - allow to start.
    double replicated_max_ratio_of_wrong_parts = 0.5;

    /// Limit parallel fetches
    UInt64 replicated_max_parallel_fetches = 0;
    UInt64 replicated_max_parallel_fetches_for_table = 0;
    /// Limit parallel sends
    UInt64 replicated_max_parallel_sends = 0;
    UInt64 replicated_max_parallel_sends_for_table = 0;

    /// If true, Replicated tables replicas on this node will try to acquire leadership.
    bool replicated_can_become_leader = true;

    /// In seconds.
    UInt64 zookeeper_session_expiration_check_period = 60;

    /** Check delay of replicas settings. */

    /// Period to check replication delay and compare with other replicas.
    UInt64 check_delay_period = 60;

    /// Period to clean old queue logs, blocks hashes and parts
    UInt64 cleanup_delay_period = 30;

    /// Minimal delay from other replicas to yield leadership. Here and further 0 means unlimited.
    UInt64 min_relative_delay_to_yield_leadership = 120;

    /// Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.
    UInt64 min_relative_delay_to_close = 300;

    /// Minimal absolute delay to close, stop serving requests and not return Ok during status check.
    UInt64 min_absolute_delay_to_close = 0;

    /// Enable usage of Vertical merge algorithm.
    UInt64 enable_vertical_merge_algorithm = 1;

    /// Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm
    UInt64 vertical_merge_algorithm_min_rows_to_activate = 16 * DEFAULT_MERGE_BLOCK_SIZE;

    /// Minimal amount of non-PK columns to activate Vertical merge algorithm
    UInt64 vertical_merge_algorithm_min_columns_to_activate = 11;

public:
    void loadFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);

    /// NOTE: will rewrite the AST to add settings that must not change in the future.
    void loadFromQuery(ASTStorage & storage_def);
};

}
