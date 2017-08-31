#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

/** Advanced settings of MergeTree.
  * Could be loaded from config.
  */
struct MergeTreeSettings
{
    /** Merge settings. */

    /// Maximum in total size of parts to merge, when there are maximum (minimum) free threads in background pool (or entries in replication queue).
    size_t max_bytes_to_merge_at_max_space_in_pool = 150ULL * 1024 * 1024 * 1024;
    size_t max_bytes_to_merge_at_min_space_in_pool = 1024 * 1024;

    /// How many tasks of merging parts are allowed simultaneously in ReplicatedMergeTree queue.
    size_t max_replicated_merges_in_queue = 16;

    /// When there is less than specified number of free entries in pool (or replicated queue),
    ///  start to lower maximum size of merge to process (or to put in queue).
    /// This is to allow small merges to process - not filling the pool with long running merges.
    size_t number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8;

    /// How many seconds to keep obsolete parts.
    time_t old_parts_lifetime = 8 * 60;

    /// How many seconds to keep tmp_-directories.
    time_t temporary_directories_lifetime = 86400;

    /** Inserts settings. */

    /// If table contains at least that many active parts, artificially slow down insert into table.
    size_t parts_to_delay_insert = 150;

    /// If more than this number active parts, throw 'Too much parts ...' exception
    size_t parts_to_throw_insert = 300;

    /// Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts.
    size_t max_delay_to_insert = 1;

    /** Replication settings. */

    /// How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).
    size_t replicated_deduplication_window = 100;
    /// Similar to previous, but determines old blocks by their lifetime.
    /// Hash of an inserted block will be deleted (and the block will not be deduplicated after) if it outside of one "window".
    /// You can set very big replicated_deduplication_window to avoid duplicating INSERTs during that period of time.
    size_t replicated_deduplication_window_seconds = 7 * 24 * 60 * 60; /// one week

    /// Keep about this number of last records in ZooKeeper log, even if they are obsolete.
    /// It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.
    size_t replicated_logs_to_keep = 100;

    /// After specified amount of time passed after replication log entry creation
    ///  and sum size of parts is greater than threshold,
    ///  prefer fetching merged part from replica instead of doing merge locally.
    /// To speed up very long merges.
    time_t prefer_fetch_merged_part_time_threshold = 3600;
    size_t prefer_fetch_merged_part_size_threshold = 10ULL * 1024 * 1024 * 1024;

    /// Max broken parts, if more - deny automatic deletion.
    size_t max_suspicious_broken_parts = 10;

    /// Not apply ALTER if number of files for modification(deletion, addition) more than this.
    size_t max_files_to_modify_in_alter_columns = 75;
    /// Not apply ALTER, if number of files for deletion more than this.
    size_t max_files_to_remove_in_alter_columns = 50;

    /// If ratio of wrong parts to total number of parts is less than this - allow to start.
    double replicated_max_ratio_of_wrong_parts = 0.5;

    /// Limit parallel fetches
    size_t replicated_max_parallel_fetches = 0;
    size_t replicated_max_parallel_fetches_for_table = 0;
    /// Limit parallel sends
    size_t replicated_max_parallel_sends = 0;
    size_t replicated_max_parallel_sends_for_table = 0;

    /// If true, Replicated tables replicas on this node will try to acquire leadership.
    bool replicated_can_become_leader = true;

    /// In seconds.
    size_t zookeeper_session_expiration_check_period = 60;

    /** Check delay of replicas settings. */

    /// Period to check replication delay and compare with other replicas.
    size_t check_delay_period = 60;

    /// Period to clean old queue logs, blocks hashes and parts
    size_t cleanup_delay_period = 30;

    /// Minimal delay from other replicas to yield leadership. Here and further 0 means unlimited.
    size_t min_relative_delay_to_yield_leadership = 120;

    /// Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.
    size_t min_relative_delay_to_close = 300;

    /// Minimal absolute delay to close, stop serving requests and not return Ok during status check.
    size_t min_absolute_delay_to_close = 0;

    /// Enable usage of Vertical merge algorithm.
    size_t enable_vertical_merge_algorithm = 1;

    /// Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm
    size_t vertical_merge_algorithm_min_rows_to_activate = 16 * DEFAULT_MERGE_BLOCK_SIZE;

    /// Minimal amount of non-PK columns to activate Vertical merge algorithm
    size_t vertical_merge_algorithm_min_columns_to_activate = 11;


    void loadFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
    {
    #define SET(NAME, GETTER) \
        try \
        { \
            NAME = config.GETTER(config_elem + "." #NAME, NAME); \
        } \
        catch (const Poco::Exception & e) \
        { \
            throw Exception( \
                    "Invalid config parameter: " + config_elem + "/" #NAME + ": " + e.message() + ".", \
                    ErrorCodes::INVALID_CONFIG_PARAMETER); \
        }

        SET(max_bytes_to_merge_at_max_space_in_pool, getUInt64);
        SET(max_bytes_to_merge_at_min_space_in_pool, getUInt64);
        SET(max_replicated_merges_in_queue, getUInt64);
        SET(number_of_free_entries_in_pool_to_lower_max_size_of_merge, getUInt64);
        SET(old_parts_lifetime, getUInt64);
        SET(temporary_directories_lifetime, getUInt64);
        SET(parts_to_delay_insert, getUInt64);
        SET(parts_to_throw_insert, getUInt64);
        SET(max_delay_to_insert, getUInt64);
        SET(replicated_deduplication_window, getUInt64);
        SET(replicated_deduplication_window_seconds, getUInt64);
        SET(replicated_logs_to_keep, getUInt64);
        SET(prefer_fetch_merged_part_time_threshold, getUInt64);
        SET(prefer_fetch_merged_part_size_threshold, getUInt64);
        SET(max_suspicious_broken_parts, getUInt64);
        SET(max_files_to_modify_in_alter_columns, getUInt64);
        SET(max_files_to_remove_in_alter_columns, getUInt64);
        SET(replicated_max_ratio_of_wrong_parts, getDouble);
        SET(replicated_max_parallel_fetches, getUInt64);
        SET(replicated_max_parallel_fetches_for_table, getUInt64);
        SET(replicated_max_parallel_sends, getUInt64);
        SET(replicated_max_parallel_sends_for_table, getUInt64);
        SET(replicated_can_become_leader, getBool);
        SET(zookeeper_session_expiration_check_period, getUInt64);
        SET(check_delay_period, getUInt64);
        SET(cleanup_delay_period, getUInt64);
        SET(min_relative_delay_to_yield_leadership, getUInt64);
        SET(min_relative_delay_to_close, getUInt64);
        SET(min_absolute_delay_to_close, getUInt64);
        SET(enable_vertical_merge_algorithm, getUInt64);
        SET(vertical_merge_algorithm_min_rows_to_activate, getUInt64);
        SET(vertical_merge_algorithm_min_columns_to_activate, getUInt64);

    #undef SET
    }
};

}
