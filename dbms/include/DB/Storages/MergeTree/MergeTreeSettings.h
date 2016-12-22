#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{


/** Advanced settings of MergeTree.
  * Could be loaded from config.
  */
struct MergeTreeSettings
{
	/** Merge settings. */

	/// Maximum in total size of parts to merge, when there are maximum (minimum) free threads in background pool (or entries in replication queue).
	size_t max_bytes_to_merge_at_max_space_in_pool = 100ULL * 1024 * 1024 * 1024;
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
	size_t max_delay_to_insert = 200;

	/** Replication settings. */

	/// How many last blocks of hashes should be kept in ZooKeeper.
	size_t replicated_deduplication_window = 100;

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

	/// Maximum number of errors during parts loading, while ReplicatedMergeTree still allowed to start.
	size_t replicated_max_unexpected_parts = 3;
	size_t replicated_max_unexpectedly_merged_parts = 2;
	size_t replicated_max_missing_obsolete_parts = 5;
	size_t replicated_max_missing_active_parts = 20;

	/// If ration of wrong parts to total number of parts is less than this - allow to start anyway.
	double replicated_max_ratio_of_wrong_parts = 0.05;

	/// In seconds.
	size_t zookeeper_session_expiration_check_period = 60;

	/** Check delay of replicas settings. */

	/// Period to check replication delay and compare with other replicas.
	size_t check_delay_period = 60;

	/// Minimal delay from other replicas to yield leadership. Here and further 0 means unlimited.
	size_t min_relative_delay_to_yield_leadership = 120;

	/// Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.
	size_t min_relative_delay_to_close = 300;

	/// Minimal absolute delay to close, stop serving requests and not return Ok during status check.
	size_t min_absolute_delay_to_close = 0;

	/// Enable usage of Vertical merge algorithm.
	size_t enable_vertical_merge_algorithm = 0;

	/// Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm
	size_t vertical_merge_algorithm_min_rows_to_activate = 16 * DEFAULT_MERGE_BLOCK_SIZE;

	/// Minimal amount of non-PK columns to activate Vertical merge algorithm
	size_t vertical_merge_algorithm_min_columns_to_activate = 11;


	void loadFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
	{
	#define SET_DOUBLE(NAME) \
		NAME = config.getDouble(config_elem + "." #NAME, NAME);

	#define SET_SIZE_T(NAME) \
		if (config.has(config_elem + "." #NAME)) NAME = parse<size_t>(config.getString(config_elem + "." #NAME));

		SET_SIZE_T(max_bytes_to_merge_at_max_space_in_pool);
		SET_SIZE_T(max_bytes_to_merge_at_min_space_in_pool);
		SET_SIZE_T(max_replicated_merges_in_queue);
		SET_SIZE_T(old_parts_lifetime);
		SET_SIZE_T(temporary_directories_lifetime);
		SET_SIZE_T(parts_to_delay_insert);
		SET_SIZE_T(parts_to_throw_insert);
		SET_SIZE_T(max_delay_to_insert);
		SET_SIZE_T(replicated_deduplication_window);
		SET_SIZE_T(replicated_logs_to_keep);
		SET_SIZE_T(prefer_fetch_merged_part_time_threshold);
		SET_SIZE_T(prefer_fetch_merged_part_size_threshold);
		SET_SIZE_T(max_suspicious_broken_parts);
		SET_SIZE_T(max_files_to_modify_in_alter_columns);
		SET_SIZE_T(max_files_to_remove_in_alter_columns);
		SET_SIZE_T(replicated_max_unexpected_parts);
		SET_SIZE_T(replicated_max_unexpectedly_merged_parts);
		SET_SIZE_T(replicated_max_missing_obsolete_parts);
		SET_SIZE_T(replicated_max_missing_active_parts);
		SET_DOUBLE(replicated_max_ratio_of_wrong_parts);
		SET_SIZE_T(zookeeper_session_expiration_check_period);
		SET_SIZE_T(check_delay_period);
		SET_SIZE_T(min_relative_delay_to_yield_leadership);
		SET_SIZE_T(min_relative_delay_to_close);
		SET_SIZE_T(min_absolute_delay_to_close);
		SET_SIZE_T(enable_vertical_merge_algorithm);
		SET_SIZE_T(vertical_merge_algorithm_min_rows_to_activate);
		SET_SIZE_T(vertical_merge_algorithm_min_columns_to_activate);

	#undef SET_SIZE_T
	#undef SET_DOUBLE
	}
};

}
