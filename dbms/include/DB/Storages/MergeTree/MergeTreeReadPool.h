#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/MergeTree/RangesInDataPart.h>
#include <mutex>


namespace DB
{


/// A batch of work for MergeTreeThreadBlockInputStream
struct MergeTreeReadTask
{
	/// data part which should be read while performing this task
	MergeTreeData::DataPartPtr data_part;
	/** Ranges to read from `data_part`.
	 *	Specified in reverse order for MergeTreeThreadBlockInputStream's convenience of calling .pop_back(). */
	MarkRanges mark_ranges;
	/// for virtual `part_index` virtual column
	std::size_t part_index_in_query;
	/// ordered list of column names used in this query, allows returning blocks with consistent ordering
	const Names & ordered_names;
	/// used to determine whether column should be filtered during PREWHERE or WHERE
	const NameSet & column_name_set;
	/// column names to read during WHERE
	const NamesAndTypesList & columns;
	/// column names to read during PREWHERE
	const NamesAndTypesList & pre_columns;
	/// should PREWHERE column be returned to requesting side?
	const bool remove_prewhere_column;
	/// resulting block may require reordering in accordance with `ordered_names`
	const bool should_reorder;

	MergeTreeReadTask(
		const MergeTreeData::DataPartPtr & data_part, const MarkRanges & ranges, const std::size_t part_index_in_query,
		const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
		const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder)
		: data_part{data_part}, mark_ranges{ranges}, part_index_in_query{part_index_in_query},
		  ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
		  remove_prewhere_column{remove_prewhere_column}, should_reorder{should_reorder}
	{}
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

/**	Provides read tasks for MergeTreeThreadBlockInputStream`s in fine-grained batches, allowing for more
 *	uniform distribution of work amongst multiple threads. All parts and their ranges are divided into `threads`
 *	workloads with at most `sum_marks / threads` marks. Then, threads are performing reads from these workloads
 *	in "sequential" manner, requesting work in small batches. As soon as some thread has exhausted
 *	it's workload, it either is signaled that no more work is available (`do_not_steal_tasks == false`) or
 *	continues taking small batches from other threads' workloads (`do_not_steal_tasks == true`).
 */
class MergeTreeReadPool : private boost::noncopyable
{
public:
	/** Pull could dynamically lower (backoff) number of threads, if read operation are too slow.
	  * Settings for that backoff.
	  */
	struct BackoffSettings
	{
		/// Pay attention only to reads, that took at least this amount of time. If set to 0 - means backoff is disabled.
		size_t min_read_latency_ms = 1000;
		/// Count events, when read throughput is less than specified bytes per second.
		size_t max_throughput = 1048576;
		/// Do not pay attention to event, if not enough time passed since previous event.
		size_t min_interval_between_events_ms = 1000;
		/// Number of events to do backoff - to lower number of threads in pool.
		size_t min_events = 2;

		/// Constants above is just an example.
		BackoffSettings(const Settings & settings)
			: min_read_latency_ms(settings.read_backoff_min_latency_ms.totalMilliseconds()),
			max_throughput(settings.read_backoff_max_throughput),
			min_interval_between_events_ms(settings.read_backoff_min_interval_between_events_ms.totalMilliseconds()),
			min_events(settings.read_backoff_min_events)
		{
		}

		BackoffSettings() : min_read_latency_ms(0) {}
	};

	BackoffSettings backoff_settings;

private:
	/** State to track numbers of slow reads.
	  */
	struct BackoffState
	{
		size_t current_threads;
		Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
		size_t num_events = 0;

		BackoffState(size_t threads) : current_threads(threads) {}
	};

	BackoffState backoff_state;

public:
	MergeTreeReadPool(
		const std::size_t threads, const std::size_t sum_marks, const std::size_t min_marks_for_concurrent_read,
		RangesInDataParts parts, MergeTreeData & data, const ExpressionActionsPtr & prewhere_actions,
		const String & prewhere_column_name, const bool check_columns, const Names & column_names,
		const BackoffSettings & backoff_settings,
		const bool do_not_steal_tasks = false);

	MergeTreeReadTaskPtr getTask(const std::size_t min_marks_to_read, const std::size_t thread);

	/** Each worker could call this method and pass information about read performance.
	  * If read performance is too low, pool could decide to lower number of threads: do not assign more tasks to several threads.
	  * This allows to overcome excessive load to disk subsystem, when reads are not from page cache.
	  */
	void profileFeedback(const ReadBufferFromFileBase::ProfileInfo info);

private:
	std::vector<std::size_t> fillPerPartInfo(
		RangesInDataParts & parts, const ExpressionActionsPtr & prewhere_actions, const String & prewhere_column_name,
		const bool check_columns);

	void fillPerThreadInfo(
		const std::size_t threads, const std::size_t sum_marks, std::vector<std::size_t> per_part_sum_marks,
		RangesInDataParts & parts, const std::size_t min_marks_for_concurrent_read);


	/** Если некоторых запрошенных столбцов нет в куске,
	  *	то выясняем, какие столбцы может быть необходимо дополнительно прочитать,
	  *	чтобы можно было вычислить DEFAULT выражение для этих столбцов.
	  *	Добавляет их в columns.
	  */
	NameSet injectRequiredColumns(const MergeTreeData::DataPartPtr & part, Names & columns) const;

	std::vector<std::unique_ptr<Poco::ScopedReadRWLock>> per_part_columns_lock;
	MergeTreeData & data;
	Names column_names;
	bool do_not_steal_tasks;
	std::vector<NameSet> per_part_column_name_set;
	std::vector<NamesAndTypesList> per_part_columns;
	std::vector<NamesAndTypesList> per_part_pre_columns;
	/// @todo actually all of these values are either true or false for the whole query, thus no vector required
	std::vector<char> per_part_remove_prewhere_column;
	std::vector<char> per_part_should_reorder;

	struct Part
	{
		MergeTreeData::DataPartPtr data_part;
		std::size_t part_index_in_query;
	};

	std::vector<Part> parts;

	struct ThreadTask
	{
		struct PartIndexAndRange
		{
			std::size_t part_idx;
			MarkRanges ranges;
		};

		std::vector<PartIndexAndRange> parts_and_ranges;
		std::vector<std::size_t> sum_marks_in_parts;
	};

	std::vector<ThreadTask> threads_tasks;

	std::set<std::size_t> remaining_thread_tasks;

	mutable std::mutex mutex;

	Logger * log = &Logger::get("MergeTreeReadPool");
};

using MergeTreeReadPoolPtr = std::shared_ptr<MergeTreeReadPool>;


}
