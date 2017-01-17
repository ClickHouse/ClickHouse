#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Common/CurrentMetrics.h>
#include <Poco/Ext/ThreadNumber.h>


namespace CurrentMetrics
{
	extern const Metric MemoryTrackingForMerges;
}


namespace DB
{

MergeListElement::MergeListElement(const std::string & database, const std::string & table, const std::string & result_part_name)
	: database{database}, table{table}, result_part_name{result_part_name}, thread_number{Poco::ThreadNumber::get()}
{
	/// Each merge is executed into separate background processing pool thread
	background_pool_task_memory_tracker = current_memory_tracker;
	if (background_pool_task_memory_tracker)
	{
		background_pool_task_memory_tracker->setNext(&memory_tracker);
		memory_tracker.setMetric(CurrentMetrics::MemoryTrackingForMerges);
	}
}

MergeInfo MergeListElement::getInfo() const
{
	MergeInfo res;
	res.database = database;
	res.table = table;
	res.result_part_name = result_part_name;
	res.elapsed = watch.elapsedSeconds();
	res.progress = progress;
	res.num_parts = num_parts;
	res.total_size_bytes_compressed = total_size_bytes_compressed;
	res.total_size_marks = total_size_marks;
	res.bytes_read_uncompressed = bytes_read_uncompressed.load(std::memory_order_relaxed);
	res.bytes_written_uncompressed = bytes_written_uncompressed.load(std::memory_order_relaxed);
	res.rows_read = rows_read.load(std::memory_order_relaxed);
	res.rows_written = rows_written.load(std::memory_order_relaxed);
	res.columns_written = columns_written.load(std::memory_order_relaxed);
	res.memory_usage = memory_tracker.get();
	res.thread_number = thread_number;

	return res;
}

MergeListElement::~MergeListElement()
{
	/// Unplug memory_tracker from current background processing pool thread
	if (background_pool_task_memory_tracker)
		background_pool_task_memory_tracker->setNext(nullptr);
}

}
