#include <Storages/MergeTree/MergeList.h>
#include <Common/CurrentMetrics.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Common/CurrentThread.h>


namespace CurrentMetrics
{
    extern const Metric MemoryTrackingForMerges;
}


namespace DB
{

MergeListElement::MergeListElement(const std::string & database, const std::string & table, const std::string & result_part_name,
    const MergeTreeData::DataPartsVector & source_parts)
        : database{database}, table{table}, result_part_name{result_part_name}, num_parts{source_parts.size()},
        thread_number{Poco::ThreadNumber::get()}
{
    for (const auto & source_part : source_parts)
        source_part_names.emplace_back(source_part->name);

    /// Each merge is executed into separate background processing pool thread
    background_thread_memory_tracker = &CurrentThread::getMemoryTracker();
    if (background_thread_memory_tracker)
    {
        memory_tracker.setMetric(CurrentMetrics::MemoryTrackingForMerges);
        background_thread_memory_tracker_prev_parent = background_thread_memory_tracker->getParent();
        background_thread_memory_tracker->setParent(&memory_tracker);
    }
}

MergeInfo MergeListElement::getInfo() const
{
    MergeInfo res;
    res.database = database;
    res.table = table;
    res.result_part_name = result_part_name;
    res.elapsed = watch.elapsedSeconds();
    res.progress = progress.load(std::memory_order_relaxed);
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

    for (const auto & source_part_name : source_part_names)
        res.source_part_names.emplace_back(source_part_name);

    return res;
}

MergeListElement::~MergeListElement()
{
    /// Unplug memory_tracker from current background processing pool thread
    if (background_thread_memory_tracker)
        background_thread_memory_tracker->setParent(background_thread_memory_tracker_prev_parent);
}

}
