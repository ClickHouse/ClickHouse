#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Common/CurrentMetrics.h>
#include <common/getThreadNumber.h>
#include <Common/CurrentThread.h>


namespace CurrentMetrics
{
    extern const Metric MemoryTrackingForMerges;
}


namespace DB
{

MergeListElement::MergeListElement(const std::string & database, const std::string & table, const FutureMergedMutatedPart & future_part)
    : database{database}, table{table}, partition_id{future_part.part_info.partition_id}
    , result_part_name{future_part.name}
    , result_data_version{future_part.part_info.getDataVersion()}
    , num_parts{future_part.parts.size()}
    , thread_number{getThreadNumber()}
{
    for (const auto & source_part : future_part.parts)
    {
        source_part_names.emplace_back(source_part->name);

        std::shared_lock<std::shared_mutex> part_lock(source_part->columns_lock);

        total_size_bytes_compressed += source_part->bytes_on_disk;
        total_size_marks += source_part->getMarksCount();
        total_rows_count += source_part->index_granularity.getTotalRows();
    }

    if (!future_part.parts.empty())
    {
        source_data_version = future_part.parts[0]->info.getDataVersion();
        is_mutation = (result_data_version != source_data_version);
    }

    /// Each merge is executed into separate background processing pool thread
    background_thread_memory_tracker = CurrentThread::getMemoryTracker();
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
    res.partition_id = partition_id;
    res.is_mutation = is_mutation;
    res.elapsed = watch.elapsedSeconds();
    res.progress = progress.load(std::memory_order_relaxed);
    res.num_parts = num_parts;
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.total_size_marks = total_size_marks;
    res.total_rows_count = total_rows_count;
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
