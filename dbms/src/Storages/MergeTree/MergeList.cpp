#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Common/CurrentMetrics.h>
#include <common/getThreadNumber.h>
#include <Common/CurrentThread.h>


namespace CurrentMetrics
{
    extern const Metric MemoryTrackingForMerges;
}


namespace DB
{

MergeListElement::MergeListElement(const std::string & database_, const std::string & table_, const FutureMergedMutatedPart & future_part)
    : database{database_}, table{table_}, partition_id{future_part.part_info.partition_id}
    , result_part_name{future_part.name}
    , result_part_path{future_part.path}
    , result_data_version{future_part.part_info.getDataVersion()}
    , num_parts{future_part.parts.size()}
    , thread_number{getThreadNumber()}
{
    for (const auto & source_part : future_part.parts)
    {
        source_part_names.emplace_back(source_part->name);
        source_part_paths.emplace_back(source_part->getFullPath());

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

MergeListElement::MergeListElement(const std::string & database_, const std::string & table_, const MergeTreeMoveEntry & moving_part)
    : database{database_}, table{table_}, partition_id{moving_part.part->info.partition_id}
    , result_part_name{moving_part.part->name}
    , result_part_path{moving_part.part->getNewPath(moving_part.reserved_space)}
    , result_data_version{moving_part.part->info.getDataVersion()}
    , num_parts{1}
    , thread_number{getThreadNumber()}
{
    source_part_names.emplace_back(moving_part.part->name);
    source_part_paths.emplace_back(moving_part.part->getFullPath());

    {
        std::shared_lock<std::shared_mutex> part_lock(moving_part.part->columns_lock);

        total_size_bytes_compressed += moving_part.part->bytes_on_disk;
        total_size_marks += moving_part.part->getMarksCount();
        total_rows_count += moving_part.part->index_granularity.getTotalRows();
    }

    source_data_version = moving_part.part->info.getDataVersion();
    is_move = 1;
}

MergeInfo MergeListElement::getInfo() const
{
    MergeInfo res;
    res.database = database;
    res.table = table;
    res.result_part_name = result_part_name;
    res.result_part_path = result_part_path;
    res.partition_id = partition_id;
    res.is_mutation = is_mutation;
    res.elapsed = watch.elapsedSeconds();
    res.progress = !is_move ? progress.load(std::memory_order_relaxed)
        : std::min(1., 1. * bytes_written_compressed / total_size_bytes_compressed);
    res.num_parts = num_parts;
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.total_size_marks = total_size_marks;
    res.total_rows_count = total_rows_count;
    res.bytes_read_uncompressed = bytes_read_uncompressed.load(std::memory_order_relaxed);
    res.bytes_written_uncompressed = bytes_written_uncompressed.load(std::memory_order_relaxed);
    res.bytes_written_compressed = bytes_written_compressed.load(std::memory_order_relaxed);
    res.rows_read = rows_read.load(std::memory_order_relaxed);
    res.rows_written = rows_written.load(std::memory_order_relaxed);
    res.columns_written = columns_written.load(std::memory_order_relaxed);
    res.memory_usage = memory_tracker.get();
    res.thread_number = thread_number;

    for (const auto & source_part_name : source_part_names)
        res.source_part_names.emplace_back(source_part_name);

    for (const auto & source_part_path : source_part_paths)
        res.source_part_paths.emplace_back(source_part_path);

    return res;
}

MergeListElement::~MergeListElement()
{
    /// Unplug memory_tracker from current background processing pool thread
    if (background_thread_memory_tracker)
        background_thread_memory_tracker->setParent(background_thread_memory_tracker_prev_parent);
}

}
