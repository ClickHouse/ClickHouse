#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Common/CurrentMetrics.h>
#include <base/getThreadId.h>
#include <Common/CurrentThread.h>


namespace DB
{


MemoryTrackerThreadSwitcher::MemoryTrackerThreadSwitcher(MergeListEntry & merge_list_entry_)
    : merge_list_entry(merge_list_entry_)
{
    // Each merge is executed into separate background processing pool thread
    background_thread_memory_tracker = CurrentThread::getMemoryTracker();
    if (background_thread_memory_tracker)
    {
        /// From the query context it will be ("for thread") memory tracker with VariableContext::Thread level,
        /// which does not have any limits and sampling settings configured.
        /// And parent for this memory tracker should be ("(for query)") with VariableContext::Process level,
        /// that has limits and sampling configured.
        MemoryTracker * parent;
        if (background_thread_memory_tracker->level == VariableContext::Thread &&
            (parent = background_thread_memory_tracker->getParent()) &&
            parent != &total_memory_tracker)
        {
            background_thread_memory_tracker = parent;
        }

        background_thread_memory_tracker_prev_parent = background_thread_memory_tracker->getParent();
        background_thread_memory_tracker->setParent(&merge_list_entry->memory_tracker);
    }

    prev_untracked_memory_limit = current_thread->untracked_memory_limit;
    current_thread->untracked_memory_limit = merge_list_entry->max_untracked_memory;

    /// Avoid accounting memory from another mutation/merge
    /// (NOTE: consider moving such code to ThreadFromGlobalPool and related places)
    prev_untracked_memory = current_thread->untracked_memory;
    current_thread->untracked_memory = merge_list_entry->untracked_memory;

    prev_query_id = current_thread->getQueryId().toString();
    current_thread->setQueryId(merge_list_entry->query_id);
}


MemoryTrackerThreadSwitcher::~MemoryTrackerThreadSwitcher()
{
    // Unplug memory_tracker from current background processing pool thread

    if (background_thread_memory_tracker)
        background_thread_memory_tracker->setParent(background_thread_memory_tracker_prev_parent);

    current_thread->untracked_memory_limit = prev_untracked_memory_limit;

    merge_list_entry->untracked_memory = current_thread->untracked_memory;
    current_thread->untracked_memory = prev_untracked_memory;

    current_thread->setQueryId(prev_query_id);
}

MergeListElement::MergeListElement(
    const StorageID & table_id_,
    FutureMergedMutatedPartPtr future_part,
    UInt64 memory_profiler_step,
    UInt64 memory_profiler_sample_probability,
    UInt64 max_untracked_memory_)
    : table_id{table_id_}
    , partition_id{future_part->part_info.partition_id}
    , result_part_name{future_part->name}
    , result_part_path{future_part->path}
    , result_part_info{future_part->part_info}
    , num_parts{future_part->parts.size()}
    , max_untracked_memory(max_untracked_memory_)
    , query_id(table_id.getShortName() + "::" + result_part_name)
    , thread_id{getThreadId()}
    , merge_type{future_part->merge_type}
    , merge_algorithm{MergeAlgorithm::Undecided}
{
    for (const auto & source_part : future_part->parts)
    {
        source_part_names.emplace_back(source_part->name);
        source_part_paths.emplace_back(source_part->getFullPath());

        total_size_bytes_compressed += source_part->getBytesOnDisk();
        total_size_marks += source_part->getMarksCount();
        total_rows_count += source_part->index_granularity.getTotalRows();
    }

    if (!future_part->parts.empty())
    {
        source_data_version = future_part->parts[0]->info.getDataVersion();
        is_mutation = (result_part_info.getDataVersion() != source_data_version);
    }

    memory_tracker.setDescription("Mutate/Merge");
    memory_tracker.setProfilerStep(memory_profiler_step);
    memory_tracker.setSampleProbability(memory_profiler_sample_probability);
}

MergeInfo MergeListElement::getInfo() const
{
    MergeInfo res;
    res.database = table_id.getDatabaseName();
    res.table = table_id.getTableName();
    res.result_part_name = result_part_name;
    res.result_part_path = result_part_path;
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
    res.thread_id = thread_id;
    res.merge_type = toString(merge_type);
    res.merge_algorithm = toString(merge_algorithm.load(std::memory_order_relaxed));

    for (const auto & source_part_name : source_part_names)
        res.source_part_names.emplace_back(source_part_name);

    for (const auto & source_part_path : source_part_paths)
        res.source_part_paths.emplace_back(source_part_path);

    return res;
}

MergeListElement::~MergeListElement() = default;


}
