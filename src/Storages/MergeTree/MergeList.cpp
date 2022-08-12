#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <base/getThreadId.h>


namespace DB
{


MemoryTrackerThreadSwitcher::MemoryTrackerThreadSwitcher(MergeListEntry & merge_list_entry_)
    : merge_list_entry(merge_list_entry_)
{
    // Each merge is executed into separate background processing pool thread
    background_thread_memory_tracker = CurrentThread::getMemoryTracker();
    background_thread_memory_tracker_prev_parent = background_thread_memory_tracker->getParent();
    background_thread_memory_tracker->setParent(&merge_list_entry->memory_tracker);

    prev_untracked_memory_limit = current_thread->untracked_memory_limit;
    current_thread->untracked_memory_limit = merge_list_entry->max_untracked_memory;

    /// Avoid accounting memory from another mutation/merge
    /// (NOTE: consider moving such code to ThreadFromGlobalPool and related places)
    prev_untracked_memory = current_thread->untracked_memory;
    current_thread->untracked_memory = merge_list_entry->untracked_memory;

    prev_query_id = std::string(current_thread->getQueryId());
    current_thread->setQueryId(merge_list_entry->query_id);
}


MemoryTrackerThreadSwitcher::~MemoryTrackerThreadSwitcher()
{
    // Unplug memory_tracker from current background processing pool thread
    background_thread_memory_tracker->setParent(background_thread_memory_tracker_prev_parent);

    current_thread->untracked_memory_limit = prev_untracked_memory_limit;

    merge_list_entry->untracked_memory = current_thread->untracked_memory;
    current_thread->untracked_memory = prev_untracked_memory;

    current_thread->setQueryId(prev_query_id);
}

MergeListElement::MergeListElement(
    const StorageID & table_id_,
    FutureMergedMutatedPartPtr future_part,
    const Settings & settings)
    : table_id{table_id_}
    , partition_id{future_part->part_info.partition_id}
    , result_part_name{future_part->name}
    , result_part_path{future_part->path}
    , result_part_info{future_part->part_info}
    , num_parts{future_part->parts.size()}
    , max_untracked_memory(settings.max_untracked_memory)
    , query_id(table_id.getShortName() + "::" + result_part_name)
    , thread_id{getThreadId()}
    , merge_type{future_part->merge_type}
    , merge_algorithm{MergeAlgorithm::Undecided}
    , description{"to apply mutate/merge in " + query_id}
{
    for (const auto & source_part : future_part->parts)
    {
        source_part_names.emplace_back(source_part->name);
        source_part_paths.emplace_back(source_part->data_part_storage->getFullPath());

        total_size_bytes_compressed += source_part->getBytesOnDisk();
        total_size_marks += source_part->getMarksCount();
        total_rows_count += source_part->index_granularity.getTotalRows();
    }

    if (!future_part->parts.empty())
    {
        source_data_version = future_part->parts[0]->info.getDataVersion();
        is_mutation = (result_part_info.getDataVersion() != source_data_version);
    }

    memory_tracker.setDescription(description.c_str());
    /// MemoryTracker settings should be set here, because
    /// later (see MemoryTrackerThreadSwitcher)
    /// parent memory tracker will be changed, and if merge executed from the
    /// query (OPTIMIZE TABLE), all settings will be lost (since
    /// current_thread::memory_tracker will have Thread level MemoryTracker,
    /// which does not have any settings itself, it relies on the settings of the
    /// thread_group::memory_tracker, but MemoryTrackerThreadSwitcher will reset parent).
    memory_tracker.setProfilerStep(settings.memory_profiler_step);
    memory_tracker.setSampleProbability(settings.memory_profiler_sample_probability);
    /// Specify sample probability also for current thread ot track more deallocations.
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        thread_memory_tracker->setSampleProbability(settings.memory_profiler_sample_probability);

    memory_tracker.setSoftLimit(settings.memory_overcommit_ratio_denominator);
    if (settings.memory_tracker_fault_probability)
        memory_tracker.setFaultProbability(settings.memory_tracker_fault_probability);

    /// Let's try to copy memory related settings from the query,
    /// since settings that we have here is not from query, but global, from the table.
    ///
    /// NOTE: Remember, that Thread level MemoryTracker does not have any settings,
    /// so it's parent is required.
    MemoryTracker * query_memory_tracker = CurrentThread::getMemoryTracker();
    MemoryTracker * parent_query_memory_tracker;
    if (query_memory_tracker->level == VariableContext::Thread &&
        (parent_query_memory_tracker = query_memory_tracker->getParent()) &&
        parent_query_memory_tracker != &total_memory_tracker)
    {
        memory_tracker.setOrRaiseHardLimit(parent_query_memory_tracker->getHardLimit());
    }

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
