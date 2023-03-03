#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <base/getThreadId.h>


namespace DB
{


ThreadGroupSwitcher::ThreadGroupSwitcher(MergeListEntry * merge_list_entry_)
    : merge_list_entry(merge_list_entry_)
{
    prev_thread_group = CurrentThread::getGroup();
    if (!prev_thread_group)
        return;

    CurrentThread::detachGroup();
    CurrentThread::attachToGroup(merge_list_entry_->thread_group);
}

ThreadGroupSwitcher::~ThreadGroupSwitcher()
{
    if (!prev_thread_group)
        return;

    if (!merge_list_entry)
        return;

    CurrentThread::detachGroup();
    CurrentThread::attachToGroup(prev_thread_group);
}

ThreadGroupSwitcher::ThreadGroupSwitcher(ThreadGroupSwitcher && other)
{
    this->swap(other);
}

ThreadGroupSwitcher& ThreadGroupSwitcher::operator=(ThreadGroupSwitcher && other)
{
    if (this != &other)
    {
        auto tmp = ThreadGroupSwitcher();
        tmp.swap(other);
        this->swap(tmp);
    }
    return *this;
}

void ThreadGroupSwitcher::swap(ThreadGroupSwitcher & other)
{
    std::swap(merge_list_entry, other.merge_list_entry);
    std::swap(prev_thread_group, other.prev_thread_group);
    std::swap(prev_query_id, other.prev_query_id);
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
    , query_id(table_id.getShortName() + "::" + result_part_name)
    , thread_id{getThreadId()}
    , merge_type{future_part->merge_type}
    , merge_algorithm{MergeAlgorithm::Undecided}
    , description{"to apply mutate/merge in " + query_id}
{
    for (const auto & source_part : future_part->parts)
    {
        source_part_names.emplace_back(source_part->name);
        source_part_paths.emplace_back(source_part->getDataPartStorage().getFullPath());

        total_size_bytes_compressed += source_part->getBytesOnDisk();
        total_size_marks += source_part->getMarksCount();
        total_rows_count += source_part->index_granularity.getTotalRows();
    }

    if (!future_part->parts.empty())
    {
        source_data_version = future_part->parts[0]->info.getDataVersion();
        is_mutation = (result_part_info.getDataVersion() != source_data_version);
    }

    thread_group = std::make_shared<ThreadGroupStatus>();

    auto p_counters = CurrentThread::get().current_performance_counters;
    while (p_counters && p_counters->level != VariableContext::Process)
        p_counters = p_counters->getParent();
    thread_group->performance_counters.setParent(p_counters);

    thread_group->master_thread_id = CurrentThread::get().thread_id;

    auto & memory_tracker = thread_group->memory_tracker;

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
    memory_tracker.setSoftLimit(settings.memory_overcommit_ratio_denominator);
    if (settings.memory_tracker_fault_probability > 0.0)
        memory_tracker.setFaultProbability(settings.memory_tracker_fault_probability);

    /// Let's try to copy memory related settings from the query,
    /// since settings that we have here is not from query, but global, from the table.
    ///
    /// NOTE: Remember, that Thread level MemoryTracker does not have any settings,
    /// so it's parent is required.
    MemoryTracker * cur_memory_tracker = CurrentThread::getMemoryTracker();

    if (cur_memory_tracker->level == VariableContext::Thread)
    {
        MemoryTracker * query_memory_tracker = cur_memory_tracker->getParent();
        if (query_memory_tracker != &total_memory_tracker)
        {
            memory_tracker.setOrRaiseHardLimit(query_memory_tracker->getHardLimit());
        }
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
    res.memory_usage = getMemoryTracker().get();
    res.thread_id = thread_id;
    res.merge_type = toString(merge_type);
    res.merge_algorithm = toString(merge_algorithm.load(std::memory_order_relaxed));

    for (const auto & source_part_name : source_part_names)
        res.source_part_names.emplace_back(source_part_name);

    for (const auto & source_part_path : source_part_paths)
        res.source_part_paths.emplace_back(source_part_path);

    return res;
}

}
