#pragma once

#include <Core/Names.h>
#include <Core/Field.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Interpreters/StorageID.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <list>
#include <mutex>
#include <atomic>


namespace CurrentMetrics
{
    extern const Metric Merge;
}

namespace DB
{

struct MergeInfo
{
    std::string database;
    std::string table;
    std::string result_part_name;
    std::string result_part_path;
    Array source_part_names;
    Array source_part_paths;
    std::string partition_id;
    bool is_mutation;
    Float64 elapsed;
    Float64 progress;
    UInt64 num_parts;
    UInt64 total_size_bytes_compressed;
    UInt64 total_size_marks;
    UInt64 total_rows_count;
    UInt64 bytes_read_uncompressed;
    UInt64 bytes_written_uncompressed;
    UInt64 rows_read;
    UInt64 rows_written;
    UInt64 columns_written;
    UInt64 memory_usage;
    UInt64 thread_id;
    std::string merge_type;
    std::string merge_algorithm;
};

struct FutureMergedMutatedPart;
using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

struct MergeListElement;
using MergeListEntry = BackgroundProcessListEntry<MergeListElement, MergeInfo>;

struct Settings;


/**
 * Since merge is executed with multiple threads, this class
 * switches the parent MemoryTracker to account all the memory used.
 */
class MemoryTrackerThreadSwitcher : boost::noncopyable
{
public:
    explicit MemoryTrackerThreadSwitcher(MergeListEntry & merge_list_entry_);
    ~MemoryTrackerThreadSwitcher();
private:
    MergeListEntry & merge_list_entry;
    MemoryTracker * background_thread_memory_tracker;
    MemoryTracker * background_thread_memory_tracker_prev_parent = nullptr;
    UInt64 prev_untracked_memory_limit;
    UInt64 prev_untracked_memory;
    String prev_query_id;
};

using MemoryTrackerThreadSwitcherPtr = std::unique_ptr<MemoryTrackerThreadSwitcher>;

struct MergeListElement : boost::noncopyable
{
    const StorageID table_id;
    std::string partition_id;

    const std::string result_part_name;
    const std::string result_part_path;
    MergeTreePartInfo result_part_info;
    bool is_mutation{};

    UInt64 num_parts{};
    Names source_part_names;
    Names source_part_paths;
    Int64 source_data_version{};

    Stopwatch watch;
    std::atomic<Float64> progress{};
    std::atomic<bool> is_cancelled{};

    UInt64 total_size_bytes_compressed{};
    UInt64 total_size_marks{};
    UInt64 total_rows_count{};
    std::atomic<UInt64> bytes_read_uncompressed{};
    std::atomic<UInt64> bytes_written_uncompressed{};

    /// In case of Vertical algorithm they are actual only for primary key columns
    std::atomic<UInt64> rows_read{};
    std::atomic<UInt64> rows_written{};

    /// Updated only for Vertical algorithm
    std::atomic<UInt64> columns_written{};

    /// Used to adjust ThreadStatus::untracked_memory_limit
    UInt64 max_untracked_memory;
    /// Used to avoid losing any allocation context
    UInt64 untracked_memory = 0;
    /// Used for identifying mutations/merges in trace_log
    std::string query_id;

    UInt64 thread_id;
    MergeType merge_type;
    /// Detected after merge already started
    std::atomic<MergeAlgorithm> merge_algorithm;

    /// Description used for logging
    /// Needs to outlive memory_tracker since it's used in its destructor
    const String description{"Mutate/Merge"};
    MemoryTracker memory_tracker{VariableContext::Process};

    MergeListElement(
        const StorageID & table_id_,
        FutureMergedMutatedPartPtr future_part,
        const Settings & settings);

    MergeInfo getInfo() const;

    MergeListElement * ptr() { return this; }

    ~MergeListElement();

    MergeListElement & ref() { return *this; }
};

/** Maintains a list of currently running merges.
  * For implementation of system.merges table.
  */
class MergeList final : public BackgroundProcessList<MergeListElement, MergeInfo>
{
private:
    using Parent = BackgroundProcessList<MergeListElement, MergeInfo>;
    std::atomic<size_t> merges_with_ttl_counter = 0;
public:
    MergeList()
        : Parent(CurrentMetrics::Merge)
    {}

    void onEntryDestroy(const Parent::Entry & entry) override
    {
        if (isTTLMergeType(entry->merge_type))
            --merges_with_ttl_counter;
    }

    void cancelPartMutations(const StorageID & table_id, const String & partition_id, Int64 mutation_version)
    {
        std::lock_guard lock{mutex};
        for (auto & merge_element : entries)
        {
            if ((partition_id.empty() || merge_element.partition_id == partition_id)
                && merge_element.table_id == table_id
                && merge_element.source_data_version < mutation_version
                && merge_element.result_part_info.getDataVersion() >= mutation_version)
                merge_element.is_cancelled = true;
        }
    }

    void cancelInPartition(const StorageID & table_id, const String & partition_id, Int64 delimiting_block_number)
    {
        std::lock_guard lock{mutex};
        for (auto & merge_element : entries)
        {
            if (merge_element.table_id == table_id
                && merge_element.partition_id == partition_id
                && merge_element.result_part_info.min_block < delimiting_block_number)
                merge_element.is_cancelled = true;
        }
    }

    /// Merge consists of two parts: assignment and execution. We add merge to
    /// merge list on execution, but checking merge list during merge
    /// assignment. This lead to the logical race condition (we can assign more
    /// merges with TTL than allowed). So we "book" merge with ttl during
    /// assignment, and remove from list after merge execution.
    ///
    /// NOTE: Not important for replicated merge tree, we check count of merges twice:
    /// in assignment and in queue before execution.
    void bookMergeWithTTL()
    {
        ++merges_with_ttl_counter;
    }

    void cancelMergeWithTTL()
    {
        --merges_with_ttl_counter;
    }

    size_t getMergesWithTTLCount() const
    {
        return merges_with_ttl_counter;
    }
};

}
