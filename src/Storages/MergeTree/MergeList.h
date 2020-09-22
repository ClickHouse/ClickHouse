#pragma once

#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeType.h>
#include <memory>
#include <list>
#include <mutex>
#include <atomic>


/** Maintains a list of currently running merges.
  * For implementation of system.merges table.
  */

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

struct MergeListElement : boost::noncopyable
{
    const std::string database;
    const std::string table;
    std::string partition_id;

    const std::string result_part_name;
    const std::string result_part_path;
    Int64 result_data_version{};
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

    MemoryTracker memory_tracker{VariableContext::Process};
    MemoryTracker * background_thread_memory_tracker;
    MemoryTracker * background_thread_memory_tracker_prev_parent = nullptr;

    UInt64 thread_id;
    MergeType merge_type;
    MergeAlgorithm merge_algorithm;

    MergeListElement(const std::string & database, const std::string & table, const FutureMergedMutatedPart & future_part);

    MergeInfo getInfo() const;

    ~MergeListElement();
};


class MergeList;

class MergeListEntry
{
    MergeList & list;

    using container_t = std::list<MergeListElement>;
    container_t::iterator it;

    CurrentMetrics::Increment num_merges {CurrentMetrics::Merge};

public:
    MergeListEntry(const MergeListEntry &) = delete;
    MergeListEntry & operator=(const MergeListEntry &) = delete;

    MergeListEntry(MergeList & list_, const container_t::iterator it_) : list(list_), it{it_} {}
    ~MergeListEntry();

    MergeListElement * operator->() { return &*it; }
    const MergeListElement * operator->() const { return &*it; }
};


class MergeList
{
    friend class MergeListEntry;

    using container_t = std::list<MergeListElement>;
    using info_container_t = std::list<MergeInfo>;

    mutable std::mutex mutex;
    container_t merges;

    std::atomic<size_t> merges_with_ttl_counter = 0;
public:
    using Entry = MergeListEntry;
    using EntryPtr = std::unique_ptr<Entry>;

    template <typename... Args>
    EntryPtr insert(Args &&... args)
    {
        std::lock_guard lock{mutex};
        auto entry = std::make_unique<Entry>(*this, merges.emplace(merges.end(), std::forward<Args>(args)...));
        if (isTTLMergeType((*entry)->merge_type))
            ++merges_with_ttl_counter;
        return entry;
    }

    info_container_t get() const
    {
        std::lock_guard lock{mutex};
        info_container_t res;
        for (const auto & merge_element : merges)
            res.emplace_back(merge_element.getInfo());
        return res;
    }

    void cancelPartMutations(const String & partition_id, Int64 mutation_version)
    {
        std::lock_guard lock{mutex};
        for (auto & merge_element : merges)
        {
            if ((partition_id.empty() || merge_element.partition_id == partition_id)
                && merge_element.source_data_version < mutation_version
                && merge_element.result_data_version >= mutation_version)
                merge_element.is_cancelled = true;
        }
    }

    size_t getExecutingMergesWithTTLCount() const
    {
        return merges_with_ttl_counter;
    }
};


inline MergeListEntry::~MergeListEntry()
{
    std::lock_guard lock{list.mutex};

    if (isTTLMergeType(it->merge_type))
        --list.merges_with_ttl_counter;

    list.merges.erase(it);
}


}
