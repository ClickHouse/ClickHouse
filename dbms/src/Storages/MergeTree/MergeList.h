#pragma once

#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
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
    Array source_part_names;
    Float64 elapsed;
    Float64 progress;
    UInt64 num_parts;
    UInt64 total_size_bytes_compressed;
    UInt64 total_size_marks;
    UInt64 bytes_read_uncompressed;
    UInt64 bytes_written_uncompressed;
    UInt64 rows_read;
    UInt64 rows_written;
    UInt64 columns_written;
    UInt64 memory_usage;
    UInt64 thread_number;
};


struct MergeListElement : boost::noncopyable
{
    const std::string database;
    const std::string table;
    const std::string result_part_name;
    Stopwatch watch;
    std::atomic<Float64> progress{};
    UInt64 num_parts{};
    Names source_part_names;
    UInt64 total_size_bytes_compressed{};
    UInt64 total_size_marks{};
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

    /// Poco thread number used in logs
    UInt32 thread_number;


    MergeListElement(const std::string & database, const std::string & table, const std::string & result_part_name,
                     const MergeTreeData::DataPartsVector & source_parts);

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

    MergeListEntry(MergeList & list, const container_t::iterator it) : list(list), it{it} {}
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

public:
    using Entry = MergeListEntry;
    using EntryPtr = std::unique_ptr<Entry>;

    template <typename... Args>
    EntryPtr insert(Args &&... args)
    {
        std::lock_guard<std::mutex> lock{mutex};
        return std::make_unique<Entry>(*this, merges.emplace(merges.end(), std::forward<Args>(args)...));
    }

    info_container_t get() const
    {
        std::lock_guard<std::mutex> lock{mutex};
        info_container_t res;
        for (const auto & merge_element : merges)
            res.emplace_back(merge_element.getInfo());
        return res;
    }
};


inline MergeListEntry::~MergeListEntry()
{
    std::lock_guard<std::mutex> lock{list.mutex};
    list.merges.erase(it);
}


}
