#pragma once

#include <filesystem>
#include <memory>
#include <Interpreters/PeriodicLog.h>
#include <Common/ZooKeeper/ErrorCounter.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct AggregatedZooKeeperLogElement
{
    /// Identifying a group.
    time_t event_time;
    Int64 session_id;
    String parent_path;
    Int32 operation;

    /// Group statistics.
    UInt32 count;
    std::unique_ptr<Coordination::ErrorCounter> errors;
    UInt64 total_latency_microseconds;

    static std::string name() { return "AggregatedZooKeeperLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class AggregatedZooKeeperLog : public PeriodicLog<AggregatedZooKeeperLogElement>
{
    using PeriodicLog<AggregatedZooKeeperLogElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override;

public:
    void observe(Int64 session_id, Int32 operation, const std::filesystem::path & path, UInt64 latency_microseconds, Coordination::Error error);

private:
    struct EntryKey
    {
        Int64 session_id;
        Int32 operation;
        String parent_path;

        bool operator==(const EntryKey & other) const;
    };
    struct EntryKeyHash
    {
        size_t operator()(const EntryKey & entry_key) const;
    };
    struct EntryStats
    {
        UInt32 count = 0;
        UInt64 total_latency_microseconds = 0;
        std::unique_ptr<Coordination::ErrorCounter> errors = std::make_unique<Coordination::ErrorCounter>();

        void observe(UInt64 latency_microseconds, Coordination::Error error);
    };

    mutable std::mutex stats_mutex;
    std::unordered_map<EntryKey, EntryStats, EntryKeyHash> stats TSA_GUARDED_BY(stats_mutex);
};

}
