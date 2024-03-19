#pragma once
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/IColumn.h>


namespace ProfileEvents
{

constexpr size_t NAME_COLUMN_INDEX = 4;
constexpr size_t VALUE_COLUMN_INDEX = 5;

struct ProfileEventsSnapshot
{
    UInt64 thread_id;
    CountersIncrement counters;
    Int64 memory_usage;
    Int64 peak_memory_usage;
    time_t current_time;
};

using ThreadIdToCountersSnapshot = std::unordered_map<UInt64, Counters::Snapshot>;

/// Dumps profile events to columns Map(String, UInt64)
void dumpToMapColumn(const Counters::Snapshot & counters, DB::IColumn * column, bool nonzero_only = true);

void getProfileEvents(
    const String & host_name,
    DB::InternalProfileEventsQueuePtr profile_queue,
    DB::Block & block,
    ThreadIdToCountersSnapshot & last_sent_snapshots);

/// This is for ProfileEvents packets.
enum Type : int8_t
{
    INCREMENT = 1,
    GAUGE     = 2,
};

extern std::shared_ptr<DB::DataTypeEnum8> TypeEnum;

}
