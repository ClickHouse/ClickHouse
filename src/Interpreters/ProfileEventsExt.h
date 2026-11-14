#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>

namespace DB
{

using InternalProfileEventsQueue = ConcurrentBoundedQueue<Block>;
using InternalProfileEventsQueuePtr = std::shared_ptr<InternalProfileEventsQueue>;

}

namespace ProfileEvents
{

constexpr size_t NAME_COLUMN_INDEX = 4;
constexpr size_t VALUE_COLUMN_INDEX = 5;

/// The gauges of the device memory of the GPU a query holds and the most it held.
constexpr auto GPU_MEMORY_USAGE_EVENT_NAME = "GPUMemoryTrackerUsage";
constexpr auto GPU_MEMORY_PEAK_USAGE_EVENT_NAME = "GPUMemoryTrackerPeakUsage";

struct ProfileEventsSnapshot
{
    UInt64 thread_id{};
    CountersIncrement counters;
    Int64 memory_usage{};
    Int64 peak_memory_usage{};
    Int64 temp_data_on_disk_usage{};
    /// The device memory of the GPU: sent only by a query that used the device.
    Int64 gpu_memory_usage{};
    Int64 gpu_peak_memory_usage{};
    time_t current_time{};
};

using ThreadIdToCountersSnapshot = std::unordered_map<UInt64, Counters::Snapshot>;

/// Dumps profile events to columns Map(String, UInt64)
void dumpToMapColumn(const Counters::Snapshot & counters, DB::IColumn * column, bool nonzero_only = true);

DB::Block getSampleBlock();

DB::Block getProfileEvents(
    const String & host_name,
    DB::InternalProfileEventsQueuePtr profile_queue,
    ThreadIdToCountersSnapshot & last_sent_snapshots);

/// Decreases the increments of `event` in a `ProfileEvents` packet `block` by `amount` in total, never below zero,
/// and decreases `amount` by what was subtracted, so that it can be spread over several packets.
void subtractIncrements(DB::Block & block, Event event, UInt64 & amount);

/// This is for ProfileEvents packets.
enum Type : int8_t
{
    INCREMENT = 1,
    GAUGE     = 2,
};

extern std::shared_ptr<DB::DataTypeEnum8> TypeEnum;

}
