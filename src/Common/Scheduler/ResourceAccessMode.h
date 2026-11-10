#pragma once

#include <string_view>

#include <Common/Scheduler/CostUnit.h>

namespace DB
{

/// Describes kind of operation that requires an access to the resource
enum class ResourceAccessMode
{
    DiskRead,
    DiskWrite,
    CPU,
    /// Deprecated spellings of `CPU`: `WORKER THREAD` acts as `CPU`, `MASTER THREAD` alone schedules nothing
    MasterThread,
    WorkerThread,
    Query,
    MemoryReservation,
};

inline CostUnit costUnitForMode(ResourceAccessMode mode)
{
    switch (mode)
    {
        case ResourceAccessMode::DiskRead: return CostUnit::IOByte;
        case ResourceAccessMode::DiskWrite: return CostUnit::IOByte;
        case ResourceAccessMode::CPU: return CostUnit::CPUNanosecond;
        case ResourceAccessMode::MasterThread: return CostUnit::CPUNanosecond;
        case ResourceAccessMode::WorkerThread: return CostUnit::CPUNanosecond;
        case ResourceAccessMode::Query: return CostUnit::QuerySlot;
        case ResourceAccessMode::MemoryReservation: return CostUnit::MemoryByte;
    }
}

}
