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
    MasterThread,
    WorkerThread,
    Query,
};

inline CostUnit costUnitForMode(ResourceAccessMode mode)
{
    switch (mode)
    {
        case ResourceAccessMode::DiskRead: return CostUnit::IOByte;
        case ResourceAccessMode::DiskWrite: return CostUnit::IOByte;
        case ResourceAccessMode::MasterThread: return CostUnit::CPUSlot;
        case ResourceAccessMode::WorkerThread: return CostUnit::CPUSlot;
        case ResourceAccessMode::Query: return CostUnit::QuerySlot;
    }
}

inline std::string_view costUnitToString(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte: return "IOByte";
        case CostUnit::CPUSlot: return "CPUSlot";
        case CostUnit::QuerySlot: return "QuerySlot";
    }
}


}
