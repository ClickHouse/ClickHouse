#pragma once

#include <Common/Scheduler/SharingMode.h>
#include <base/types.h>
#include <string_view>


namespace DB
{

/// Cost in terms of used resource (e.g. bytes for network IO)
using ResourceCost = Int64;

/// Describes what resource request cost means.
/// One resource could not mix different cost units.
enum class CostUnit
{
    IOByte,
    CPUNanosecond,
    QuerySlot,
    MemoryByte
};

inline std::string_view costUnitToString(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte: return "IOByte";
        case CostUnit::CPUNanosecond: return "CPUNanosecond";
        case CostUnit::QuerySlot: return "QuerySlot";
        case CostUnit::MemoryByte: return "MemoryByte";
    }
}

inline SharingMode getSharingMode(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte:        return SharingMode::TimeShared;
        case CostUnit::CPUNanosecond: return SharingMode::TimeShared;
        case CostUnit::QuerySlot:     return SharingMode::TimeShared;
        case CostUnit::MemoryByte:    return SharingMode::SpaceShared;
    }
}

}
