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

std::string_view costUnitToString(CostUnit unit);
SharingMode getSharingMode(CostUnit unit);
String formatReadableCost(ResourceCost cost, CostUnit unit);

}
