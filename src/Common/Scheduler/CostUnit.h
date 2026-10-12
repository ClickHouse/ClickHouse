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

/// Whether a running allocation of a space-shared resource may be evicted (killed) to satisfy another allocation.
/// Memory reservations are evictable, while query slots are not: a query that holds a slot is never stopped
/// to admit another query, so a pending query waits until a slot is released.
bool isEvictable(CostUnit unit);
String formatReadableCost(ResourceCost cost, CostUnit unit);

}
