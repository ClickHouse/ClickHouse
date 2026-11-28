#include <Common/Scheduler/CostUnit.h>
#include <Common/formatReadable.h>


namespace DB
{

std::string_view costUnitToString(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte: return "IOByte";
        case CostUnit::CPUNanosecond: return "CPUNanosecond";
        case CostUnit::QuerySlot: return "QuerySlot";
        case CostUnit::MemoryByte: return "MemoryByte";
    }
}

SharingMode getSharingMode(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte:        return SharingMode::TimeShared;
        case CostUnit::CPUNanosecond: return SharingMode::TimeShared;
        case CostUnit::QuerySlot:     return SharingMode::SpaceShared;
        case CostUnit::MemoryByte:    return SharingMode::SpaceShared;
    }
}

bool isEvictable(CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte:        return false;
        case CostUnit::CPUNanosecond: return false;
        case CostUnit::QuerySlot:     return false;
        case CostUnit::MemoryByte:    return true;
    }
}

String formatReadableCost(ResourceCost cost, CostUnit unit)
{
    switch (unit)
    {
        case CostUnit::IOByte:        return formatReadableSizeWithBinarySuffix(cost);
        case CostUnit::CPUNanosecond: return formatReadableTime(static_cast<double>(cost));
        case CostUnit::QuerySlot:     return formatReadableQuantity(cost);
        case CostUnit::MemoryByte:    return formatReadableSizeWithBinarySuffix(cost);
    }
}

}
