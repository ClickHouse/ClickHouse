#pragma once

#include <string_view>

namespace DB
{

/// Describes what resource request cost means.
/// One resource could not mix different cost units.
enum class CostUnit
{
    IOByte,
    CPUSlot,
    QuerySlot,
};

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
