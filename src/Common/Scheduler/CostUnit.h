#pragma once

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

}
