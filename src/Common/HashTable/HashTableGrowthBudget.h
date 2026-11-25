#pragma once

#include <base/types.h>


/// How a hash table grower may use the Robin Hood mode, see `HashTableGrowerWithPrecalculationAndRobinHood`.
enum class RobinHoodMode : UInt8
{
    Never,
    /// Switch to it, in place instead of growing, when `HashTableGrowthBudget` says that growing costs too much memory.
    OnMemoryPressure,
    /// Switch to it when the hash table becomes large.
    Always,
};

/** Tells a grower in the mode `RobinHoodMode::OnMemoryPressure` whether the hash table should stay smaller.
  *
  * The grower asks before it adds cells to a bucket. After a yes the bucket switches to the Robin Hood mode in place, which
  * adds no memory, or it is sized for that mode. The budget may remember a yes and answer later questions at once.
  */
struct HashTableGrowthBudget
{
    virtual ~HashTableGrowthBudget() = default;

    /// Whether to take the smaller table instead of adding `growth_cells` cells to a bucket that now has `bucket_cells`.
    virtual bool preferSmallerTable(size_t growth_cells, size_t bucket_cells) = 0;
};
