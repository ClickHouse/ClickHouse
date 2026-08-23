#pragma once

#include <Common/ProcessorMemoryStats.h>

#include <cstddef>

namespace DB
{

/// Memory spilling interface of a processor.
/// Aggregate, join and sort processors can be spillable.
///
/// Kept separate from IProcessor so that the spilling API can evolve without
/// recompiling every translation unit that uses processors.
class ISpillable
{
public:
    virtual ~ISpillable() = default;

    virtual ProcessorMemoryStats getMemoryStats() = 0;

    // If the in-memory data's size is not larger then bytes, it doesn't spill
    virtual bool spillOnSize(size_t bytes) = 0;
};

}
