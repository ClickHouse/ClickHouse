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
///
/// If processes shares spilling/memory state, it should share ISpillable object.
class ISpillable
{
public:
    virtual ~ISpillable() = default;

    virtual ProcessorMemoryStats getMemoryStats() const = 0;

    /// Request to spill @at_least_bytes and return how many had been spilled
    virtual size_t spill(size_t at_least_bytes) = 0;
};

}
