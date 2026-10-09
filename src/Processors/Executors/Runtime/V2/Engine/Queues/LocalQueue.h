#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Enabled.h>
#include <base/types.h>

#include <array>
#include <span>
#include <string>

namespace DB
{

namespace Runtime::V2
{

struct ExecutingProcessor;

/// Bounded Chase-Lev ring: the owner pushes and pops LIFO at head, thieves steal batches of the oldest entries.
/// https://dl.acm.org/doi/epdf/10.1145/1073970.1073974
class LocalQueue
{
    struct Slot
    {
        Wyrd::Atomic<ExecutingProcessor *> value;
    };

public:
    static constexpr UInt32 capacity = Wyrd::enabled() ? 8 : 256;

    explicit LocalQueue(std::string name);

    UInt32 pushManyLifo(std::span<ExecutingProcessor * const> many);

    ExecutingProcessor * popLifo();
    UInt32 stealFifo(std::span<ExecutingProcessor *> out);

    UInt32 size() const;
    UInt32 freeSlots() const;

private:
    std::array<Slot, capacity> buffer;

    /// Model:
    ///         - - A B C D E F - - - -
    ///             ^     ^   ^
    ///            [bounds]  head

    /// Next free buffer slot.
    alignas(128) Wyrd::Atomic<UInt32> head = 0;

    /// Two UInt32 halves (tail, taken) in one word.
    alignas(128) Wyrd::Atomic<UInt64> bounds = 0;
};

}

}
