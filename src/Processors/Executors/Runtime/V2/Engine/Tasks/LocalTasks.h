#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Enabled.h>

#include <array>
#include <optional>
#include <span>

namespace DB
{

namespace Runtime::V2
{

/// Bounded Chase-Lev ring: the owner pushes and pops LIFO at head, thieves steal batches of the oldest tasks.
/// https://dl.acm.org/doi/epdf/10.1145/1073970.1073974
class LocalTasks
{
    struct Slot
    {
        Wyrd::Atomic<PackedTask> packed;
    };

public:
    static constexpr UInt32 capacity = Wyrd::enabled() ? 8 : 256;

    LocalTasks();

    bool push(const Task & task);
    UInt32 pushMany(std::span<const Task> many);

    std::optional<Task> pop();
    UInt32 steal(std::span<Task> out);

    UInt32 size() const;

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
