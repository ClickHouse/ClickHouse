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

/// Lock-Free Batch Workstealing Deque.
/// Implementation of https://www.tvandijk.nl/pdf/2014lace.pdf

class LocalTasks
{
    struct Slot
    {
        Wyrd::Atomic<PackedTask> packed;
    };

    void setSplit(UInt32 new_split);
    void growShared();
    bool shrinkShared();
    void afterPush(UInt32 first);

public:
    static constexpr UInt32 capacity = Wyrd::enabled() ? 8 : 256;

    bool push(const Task & task);
    UInt32 pushMany(std::span<const Task> many);

    std::optional<Task> pop();
    UInt32 offload(std::span<Task> out);
    UInt32 grab(std::span<Task> out);

    UInt32 size() const;

private:
    std::array<Slot, capacity> buffer;

    /// Owner state.
    alignas(128) Wyrd::Atomic<UInt32> head = 0;
    UInt32 owner_split = 0;
    bool owner_all_stolen = true;

    /// Shared state.
    alignas(128) Wyrd::Atomic<UInt64> bounds = 0;
    Wyrd::Atomic<bool> all_stolen = true;

    /// Stealers state.
    alignas(128) Wyrd::Atomic<bool> split_request = false;
};

}

}
