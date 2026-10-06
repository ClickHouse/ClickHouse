#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>

namespace DB
{

namespace Runtime::V2
{

constexpr UInt64 state_bits = 52;
constexpr UInt64 step_bits = 64 - state_bits;
constexpr UInt64 state_mask = (UInt64{1} << state_bits) - 1;

static_assert(sizeof(void *) == sizeof(PackedTask));
static_assert(sizeof(Task) == sizeof(PackedTask));

Task Task::create(ProcessorState & state, Step step)
{
    Task task{};
    task.packed = (static_cast<UInt64>(step) << state_bits) | (reinterpret_cast<UInt64>(&state) & state_mask);
    return task;
}

Task Task::unpack(PackedTask packed)
{
    Task task{};
    task.packed = packed;
    return task;
}

PackedTask Task::pack(Task task)
{
    return task.packed;
}

ProcessorState & Task::state() const
{
    return *reinterpret_cast<ProcessorState *>(static_cast<Int64>(packed << step_bits) >> step_bits);
}

Task::Step Task::step() const
{
    return static_cast<Step>(packed >> state_bits);
}

}

}
