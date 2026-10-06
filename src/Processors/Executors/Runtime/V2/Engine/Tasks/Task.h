#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorState.h>

namespace DB
{

namespace Runtime::V2
{

using PackedTask = UInt64;

class Task
{
public:
    enum class Step : UInt8
    {
        Prepare,
        Work,
        AsyncReady,
        UpdatePipeline,
    };

    static Task create(ProcessorState & state, Step step);
    static Task unpack(PackedTask packed);
    static PackedTask pack(Task task);

    ProcessorState & state() const;
    Step step() const;

private:
    PackedTask packed;
};

}

}
