#pragma once

#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

namespace DB
{

struct Task
{
    enum class Kind
    {
        Prepare,
        Work,
        UpdatePipeline,
    };

    ProcessorState * state = nullptr;
    Kind kind = Kind::Prepare;
};

}
