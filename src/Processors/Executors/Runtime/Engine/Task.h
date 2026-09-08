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
        AsyncReady,
        UpdatePipeline,
    };

    ProcessorState * state = nullptr;
    Kind kind = Kind::Prepare;
};

struct AsyncTask
{
    ProcessorState * state = nullptr;
    int fd = -1;
    uint32_t events = 0;
    int64_t timeout_ms = -1;
};

}
