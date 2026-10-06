#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorState.h>

namespace DB
{

namespace Runtime::V2
{

struct ProcessorState;

struct PollRequest
{
    ProcessorState * state = nullptr;
    int fd = -1;
    uint32_t events = 0;
    int64_t timeout_ms = -1;
};

}

}
