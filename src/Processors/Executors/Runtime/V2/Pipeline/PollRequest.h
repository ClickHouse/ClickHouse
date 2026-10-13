#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>

#include <cstdint>

namespace DB
{

namespace Runtime::V2
{

struct PollRequest
{
    ExecutingProcessor * processor = nullptr;
    int fd = -1;
    uint32_t events = 0;
    int64_t timeout_ms = -1;
};

}

}
