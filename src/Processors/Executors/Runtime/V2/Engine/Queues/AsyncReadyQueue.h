#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Common/VectorWithMemoryTracking.h>

#include <span>

namespace DB
{

namespace Runtime::V2
{

class AsyncReadyQueue
{
public:
    AsyncReadyQueue();

    void pushMany(std::span<ExecutingProcessor * const> many);
    size_t popManyLifo(std::span<ExecutingProcessor *> out, size_t sharers);
    size_t size() const;

private:
    Wyrd::Mutex mutex;
    VectorWithMemoryTracking<ExecutingProcessor *> entries;
    Wyrd::Atomic<size_t> count = 0;
};

}

}
